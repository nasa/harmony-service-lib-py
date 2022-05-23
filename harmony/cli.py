"""
======
cli.py
======

Parses CLI arguments provided by Harmony and invokes the subsetter accordingly
"""

import json
import logging
from os import path, makedirs
import datetime

from pystac import Catalog, CatalogType

from harmony.exceptions import CanceledException, HarmonyException
from harmony.message import Message
from harmony.logging import setup_stdout_log_formatting, build_logger
from harmony.util import (receive_messages, delete_message, change_message_visibility,
                          config, create_decrypter)


def setup_cli(parser):
    """
    Adds Harmony arguments to the CLI being parsed by the provided parser

    Parameters
    ----------
    parser : argparse.ArgumentParser
        The parser being used to parse CLI arguments
    """
    parser.add_argument('--harmony-action',
                        choices=['invoke', 'start'],
                        help=('the action Harmony needs to perform, "invoke" to run once and quit, '
                              '"start" to listen to a queue'))
    parser.add_argument('--harmony-input',
                        help=('the input data for the action provided by Harmony, required for '
                              '--harmony-action=invoke'))
    parser.add_argument('--harmony-sources',
                        help=('file path that contains a STAC catalog with items and metadata to '
                              'be processed by the service.  Required for non-deprecated '
                              'invocations '))
    parser.add_argument('--harmony-metadata-dir',
                        help=('file path where output metadata should be written. The resulting '
                              'STAC catalog will be written to catalog.json in the supplied dir '
                              'with child resources in the same directory or a descendant '
                              'directory.  The remaining message, less any completed operations, '
                              'should be written to message.json in the supplied directory.  If '
                              'there is an error, it will be written to error.json in the supplied dir '))
    parser.add_argument('--harmony-data-location',
                        help=('the location where output data should be written, either a directory '
                              'or S3 URI prefix.  If set, overrides any value set by the message'))
    parser.add_argument('--harmony-queue-url',
                        help='the queue URL to listen on, required for --harmony-action=start')
    parser.add_argument('--harmony-visibility-timeout',
                        type=int,
                        default=600,
                        help=('the number of seconds the service is given to process a message '
                              'before processing is assumed to have failed'))
    parser.add_argument('--harmony-wrap-stdout',
                        action='store_const',
                        const=True,
                        help='Do not wrap STDOUT and STDERR in the Harmony log output format')


def is_harmony_cli(args):
    """
    Returns True if the passed parsed CLI arguments constitute a Harmony CLI invocation, False otherwise

    Parameters
    ----------
    args : Namespace
        Argument values parsed from the command line, presumably via ArgumentParser.parse_args

    Returns
    -------
    is_harmony_cli : bool
        True if the provided arguments constitute a Harmony CLI invocation, False otherwise
    """
    return args.harmony_action is not None


def _invoke_deprecated(AdapterClass, message_string, config):
    """
    Handles --harmony-action=invoke by invoking the adapter for the given input message

    Parameters
    ----------
    AdapterClass : class
        The BaseHarmonyAdapter subclass to use to handle service invocations
    message_string : string
        The Harmony input message
    config : harmony.util.Config
        A configuration instance for this service
    Returns
    -------
    True if the operation completed successfully, False otherwise
    """

    secret_key = config.shared_secret_key
    decrypter = create_decrypter(bytes(secret_key, 'utf-8'))

    message_data = json.loads(message_string)
    adapter = AdapterClass(Message(message_data, decrypter))
    adapter.set_config(config)

    try:
        adapter.invoke()
        if not adapter.is_complete:
            adapter.completed_with_error('The backend service did not respond')

    except CanceledException:
        # If we see the request has been canceled do not try calling back to harmony again
        # Enable this logging after fixing HARMONY-410
        # logging.error('Service request canceled by Harmony, exiting')
        pass
    except HarmonyException as e:
        logging.error(e, exc_info=1)
        if not adapter.is_complete:
            adapter.completed_with_error(str(e))
    except BaseException as e:
        # Make sure we always call back if the error is in a Harmony invocation and we have
        # successfully parsed enough that we know where to call back to
        logging.error(e, exc_info=1)
        if not adapter.is_complete:
            msg = 'Service request failed with an unknown error'
            adapter.completed_with_error(msg)
        raise
    return not adapter.is_failed


def _write_error(metadata_dir, message, category='Unknown'):
    """
    Writes the given error message to error.json in the provided metadata dir

    Parameters
    ----------
    metadata_dir : string
        Directory into which the error should be written
    message : string
        The error message to write
    category : string
        The error category to write
    """
    with open(path.join(metadata_dir, 'error.json'), 'w') as file:
        json.dump({'error': message, 'category': category}, file)


def _build_adapter(AdapterClass, message_string, sources_path, data_location, config):
    """
    Creates the adapter to be invoked for the given harmony input

    Parameters
    ----------
    AdapterClass : class
        The BaseHarmonyAdapter subclass to use to handle service invocations
    message_string : string
        The Harmony input message
    sources_path : string
        A file location containing a STAC catalog corresponding to the input message sources
    data_location : string
        The name of the directory where output should be written
    config : harmony.util.Config
        A configuration instance for this service
    Returns
    -------
        BaseHarmonyAdapter subclass instance
            The adapter to be invoked
    """
    catalog = Catalog.from_file(sources_path)
    secret_key = config.shared_secret_key

    if bool(secret_key):
        decrypter = create_decrypter(bytes(secret_key, 'utf-8'))
    else:
        def identity(arg):
            return arg
        decrypter = identity

    message = Message(json.loads(message_string), decrypter)
    if data_location:
        message.stagingLocation = data_location
    adapter = AdapterClass(message, catalog)
    adapter.set_config(config)

    return adapter


def _invoke(adapter, metadata_dir):
    """
    Handles --harmony-action=invoke by invoking the adapter for the given input message

    Parameters
    ----------
    adapter : BaseHarmonyAdapter
        The BaseHarmonyAdapter subclass to use to handle service invocations
    metadata_dir : string
        The name of the directory where STAC and message output should be written
    Returns
    -------
    True if the operation completed successfully, False otherwise
    """
    try:
        makedirs(metadata_dir, exist_ok=True)
        (out_message, out_catalog) = adapter.invoke()
        out_catalog.normalize_and_save(metadata_dir, CatalogType.SELF_CONTAINED)

        with open(path.join(metadata_dir, 'message.json'), 'w') as file:
            json.dump(out_message.output_data, file)
    except HarmonyException as err:
        logging.error(err, exc_info=1)
        _write_error(metadata_dir, err.message, err.category)
        raise
    except BaseException as err:
        logging.error(err, exc_info=1)
        _write_error(metadata_dir, 'Service request failed with an unknown error')
        raise


def _start(AdapterClass, queue_url, visibility_timeout_s, config):
    """
    Handles --harmony-action=start by listening to the given queue_url and invoking the
    AdapterClass on any received messages

    Parameters
    ----------
    AdapterClass : class
        The BaseHarmonyAdapter subclass to use to handle service invocations
    queue_url : string
        The SQS queue to listen on
    visibility_timeout_s : int
        The time interval during which the message can't be picked up by other
        listeners on the queue.
    config : harmony.util.Config
        A configuration instance for this service
    """
    for receipt, message in receive_messages(queue_url, visibility_timeout_s, cfg=config):
        # Behavior here is slightly different than _invoke.  Whereas _invoke ensures
        # that the backend receives a callback whenever possible in the case of an
        # exception, the message queue listener prefers to let the message become
        # visibile again and let retry and dead letter queue policies determine visibility
        adapter = AdapterClass(Message(message))
        adapter.set_config(config)

        try:
            adapter.invoke()
        except Exception:
            logging.error('Adapter threw an exception', exc_info=True)
        finally:
            if adapter.is_complete:
                delete_message(queue_url, receipt, cfg=config)
            else:
                change_message_visibility(queue_url, receipt, 0, cfg=config)
            try:
                adapter.cleanup()
            except Exception:
                logging.error(
                    'Adapter threw an exception on cleanup', exc_info=True)


def run_cli(parser, args, AdapterClass, cfg=None):
    """
    Runs the Harmony CLI invocation captured by the given args

    Parameters
    ----------
    parser : argparse.ArgumentParser
        The parser being used to parse CLI arguments, used to provide CLI argument errors
    args : Namespace
        Argument values parsed from the command line, presumably via ArgumentParser.parse_args
    AdapterClass : class
        The BaseHarmonyAdapter subclass to use to handle service invocations
    cfg : harmony.util.Config
        A configuration instance for this service
    """
    if cfg is None:
        cfg = config()
    if args.harmony_wrap_stdout:
        setup_stdout_log_formatting(cfg)

    if args.harmony_action == 'invoke':
        start_time = datetime.datetime.now()
        if not bool(args.harmony_input):
            parser.error(
                '--harmony-input must be provided for --harmony-action=invoke')
        elif not bool(args.harmony_sources):
            successful = _invoke_deprecated(AdapterClass, args.harmony_input, cfg)
            if not successful:
                raise Exception('Service operation failed')
        else:
            try:
                adapter = _build_adapter(AdapterClass,
                                         args.harmony_input,
                                         args.harmony_sources,
                                         args.harmony_data_location,
                                         cfg)
                adapter.logger.info(f'timing.{cfg.app_name}.start')
                _invoke(adapter, args.harmony_metadata_dir)
            finally:
                time_diff = datetime.datetime.now() - start_time
                duration_ms = int(round(time_diff.total_seconds() * 1000))
                duration_logger = build_logger(cfg)
                extra_fields = {
                    'user': adapter.message.user,
                    'requestId': adapter.message.requestId,
                    'durationMs': duration_ms
                }
                duration_logger.info(f'timing.{cfg.app_name}.end', extra=extra_fields)

    if args.harmony_action == 'start':
        if not bool(args.harmony_queue_url):
            parser.error(
                '--harmony-queue-url must be provided for --harmony-action=start')
        else:
            return _start(AdapterClass, args.harmony_queue_url, args.harmony_visibility_timeout, cfg)
