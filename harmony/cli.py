"""
======
cli.py
======

Parses CLI arguments provided by Harmony and invokes the subsetter accordingly
"""

import sys
import logging
from harmony.message import Message
from harmony.util import (CanceledException, HarmonyException, receive_messages, delete_message,
                          change_message_visibility, setup_stdout_log_formatting, get_env, create_decrypter)


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
                        help='the action Harmony needs to perform, "invoke" to run once and quit, "start" to listen to a queue')
    parser.add_argument('--harmony-input',
                        help='the input data for the action provided by Harmony, required for --harmony-action=invoke')
    parser.add_argument('--harmony-queue-url',
                        help='the queue URL to listen on, required for --harmony-action=start')
    parser.add_argument('--harmony-visibility-timeout',
                        type=int,
                        default=600,
                        help='the number of seconds the service is given to process a message before processing is assumed to have failed')
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
    return args.harmony_action != None


def _invoke(AdapterClass, message_string):
    """
    Handles --harmony-action=invoke by invoking the adapter for the given input message

    Parameters
    ----------
    AdapterClass : class
        The BaseHarmonyAdapter subclass to use to handle service invocations
    message_string : string
        The Harmony input message
    Returns
    -------
    True if the operation completed successfully, False otherwise
    """

    secret_key = get_env('SHARED_SECRET_KEY')
    adapter = AdapterClass(
        Message(message_string, create_decrypter(bytes(secret_key, 'utf-8'))))

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
    return not adapter.is_failed


def _start(AdapterClass, queue_url, visibility_timeout_s):
    """
    Handles --harmony-action=start by listening to the given queue_url and invoking the
    AdapterClass on any received messages

    Parameters
    ----------
    AdapterClass : class
        The BaseHarmonyAdapter subclass to use to handle service invocations
    queue_url : string
        The SQS queue to listen on
    """
    for receipt, message in receive_messages(queue_url, visibility_timeout_s):
        # Behavior here is slightly different than _invoke.  Whereas _invoke ensures
        # that the backend receives a callback whenever possible in the case of an
        # exception, the message queue listener prefers to let the message become
        # visibile again and let retry and dead letter queue policies determine visibility
        adapter = AdapterClass(Message(message))
        try:
            adapter.invoke()
        except Exception:
            logging.error('Adapter threw an exception', exc_info=True)
        finally:
            if adapter.is_complete:
                delete_message(queue_url, receipt)
            else:
                change_message_visibility(queue_url, receipt, 0)
            try:
                adapter.cleanup()
            except Exception:
                logging.error(
                    'Adapter threw an exception on cleanup', exc_info=True)


def run_cli(parser, args, AdapterClass):
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
    """
    if args.harmony_wrap_stdout:
        setup_stdout_log_formatting()

    if args.harmony_action == 'invoke':
        if not bool(args.harmony_input):
            parser.error(
                '--harmony-input must be provided for --harmony-action=invoke')
        else:
            successful = _invoke(AdapterClass, args.harmony_input)
            if not successful:
                raise Exception('Service operation failed')

    if args.harmony_action == 'start':
        if not bool(args.harmony_queue_url):
            parser.error(
                '--harmony-queue-url must be provided for --harmony-action=start')
        else:
            return _start(AdapterClass, args.harmony_queue_url, args.harmony_visibility_timeout)
