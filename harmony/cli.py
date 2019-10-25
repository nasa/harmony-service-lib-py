"""
=========
cli.py
=========

Parses CLI arguments provided by Harmony and invokes the subsetter accordingly
"""

import sys
from harmony.message import Message
from harmony.util import callback_with_error

def setup_cli(parser):
    """
    Adds Harmony arguments to the CLI being parsed by the provided parser

    Parameters
    ----------
    parser : argparse.ArgumentParser
        The parser being used to parse CLI arguments

    Returns
    -------
    None
    """
    parser.add_argument('--harmony-action',
                        choices=['invoke'],
                        help='the action Harmony needs to perform (currently only "invoke")')
    parser.add_argument('--harmony-input',
                        help='the input data for the action provided by Harmony')


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


def run_cli(parser, args, AdapterClass):
    """
    Runs the Harmony CLI invocation captured by the given args

    Parameters
    ----------
    parser : argparse.ArgumentParser
        The parser being used to parse CLI arguments, used to provide CLI argument errors
    args : Namespace
        Argument values parsed from the command line, presumably via ArgumentParser.parse_args

    Returns
    -------
    is_harmony_cli : bool
        True if the provided arguments constitute a Harmony CLI invocation, False otherwise
    """

    if args.harmony_action in ['invoke'] and not bool(args.harmony_input):
        parser.error(
            '--harmony-input must be provided for --harmony-action  %s' % (args.harmony_action))

    message = None
    output_name = None
    if args.harmony_input:
        message = Message(args.harmony_input)

    adapter = AdapterClass(message)
    try:
        if args.harmony_action == 'invoke':
            adapter.invoke()
    except:
        # Make sure we always call back if the error is in a Harmony invocation and we have
        # successfully parsed enough that we know where to call back to
        if not adapter.is_complete:
            adapter.completed_with_error('An unexpected error occurred')
        raise
