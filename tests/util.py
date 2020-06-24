import unittest
import sys
import argparse
from unittest.mock import patch, MagicMock

from harmony import cli


def mock_receive(client, parser, AdapterClass, *messages):
    """
    Mocks an sqs receive call
    """
    sqs = MagicMock()
    side_effects = []

    for i, message in enumerate(messages):
        contents = []
        if message:
            contents.append({ 'Body': message, 'ReceiptHandle': i })
        side_effects.append({ 'Messages': contents })

    print(side_effects)
    sqs.receive_message.side_effect = side_effects
    client.return_value = sqs
    args = parser.parse_args()
    try:
        cli.run_cli(parser, args, AdapterClass)
    except RuntimeError as e:
        if str(e) == 'generator raised StopIteration':
            pass # Expection.  Happens every call when messages are exhausted, allowing us to stop iterating.
        else:
            raise
    return sqs
  