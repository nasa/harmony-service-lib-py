from unittest.mock import MagicMock

from harmony import cli


def mock_receive(cfg, client, parser, AdapterClass, *messages):
    """
    Mocks an sqs receive call
    """
    sqs = MagicMock()
    side_effects = []

    for i, message in enumerate(messages):
        contents = []
        if message:
            contents.append({'Body': message, 'ReceiptHandle': i})
        # this allows us to test what happens when receiving a message from the queue fails
        if isinstance(message, Exception):
            side_effects = message
            break
        else:
            side_effects.append({'Messages': contents})

    print(side_effects)
    sqs.receive_message.side_effect = side_effects
    client.return_value = sqs
    args = parser.parse_args()
    try:
        cli.run_cli(parser, args, AdapterClass, cfg=cfg)
    except RuntimeError as e:
        if str(e) == 'generator raised StopIteration':
            # Expection.  Happens every call when messages are exhausted, allowing us to stop iterating.
            pass
        else:
            raise
    return sqs
