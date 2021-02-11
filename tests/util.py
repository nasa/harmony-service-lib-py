import argparse
import sys
from unittest.mock import patch, MagicMock
from contextlib import contextmanager

from harmony import cli, util


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


def cli_test(*cli_args):
    """
    Decorator that takes a list of CLI parameters, patches them into
    sys.argv and passes a parser into the wrapped method
    """
    def cli_test_wrapper(func):
        def wrapper(self):
            with cli_parser(*cli_args) as parser:
                func(self, parser)
        return wrapper
    return cli_test_wrapper


@contextmanager
def cli_parser(*cli_args):
    """
    Returns a parser for the given CLI args

    Returns
    -------
    argparse.ArgumentParser
        the parser for the given CLI args
    """
    with patch.object(sys, 'argv', ['example'] + list(cli_args)):
        parser = argparse.ArgumentParser(
            prog='example', description='Run an example service')
        cli.setup_cli(parser)
        yield parser


def config_fixture(fallback_authn_enabled=False,
                   edl_username='yoda',
                   edl_password='password_this_is',
                   use_localstack=False,
                   staging_bucket='UNKNOWN',
                   staging_path='UNKNOWN',
                   oauth_client_id=None):
    c = util.config(validate=False)

    return util.Config(
        # Override
        fallback_authn_enabled=fallback_authn_enabled,
        edl_username=edl_username,
        edl_password=edl_password,
        use_localstack=use_localstack,
        staging_path=staging_path,
        staging_bucket=staging_bucket,
        oauth_client_id=oauth_client_id,
        # Default
        env=c.env,
        app_name=c.app_name,
        oauth_host=c.oauth_host,
        oauth_uid=c.oauth_uid,
        oauth_password=c.oauth_password,
        oauth_redirect_uri=c.oauth_redirect_uri,
        backend_host=c.backend_host,
        localstack_host=c.localstack_host,
        aws_default_region=c.aws_default_region,
        text_logger=c.text_logger,
        health_check_path=c.health_check_path,
        shared_secret_key=c.shared_secret_key,
    )
