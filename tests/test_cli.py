import unittest
import sys
import argparse
from unittest.mock import patch

from harmony import cli, BaseHarmonyAdapter

def cli_test(*cli_args):
    """
    Decorator that takes a list of CLI parameters, patches them into
    sys.argv and passes a parser into the wrapped method
    """
    def cli_test_wrapper(func):
        def wrapper(self):
            with patch.object(sys, 'argv', ['example'] + list(cli_args)):
                parser = argparse.ArgumentParser(prog='example', description='Run an example service')
                cli.setup_cli(parser)
                func(self, parser)
        return wrapper
    return cli_test_wrapper

class MockAdapter(BaseHarmonyAdapter):
    """
    Dummy class to mock adapter calls and record the input messages
    """
    message = None
    error = None
    def __init__(self, message):
        MockAdapter.message = message

    def invoke(self):
        self.is_complete = True

    def completed_with_error(self, error):
        MockAdapter.error = error


class IsHarmonyCli(unittest.TestCase):
    @cli_test('--something-else', 'invoke')
    def test_when_not_passing_harmony_action_it_returns_false(self, parser):
        parser.add_argument('--something-else')
        args = parser.parse_args()
        self.assertFalse(cli.is_harmony_cli(args))

    @cli_test('--harmony-action', 'invoke')
    def test_when_passing_harmony_action_it_returns_true(self, parser):
        args = parser.parse_args()
        self.assertTrue(cli.is_harmony_cli(args))

    @cli_test()
    def test_when_passing_nothing_it_returns_false(self, parser):
        args = parser.parse_args()
        self.assertFalse(cli.is_harmony_cli(args))

class RunCli(unittest.TestCase):
    def tearDown(self):
        MockAdapter.message = None
        MockAdapter.error = None

    @cli_test('--harmony-action', 'invoke')
    def test_when_harmony_input_is_not_provided_it_terminates_with_error(self, parser):
        with patch.object(parser, 'error') as error_method:
            args = parser.parse_args()
            cli.run_cli(parser, args, MockAdapter)
            error_method.assert_called_once_with('--harmony-input must be provided for --harmony-action invoke')

    @cli_test('--harmony-action', 'invoke', '--harmony-input', '{"test": "input"}')
    def test_when_harmony_input_is_provided_it_creates_and_invokes_an_adapter(self, parser):
        args = parser.parse_args()
        cli.run_cli(parser, args, MockAdapter)
        self.assertEqual({'test': 'input'}, MockAdapter.message.data)

    @cli_test('--harmony-action', 'invoke', '--harmony-input', '{"test": "input"}')
    def test_when_the_backend_service_doesnt_respond_it_responds_with_an_error(self, parser):
        class MockImpl(MockAdapter):
            def invoke(self):
                self.is_complete = False

        args = parser.parse_args()
        try:
            cli.run_cli(parser, args, MockImpl)
        except:
            pass
        self.assertEqual(MockImpl.error, 'The backend service did not respond')

    @cli_test('--harmony-action', 'invoke', '--harmony-input', '{"test": "input"}')
    def test_when_the_backend_service_throws_an_exception_before_response_it_responds_with_an_error(self, parser):
        class MockImpl(MockAdapter):
            def invoke(self):
                self.is_complete = False
                raise Exception('Something bad happened')

        args = parser.parse_args()
        try:
            cli.run_cli(parser, args, MockImpl)
        except:
            pass
        self.assertEqual(MockImpl.error, 'An unexpected error occurred')

    @cli_test('--harmony-action', 'invoke', '--harmony-input', '{"test": "input"}')
    def test_when_the_backend_service_throws_an_exception_afterresponse_it_does_not_respond_again(self, parser):
        class MockImpl(MockAdapter):
            def invoke(self):
                self.is_complete = True
                raise Exception('Something bad happened')

        args = parser.parse_args()
        try:
            cli.run_cli(parser, args, MockImpl)
        except:
            pass
        self.assertIsNone(MockImpl.error)


if __name__ == '__main__':
    unittest.main()