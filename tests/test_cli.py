import os
import unittest
from unittest.mock import patch

import harmony.util
from harmony import cli, BaseHarmonyAdapter
from tests.util import mock_receive, cli_test


class MockAdapter(BaseHarmonyAdapter):
    """
    Dummy class to mock adapter calls and record the input messages
    """
    messages = []
    errors = []
    cleaned_up = []

    def __init__(self, message):
        super().__init__(self, message)
        MockAdapter.messages.append(message.data)

    def invoke(self):
        self.is_complete = True
        self.is_failed = False

    def completed_with_error(self, error):
        MockAdapter.errors.append(error)

    def cleanup(self):
        MockAdapter.cleaned_up.append(True)


class TestIsHarmonyCli(unittest.TestCase):
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


class TestCliInvokeAction(unittest.TestCase):
    def setUp(self):
        self.config = harmony.util.config(validate=False)
        with open('/tmp/operation.json', 'w') as f:
            f.write('{"test": "input"}')

    def tearDown(self):
        os.remove('/tmp/operation.json')
        MockAdapter.messages = []
        MockAdapter.errors = []
        MockAdapter.cleaned_up = []

    @cli_test('--harmony-action', 'invoke')
    def test_when_harmony_input_is_not_provided_it_terminates_with_error(self, parser):
        with patch.object(parser, 'error') as error_method:
            args = parser.parse_args()
            cli.run_cli(parser, args, MockAdapter, self.config)
            error_method.assert_called_once_with(
                '--harmony-input or --harmony-input-file must be provided for --harmony-action=invoke')

    @cli_test('--harmony-action', 'invoke', '--harmony-input', '{"test": "input"}')
    def test_when_harmony_input_is_provided_it_creates_and_invokes_an_adapter(self, parser):
        args = parser.parse_args()
        cli.run_cli(parser, args, MockAdapter, self.config)
        self.assertListEqual([{'test': 'input'}], MockAdapter.messages)

    @cli_test('--harmony-action', 'invoke', '--harmony-input-file', '/tmp/operation.json')
    def test_when_harmony_input_file_is_provided_it_creates_and_invokes_an_adapter(self, parser):
        args = parser.parse_args()

        cli.run_cli(parser, args, MockAdapter, self.config)
        self.assertListEqual([{'test': 'input'}], MockAdapter.messages)

    @cli_test('--harmony-action', 'invoke', '--harmony-input', '{"test": "input"}')
    def test_when_the_backend_service_doesnt_respond_it_responds_with_an_error(self, parser):
        class MockImpl(MockAdapter):
            def invoke(self):
                self.is_complete = False

        args = parser.parse_args()
        try:
            cli.run_cli(parser, args, MockImpl, self.config)
        except Exception:
            pass
        self.assertListEqual(
            MockImpl.errors, ['The backend service did not respond'])

    @cli_test('--harmony-action', 'invoke', '--harmony-input', '{"test": "input"}')
    def test_when_the_backend_service_throws_an_exception_before_response_it_responds_with_an_error(self, parser):
        class MockImpl(MockAdapter):
            def invoke(self):
                self.is_complete = False
                raise Exception('Something bad happened')

        args = parser.parse_args()
        with self.assertRaises(Exception) as context:
            cli.run_cli(parser, args, MockImpl, self.config)

        self.assertTrue('Something bad happened' in str(context.exception))
        self.assertListEqual(
            MockImpl.errors, ['Service request failed with an unknown error'])

    @cli_test('--harmony-action', 'invoke', '--harmony-input', '{"test": "input"}')
    def test_when_the_backend_service_throws_an_exception_afterresponse_it_does_not_respond_again(self, parser):
        class MockImpl(MockAdapter):
            def invoke(self):
                self.is_complete = True
                raise Exception('Something bad happened')

        args = parser.parse_args()
        try:
            cli.run_cli(parser, args, MockImpl, self.config)
        except Exception:
            pass
        self.assertListEqual(MockImpl.errors, [])


class TestCliStartAction(unittest.TestCase):
    def setUp(self):
        self.config = harmony.util.config(validate=False)

    def tearDown(self):
        MockAdapter.messages = []
        MockAdapter.errors = []
        MockAdapter.cleaned_up = []

    @cli_test('--harmony-action', 'start')
    def test_when_queue_url_is_not_provided_it_terminates_with_error(self, parser):
        with patch.object(parser, 'error') as error_method:
            args = parser.parse_args()
            cli.run_cli(parser, args, MockAdapter, self.config)
            error_method.assert_called_once_with(
                '--harmony-queue-url must be provided for --harmony-action=start')

    @cli_test('--harmony-action', 'start', '--harmony-queue-url', 'test-queue-url')
    @patch('boto3.client')
    def test_listens_on_the_provided_queue(self, parser, client):
        sqs = mock_receive(self.config, client, parser, MockAdapter)
        sqs.receive_message.assert_called_with(
            QueueUrl='test-queue-url',
            VisibilityTimeout=600,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1)
        self.assertListEqual(MockAdapter.messages, [])
        self.assertListEqual(MockAdapter.errors, [])

    @cli_test('--harmony-action', 'start',
              '--harmony-queue-url', 'test-queue-url',
              '--harmony-visibility-timeout', '100')
    @patch('boto3.client')
    def test_uses_optional_visibility_timeouts_from_the_command_line(self, parser, client):
        sqs = mock_receive(self.config, client, parser, MockAdapter)
        sqs.receive_message.assert_called_with(
            QueueUrl='test-queue-url',
            VisibilityTimeout=100,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1)
        self.assertListEqual(MockAdapter.messages, [])
        self.assertListEqual(MockAdapter.errors, [])

    @cli_test('--harmony-action', 'start', '--harmony-queue-url', 'test-queue-url')
    @patch('boto3.client')
    def test_sends_queue_messages_to_the_adapter(self, parser, client):
        mock_receive(self.config, client, parser, MockAdapter,
                     '{"test": "a"}', None, '{"test": "b"}')
        self.assertEqual(MockAdapter.messages, [{'test': 'a'}, {'test': 'b'}])

    @cli_test('--harmony-action', 'start', '--harmony-queue-url', 'test-queue-url')
    @patch('boto3.client')
    def test_when_the_adapter_completes_the_request_it_deletes_the_queue_message(self, parser, client):
        sqs = mock_receive(self.config, client, parser, MockAdapter,
                           '{"test": "a"}', None, '{"test": "b"}')
        sqs.delete_message.assert_called_with(
            QueueUrl='test-queue-url',
            ReceiptHandle=2)

    @cli_test('--harmony-action', 'start', '--harmony-queue-url', 'test-queue-url')
    @patch('boto3.client')
    def test_when_the_adapter_completes_the_request_it_calls_cleanup_on_the_adapter(self, parser, client):
        mock_receive(self.config, client, parser, MockAdapter,
                     '{"test": "a"}', None, '{"test": "b"}')
        self.assertListEqual(MockAdapter.cleaned_up, [True, True])

    @cli_test('--harmony-action', 'start', '--harmony-queue-url', 'test-queue-url')
    @patch('boto3.client')
    def test_when_the_adapter_runs_without_completing_the_request_it_returns_the_message_to_the_queue(
        self, parser, client
    ):
        class MockImpl(MockAdapter):
            def invoke(self):
                self.is_complete = False

        sqs = mock_receive(self.config, client, parser, MockImpl,
                           '{"test": "a"}', None, '{"test": "b"}')
        sqs.delete_message.assert_not_called()
        sqs.change_message_visibility.assert_called_with(
            QueueUrl='test-queue-url',
            VisibilityTimeout=0,
            ReceiptHandle=2)

    @cli_test('--harmony-action', 'start', '--harmony-queue-url', 'test-queue-url')
    @patch('boto3.client')
    def test_when_the_adapter_runs_without_completing_the_request_it_calls_cleanup_on_the_adapter(self, parser, client):
        class MockImpl(MockAdapter):
            def invoke(self):
                self.is_complete = False

        mock_receive(self.config, client, parser, MockImpl, '{"test": "a"}')
        self.assertListEqual(MockImpl.cleaned_up, [True])

    @cli_test('--harmony-action', 'start', '--harmony-queue-url', 'test-queue-url')
    @patch('boto3.client')
    def test_when_the_adapter_throws_before_completing_the_request_it_returns_the_message_to_the_queue(
        self, parser, client
    ):
        class MockImpl(MockAdapter):
            def invoke(self):
                self.is_complete = False
                raise Exception('Something bad happened')

        sqs = mock_receive(self.config, client, parser, MockImpl,
                           '{"test": "a"}', None, '{"test": "b"}')
        sqs.delete_message.assert_not_called()
        sqs.change_message_visibility.assert_called_with(
            QueueUrl='test-queue-url',
            VisibilityTimeout=0,
            ReceiptHandle=2)

        self.assertListEqual(MockImpl.cleaned_up, [True, True])

    @cli_test('--harmony-action', 'start', '--harmony-queue-url', 'test-queue-url')
    @patch('boto3.client')
    def test_when_the_adapter_throws_after_completing_the_request_it_deletes_the_queue_message(self, parser, client):
        class MockImpl(MockAdapter):
            def invoke(self):
                self.is_complete = True
                raise Exception('Something bad happened')

        sqs = mock_receive(self.config, client, parser, MockImpl,
                           '{"test": "a"}', None, '{"test": "b"}')
        sqs.delete_message.assert_called_with(
            QueueUrl='test-queue-url',
            ReceiptHandle=2)
        sqs.change_message_visibility.assert_not_called()

    @cli_test('--harmony-action', 'start', '--harmony-queue-url', 'test-queue-url')
    @patch('boto3.client')
    def test_when_the_adapter_throws_it_calls_cleanup_on_the_adapter(self, parser, client):
        class MockImpl(MockAdapter):
            def invoke(self):
                self.is_complete = False
                raise Exception('Something bad happened')

        mock_receive(self.config, client, parser, MockImpl, '{"test": "a"}', None, '{"test": "b"}')
        self.assertListEqual(MockImpl.cleaned_up, [True, True])

    @cli_test('--harmony-action', 'start', '--harmony-queue-url', 'test-queue-url')
    @patch('boto3.client')
    def test_when_cleanup_throws_it_continues_processing_queue_messages(self, parser, client):
        class MockImpl(MockAdapter):
            def cleanup(self):
                raise Exception('Something bad happened')

        mock_receive(self.config, client, parser, MockImpl, '{"test": "a"}', None, '{"test": "b"}')
        self.assertEqual(MockAdapter.messages, [{'test': 'a'}, {'test': 'b'}])


if __name__ == '__main__':
    unittest.main()
