import unittest
from io import StringIO

from harmony.logging import build_logger
from tests.util import config_fixture
from harmony.message import Message
from .example_messages import minimal_message


class TestLogging(unittest.TestCase):

    def setUp(self):
        self.harmony_message = Message(minimal_message)

    def configure_logger(self, text_logger):
        self.buffer = StringIO()
        self.logger = build_logger(
            config_fixture(text_logger=text_logger), 
            stream=self.buffer)

    def test_msg_token_not_logged(self):
        self.configure_logger(text_logger=False)
        self.logger.info(self.harmony_message)
        log = self.buffer.getvalue()
        assert("accessToken = '<redacted>'" in log)
        assert("ABCD1234567890" not in log) # the access token of minimal_message
        # check the same but with the text logger
        self.configure_logger(text_logger=True)
        self.logger.info(self.harmony_message)
        log = self.buffer.getvalue()
        assert("accessToken = '<redacted>'" in log)
        assert("ABCD1234567890" not in log) # the access token of minimal_message

    def test_arg_token_not_logged(self):
        log_call_arguments = ['the Harmony message is %s', self.harmony_message]
        self.configure_logger(text_logger=False)
        self.logger.info(*log_call_arguments)
        log = self.buffer.getvalue()
        assert("accessToken = '<redacted>'" in log)
        assert("ABCD1234567890" not in log) # the access token of minimal_message
        # check the same but with the text logger
        self.configure_logger(text_logger=True)
        self.logger.info(*log_call_arguments)
        log = self.buffer.getvalue()
        assert("accessToken = '<redacted>'" in log)
        assert("ABCD1234567890" not in log) # the access token of minimal_message

    def test_dict_token_not_logged(self):
        log_call_arguments = ['the Harmony message is %s', { 'the_harmony_message': self.harmony_message }]
        self.configure_logger(text_logger=False)
        self.logger.info(*log_call_arguments)
        log = self.buffer.getvalue()
        assert("accessToken = '<redacted>'" in log)
        assert("ABCD1234567890" not in log) # the access token of minimal_message
        # check the same but with the text logger
        self.configure_logger(text_logger=True)
        self.logger.info(*log_call_arguments)
        log = self.buffer.getvalue()
        assert("accessToken = '<redacted>'" in log)
        assert("ABCD1234567890" not in log) # the access token of minimal_message
        
        