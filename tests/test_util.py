import unittest

from base64 import b64encode
from nacl.secret import SecretBox
from nacl.utils import random
from unittest.mock import patch, MagicMock, mock_open
import os
import boto3
import pathlib
from urllib.error import HTTPError
from harmony import util

from tests.test_cli import MockAdapter, cli_test
from tests.util import mock_receive

class MockDecode():
  def __init__(self, msg):
    self.message = msg

  def decode(self):
    return self.message

class MockHTTPError(HTTPError):
    def __init__(self, url='http://example.com', code=500, msg='Internal server error', hdrs=[], fp=None):
        super().__init__(url, code, msg, hdrs, fp)
        self.message = msg

    def read(self):
        return MockDecode(self.message)

class TestDownload(unittest.TestCase):
    def setUp(self):
        util._s3 = None

    @patch('boto3.client')
    def test_when_given_an_s3_uri_it_downloads_the_s3_file(self, client):
        s3 = MagicMock()
        client.return_value = s3
        util.download('s3://example/file.txt', 'tmp')
        client.assert_called_with('s3', region_name='us-west-2')
        bucket, path, filename = s3.download_file.call_args[0]
        self.assertEqual(bucket, 'example')
        self.assertEqual(path, 'file.txt')
        self.assertEqual(filename.split('.')[-1], 'txt')

    @patch('urllib.request.urlopen')
    @patch.dict(os.environ, { 'EDL_USERNAME' : 'jdoe', 'EDL_PASSWORD': 'abc' })
    def test_when_given_an_http_url_it_downloads_the_url(self, urlopen):
        mopen = mock_open()
        with patch('builtins.open', mopen):
            util.download('https://example.com/file.txt', 'tmp')
            urlopen.assert_called_with('https://example.com/file.txt')
            mopen.assert_called()

    def test_when_given_a_file_url_it_returns_the_file_path(self):
        self.assertEqual(util.download('file://example/file.txt', 'tmp'), 'example/file.txt')

    def test_when_given_a_file_path_it_returns_the_file_path(self):
        self.assertEqual(util.download('example/file.txt', 'tmp'), 'example/file.txt')

    @patch('urllib.request.urlopen')
    def test_when_the_url_returns_a_401_it_throws_a_forbidden_exception(self, urlopen):
        url = 'https://example.com/file.txt'
        urlopen.side_effect = MockHTTPError(url=url, code=401, msg='Forbidden 401 message')
        with self.assertRaises(util.ForbiddenException) as cm:
          util.download(url, 'tmp')
          self.fail('An exception should have been raised')
        self.assertEqual(str(cm.exception), 'Forbidden 401 message')

    @patch('urllib.request.urlopen')
    def test_when_the_url_returns_a_403_it_throws_a_forbidden_exception(self, urlopen):
        url = 'https://example.com/file.txt'
        urlopen.side_effect = MockHTTPError(url=url, code=403, msg='Forbidden 403 message')
        with self.assertRaises(util.ForbiddenException) as cm:
          util.download(url, 'tmp')
          self.fail('An exception should have been raised')
        self.assertEqual(str(cm.exception), 'Forbidden 403 message')

    @patch('urllib.request.urlopen')
    def test_when_the_url_returns_a_eula_error_it_returns_a_human_readable_message(self, urlopen):
        url = 'https://example.com/file.txt'
        urlopen.side_effect = MockHTTPError(url=url, code=403, msg='{"status_code":403,"error_description":"EULA Acceptance Failure","resolution_url":"https://example.com/approve_app?client_id=foo"}')
        with self.assertRaises(util.ForbiddenException) as cm:
          util.download(url, 'tmp')
          self.fail('An exception should have been raised')
        self.assertEqual(str(cm.exception), 'Request could not be completed because you need to agree to the EULA at https://example.com/approve_app?client_id=foo')

    @patch('urllib.request.urlopen')
    def test_when_the_url_returns_a_500_it_does_not_raise_a_forbidden_exception_and_does_not_return_details_to_user(self, urlopen):
        url = 'https://example.com/file.txt'
        urlopen.side_effect = MockHTTPError(url=url, code=500)
        try:
          util.download(url, 'tmp')
          self.fail('An exception should have been raised')
        except util.ForbiddenException:
          self.fail('ForbiddenException raised when it should not have')
        except Exception:
          pass


class TestDecrypter(unittest.TestCase):
    def test_when_using_nop_decrypter_the_plaintext_is_the_same_as_cyphertext(self):
        decrypter = util.nop_decrypter
        cyphertext = 'This is a terribly encrypted message.'
        expected = cyphertext

        actual = decrypter(cyphertext)

        self.assertEqual(actual, expected)

    def test_when_encrypting_with_a_key_the_decrypter_works_when_using_the_shared_key(self):
        nonce = random(SecretBox.NONCE_SIZE)
        shared_key = random(SecretBox.KEY_SIZE)
        box = SecretBox(shared_key)
        plaintext = 'The ship has arrived at the port'
        encrypted_msg = box.encrypt(bytes(plaintext, 'utf-8'), nonce)
        nonce_str = b64encode(encrypted_msg.nonce).decode("utf-8")
        encrypted_msg_str = b64encode(encrypted_msg.ciphertext).decode("utf-8")
        message = f'{nonce_str}:{encrypted_msg_str}'

        decrypter = util.create_decrypter(shared_key)
        decrypted_text = decrypter(message)

        self.assertNotEqual(plaintext, encrypted_msg.ciphertext)
        self.assertEqual(plaintext, decrypted_text)

    def test_when_encrypting_with_a_key_the_decrypter_fails_when_not_using_the_shared_key(self):
        nonce = random(SecretBox.NONCE_SIZE)
        shared_key = random(SecretBox.KEY_SIZE)
        box = SecretBox(shared_key)
        plaintext = b'The ship has arrived at the port'
        encrypted_msg = box.encrypt(plaintext, nonce)
        nonce_str = b64encode(encrypted_msg.nonce).decode("utf-8")
        encrypted_msg_str = b64encode(encrypted_msg.ciphertext).decode("utf-8")
        message = f'{nonce_str}:{encrypted_msg_str}'

        incorrect_key = random(SecretBox.KEY_SIZE)
        decrypter = util.create_decrypter(incorrect_key)
        with self.assertRaises(Exception):
            decrypter(message)

        self.assertNotEqual(plaintext, encrypted_msg.ciphertext)

class TestStage(unittest.TestCase):
    @patch('boto3.client')
    @patch.dict(os.environ, { 'STAGING_BUCKET': 'example', 'STAGING_PATH' : 'staging/path', 'ENV' : 'not_test_we_swear' })
    def test_uploads_to_s3_and_returns_its_s3_url(self, client):
        # Sets a non-test ENV environment variable to force things through the (mocked) download path
        s3 = MagicMock()
        s3.generate_presigned_url.return_value = 'https://example.com/presigned.txt'
        client.return_value = s3
        result = util.stage('file.txt', 'remote.txt', 'text/plain')
        s3.upload_file.assert_called_with('file.txt', 'example', 'staging/path/remote.txt', ExtraArgs={'ContentType': 'text/plain'})
        self.assertEqual(result, 's3://example/staging/path/remote.txt')

    @patch('boto3.client')
    @patch.dict(os.environ, { 'STAGING_BUCKET': 'example', 'STAGING_PATH' : 'staging/path', 'ENV' : 'not_test_we_swear' })
    def test_uses_location_prefix_when_provided(self, client):
        # Sets a non-test ENV environment variable to force things through the (mocked) download path
        s3 = MagicMock()
        s3.generate_presigned_url.return_value = 'https://example.com/presigned.txt'
        client.return_value = s3
        result = util.stage('file.txt', 'remote.txt', 'text/plain', location="s3://different-example/public/location/")
        s3.upload_file.assert_called_with('file.txt', 'different-example', 'public/location/remote.txt', ExtraArgs={'ContentType': 'text/plain'})
        self.assertEqual(result, 's3://different-example/public/location/remote.txt')

class TestS3Parameters(unittest.TestCase):
    def test_when_using_localstack_it_uses_localstack_host(self):
        use_localstack = True
        localstack_host = 'testhost'
        region = 'tatooine-desert-1'

        expected = {
            'endpoint_url': f'http://{localstack_host}:4566',
            'use_ssl': False,
            'aws_access_key_id': 'ACCESS_KEY',
            'aws_secret_access_key': 'SECRET_KEY',
            'region_name': f'{region}'
        }

        actual = util._aws_parameters(use_localstack, localstack_host, region)
        self.assertDictEqual(expected, actual)

    def test_when_not_using_localstack_it_ignores_localstack_host(self):
        use_localstack = False
        localstack_host = 'localstack'
        region = 'westeros-north-3'

        expected = {
            'region_name': f'{region}'
        }

        actual = util._aws_parameters(use_localstack, localstack_host, region)

        self.assertDictEqual(expected, actual)

class TestSQSReadHealthUpdate(unittest.TestCase):
    @cli_test('--harmony-action', 'start', '--harmony-queue-url', 'test-queue-url')
    @patch('boto3.client')
    @patch.object(pathlib.Path, '__new__')
    def test_when_reading_from_queue_health_update_happens(self, parser, mock_path, client):
        all_test_cases = [
            # message received
            ['{"test": "a"}'],

            # no message received
            [None],

            # error receiving message
            [Exception()]
        ]
        for messages in all_test_cases:
            with self.subTest(messages=messages):
                try:
                    mock_receive(client, parser, MockAdapter, *messages)
                except Exception:
                    pass
                mock_path.return_value.touch.assert_called()
