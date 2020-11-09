from base64 import b64encode
import os
import pathlib
import unittest
from unittest.mock import mock_open, patch, MagicMock, Mock
from urllib.error import HTTPError
from urllib.parse import urlencode

from nacl.secret import SecretBox
from nacl.utils import random
from parameterized import parameterized

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


class TestRequests(unittest.TestCase):
    def test_when_provided_an_access_token_it_creates_a_proper_auth_header(self):
        access_token = 'AHIGHLYRANDOMSTRING'
        expected = ('Authorization', f'Bearer {access_token}')

        actual = util._bearer_token_auth_header(access_token)

        self.assertEqual(expected, actual)

    def test_when_provided_an_access_token_it_creates_a_nonredirectable_auth_header(self):
        url = 'https://example.com/file.txt'
        access_token='OPENSESAME'
        expected_header = dict([util._bearer_token_auth_header(access_token)])

        actual_request = util._request_with_bearer_token_auth_header(url, access_token, None)

        self.assertFalse(expected_header.items() <= actual_request.headers.items())
        self.assertTrue(expected_header.items() <= actual_request.unredirected_hdrs.items())


class TestDownload(unittest.TestCase):
    def setUp(self):
        util._s3 = None

    @patch('boto3.client')
    def test_when_given_an_s3_uri_it_downloads_the_s3_file(self, client):
        s3 = MagicMock()
        client.return_value = s3

        util.download('s3://example/file.txt', 'tmp', access_token='FOO')

        client.assert_called_with('s3', region_name='us-west-2')
        bucket, path, filename = s3.download_file.call_args[0]
        self.assertEqual(bucket, 'example')
        self.assertEqual(path, 'file.txt')
        self.assertEqual(filename.split('.')[-1], 'txt')

    def _verify_urlopen(self, url, access_token, data, urlopen, expected_urlopen_calls=1, verify_bearer_token=True):
        """Verify that the urlopen function was called with the correct Request values."""

        # In some error cases, we expect urlopen to be called more than once
        self.assertEqual(urlopen.call_count, expected_urlopen_calls)

        # Verify that we have a request argument with the correct url
        args = urlopen.call_args.args
        self.assertTrue(len(args), 1)
        request = args[0]
        self.assertEqual(request.full_url, url)

        if access_token is not None and verify_bearer_token:
            # Verify that the request has a bearer token auth header
            # that's not redirectable, and no other headers.
            expected_header = dict([util._bearer_token_auth_header(access_token)])
            self.assertFalse(expected_header.items() <= request.headers.items())
            self.assertTrue(expected_header.items() <= request.unredirected_hdrs.items())
        else:
            # We should not have any headers
            self.assertEqual(len(request.headers), 0)
            self.assertEqual(len(request.unredirected_hdrs), 0)

        # If we've got data to POST, verify it's in the request
        if data is not None:
            self.assertEqual(urlencode(data).encode('utf-8'), request.data)

    @parameterized.expand([('with_access_token', 'OPENSESAME'), ('without_access_token', None)])
    @patch('urllib.request.OpenerDirector.open')
    @patch.dict(os.environ, { 'EDL_USERNAME' : 'jdoe', 'EDL_PASSWORD': 'abc' })
    def test_when_given_an_http_url_it_downloads_the_url(self, name, access_token, urlopen):
        url = 'https://example.com/file.txt'

        mopen = mock_open()
        with patch('builtins.open', mopen):
            util.download(url, 'tmp', access_token=access_token)

            self._verify_urlopen(url, access_token, None, urlopen)
            mopen.assert_called()

    @parameterized.expand([('with_access_token', 'OPENSESAME'), ('without_access_token', None)])
    @patch('urllib.request.OpenerDirector.open')
    @patch.dict(os.environ, {'EDL_USERNAME': 'jdoe', 'EDL_PASSWORD': 'abc'})
    def test_when_given_a_url_and_data_it_downloads_with_query_string(self, name, access_token, urlopen):
        url = 'https://example.com/file.txt'
        data = {'param': 'value'}

        mopen = mock_open()
        with patch('builtins.open', mopen):
            util.download(url, 'tmp', access_token=access_token, data=data)
            self._verify_urlopen(url, access_token, data, urlopen)
            mopen.assert_called()

    @parameterized.expand([('with_access_token', 'OPENSESAME'), ('without_access_token', None)])
    def test_when_given_a_file_url_it_returns_the_file_path(self, name, access_token):
        self.assertEqual(util.download('file://example/file.txt', 'tmp', access_token=access_token),
                         'example/file.txt')

    @parameterized.expand([('with_access_token', 'OPENSESAME'), ('without_access_token', None)])
    def test_when_given_a_file_path_it_returns_the_file_path(self, name, access_token):
        self.assertEqual(util.download('example/file.txt', 'tmp', access_token=access_token),
                         'example/file.txt')

    @parameterized.expand([('with_access_token', 'OPENSESAME'), ('without_access_token', None)])
    @patch('urllib.request.OpenerDirector.open')
    def test_when_the_url_returns_a_401_it_throws_a_forbidden_exception(self, name, access_token, urlopen):
        url = 'https://example.com/file.txt'

        urlopen.side_effect = MockHTTPError(url=url, code=401, msg='Forbidden 401 message')

        with self.assertRaises(util.ForbiddenException) as cm:
            util.download(url, 'tmp', access_token=access_token)
            self.fail('An exception should have been raised')
        self.assertEqual(str(cm.exception), 'Forbidden 401 message')

    @parameterized.expand([('with_access_token', 'OPENSESAME'), ('without_access_token', None)])
    @patch('urllib.request.OpenerDirector.open')
    def test_when_the_url_returns_a_403_it_throws_a_forbidden_exception(self, name, access_token, urlopen):
        url = 'https://example.com/file.txt'

        urlopen.side_effect = MockHTTPError(url=url, code=403, msg='Forbidden 403 message')

        with self.assertRaises(util.ForbiddenException) as cm:
            util.download(url, 'tmp', access_token=access_token)
            self.fail('An exception should have been raised')
        self.assertEqual(str(cm.exception), 'Forbidden 403 message')

    @parameterized.expand([('with_access_token', 'OPENSESAME'), ('without_access_token', None)])
    @patch('urllib.request.OpenerDirector.open')
    def test_when_the_url_returns_a_eula_error_it_returns_a_human_readable_message(self, name, access_token, urlopen):
        url = 'https://example.com/file.txt'

        urlopen.side_effect = \
            MockHTTPError(
              url=url,
              code=403,
              msg=('{"status_code":403,"error_description":"EULA Acceptance Failure","resolution_url":"https://example.com/approve_app?client_id=foo"}')
            )

        with self.assertRaises(util.ForbiddenException) as cm:
            util.download(url, 'tmp', access_token=access_token)
            self.fail('An exception should have been raised')
        self.assertEqual(str(cm.exception), 'Request could not be completed because you need to agree to the EULA at https://example.com/approve_app?client_id=foo')

    @parameterized.expand([('with_access_token', 'OPENSESAME'), ('without_access_token', None)])
    @patch('urllib.request.OpenerDirector.open')
    def test_when_the_url_returns_a_500_it_does_not_raise_a_forbidden_exception_and_does_not_return_details_to_user(self, name, access_token, urlopen):
        url = 'https://example.com/file.txt'

        urlopen.side_effect = MockHTTPError(url=url, code=500)

        try:
            util.download(url, 'tmp', access_token=access_token)
            self.fail('An exception should have been raised')
        except util.ForbiddenException:
            self.fail('ForbiddenException raised when it should not have')
        except Exception as e:
            pass

    @patch('urllib.request.OpenerDirector.open')
    def test_when_given_an_access_token_and_the_url_returns_an_error_it_falls_back_to_basic_auth(self, urlopen):
        url = 'https://example.com/file.txt'
        access_token='OPENSESAME'
        urlopen.side_effect = [MockHTTPError(url=url, code=400, msg='Forbidden 400 message'), Mock()]

        with patch('builtins.open'):
            util.download(url, 'tmp', access_token=access_token)

            self._verify_urlopen(url, access_token, None, urlopen, expected_urlopen_calls=2, verify_bearer_token=False)

    @patch('urllib.request.OpenerDirector.open')
    def test_when_no_access_token_is_provided_it_uses_basic_auth_and_downloads(self, urlopen):
        url = 'https://example.com/file.txt'
        access_token = None

        with patch('builtins.open'):
            util.download(url, 'tmp')

            self._verify_urlopen(url, access_token, None, urlopen)


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

class TestGenerateOutputFilename(unittest.TestCase):
    def test_includes_provided_regridded_subsetted_ext(self):
        url = 'https://example.com/fake-path/abc.123.nc/?query=true'
        ext = 'zarr'

        # Basic cases
        variables = []
        self.assertEqual(util.generate_output_filename(url, ext), 'abc.123.zarr')
        self.assertEqual(util.generate_output_filename(url, ext, is_subsetted=True), 'abc.123_subsetted.zarr')
        self.assertEqual(util.generate_output_filename(url, ext, is_regridded=True), 'abc.123_regridded.zarr')
        self.assertEqual(util.generate_output_filename(url, ext, is_subsetted=True, is_regridded=True), 'abc.123_regridded_subsetted.zarr')
        self.assertEqual(util.generate_output_filename(url, ext, variable_subset=variables, is_subsetted=True, is_regridded=True), 'abc.123_regridded_subsetted.zarr')

    def test_includes_single_variable_name_replacing_slashes(self):
        url = 'https://example.com/fake-path/abc.123.nc/?query=true'
        ext = 'zarr'

        # Variable name contains full path with '/' ('/' replaced with '_')
        variables = ['/path/to/VarB']
        self.assertEqual(util.generate_output_filename(url, ext, variable_subset=variables, is_subsetted=True, is_regridded=True), 'abc.123__path_to_VarB_regridded_subsetted.zarr')

    def test_includes_single_variable(self):
        url = 'https://example.com/fake-path/abc.123.nc/?query=true'
        ext = 'zarr'

        # Single variable cases
        variables = ['VarA']
        self.assertEqual(util.generate_output_filename(url, ext), 'abc.123.zarr')
        self.assertEqual(util.generate_output_filename(url, ext, is_subsetted=True, is_regridded=True), 'abc.123_regridded_subsetted.zarr')
        self.assertEqual(util.generate_output_filename(url, ext, variable_subset=variables), 'abc.123_VarA.zarr')
        self.assertEqual(util.generate_output_filename(url, ext, variable_subset=variables, is_subsetted=True, is_regridded=True), 'abc.123_VarA_regridded_subsetted.zarr')

    def test_excludes_multiple_variable(self):
        url = 'https://example.com/fake-path/abc.123.nc/?query=true'
        ext = 'zarr'

        # Multiple variable cases (no variable name in suffix)
        variables = ['VarA', 'VarB']
        self.assertEqual(util.generate_output_filename(url, ext, is_subsetted=True, is_regridded=True), 'abc.123_regridded_subsetted.zarr')
        self.assertEqual(util.generate_output_filename(url, ext, variable_subset=variables, is_subsetted=True, is_regridded=True), 'abc.123_regridded_subsetted.zarr')

    def test_avoids_overwriting_single_suffixes(self):
        ext = 'zarr'

        # URL already containing a suffix
        variables = ['VarA']
        url = 'https://example.com/fake-path/abc.123_regridded.zarr'
        self.assertEqual(util.generate_output_filename(url, ext, is_subsetted=True), 'abc.123_regridded_subsetted.zarr')
        self.assertEqual(util.generate_output_filename(url, ext, variable_subset=variables, is_subsetted=True, is_regridded=True), 'abc.123_VarA_regridded_subsetted.zarr')

    def test_avoids_overwriting_multiple_suffixes(self):
        ext = 'zarr'
        # URL already containing all suffixes
        variables = ['VarA']
        url = 'https://example.com/fake-path/abc.123_VarA_regridded_subsetted.zarr'
        self.assertEqual(util.generate_output_filename(url, ext, variable_subset=variables, is_subsetted=True, is_regridded=True), 'abc.123_VarA_regridded_subsetted.zarr')
