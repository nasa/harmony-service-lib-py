import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import boto3
import pathlib
from harmony import util
from tests.test_cli import MockAdapter, cli_test
from tests.util import mock_receive

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
