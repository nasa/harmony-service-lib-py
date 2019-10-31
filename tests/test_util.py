import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import boto3
from harmony import util

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
    @patch.dict(os.environ, { 'EDL_ENDPOINT': 'https://example.com', 'EDL_USERNAME' : 'jdoe', 'EDL_PASSWORD': 'abc' })
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
    def test_uploads_to_s3_and_returns_a_presigned_url(self, client):
        # Sets a non-test ENV environment variable to force things through the (mocked) download path
        s3 = MagicMock()
        s3.generate_presigned_url.return_value = 'https://example.com/presigned.txt'
        client.return_value = s3
        result = util.stage('file.txt', 'remote.txt', 'text/plain')
        s3.upload_file.assert_called_with('file.txt', 'example', 'staging/path/remote.txt', ExtraArgs={'ContentType': 'text/plain'})
        s3.generate_presigned_url.assert_called_with('get_object', Params={'Bucket': 'example', 'Key': 'staging/path/remote.txt'})
        self.assertEqual(result, 'https://example.com/presigned.txt')
