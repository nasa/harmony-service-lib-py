import pathlib
import unittest
from unittest.mock import patch, MagicMock
from urllib.error import HTTPError

from harmony import aws
from harmony import util
from harmony.message import Variable
from tests.test_cli import MockAdapter, cli_test
from tests.util import mock_receive, config_fixture


class TestDownload(unittest.TestCase):
    def setUp(self):
        util._s3 = None
        self.config = config_fixture()


class TestStage(unittest.TestCase):
    def setUp(self):
        self.config = util.config(validate=False)

    @patch('boto3.client')
    def test_uploads_to_s3_and_returns_its_s3_url(self, client):
        # Sets a non-test ENV environment variable to force things through the (mocked) download path
        s3 = MagicMock()
        s3.generate_presigned_url.return_value = 'https://example.com/presigned.txt'
        client.return_value = s3
        cfg = config_fixture(use_localstack=True, staging_bucket='example', staging_path='staging/path')

        result = util.stage('file.txt', 'remote.txt', 'text/plain', cfg=cfg)

        s3.upload_file.assert_called_with('file.txt', 'example', 'staging/path/remote.txt',
                                          ExtraArgs={'ContentType': 'text/plain'})
        self.assertEqual(result, 's3://example/staging/path/remote.txt')

    @patch('boto3.client')
    def test_uses_location_prefix_when_provided(self, client):
        # Sets a non-test ENV environment variable to force things through the (mocked) download path
        s3 = MagicMock()
        s3.generate_presigned_url.return_value = 'https://example.com/presigned.txt'
        client.return_value = s3
        cfg = config_fixture(use_localstack=True, staging_bucket='example', staging_path='staging/path')

        result = util.stage('file.txt', 'remote.txt', 'text/plain',
                            location="s3://different-example/public/location/", cfg=cfg)

        s3.upload_file.assert_called_with('file.txt', 'different-example', 'public/location/remote.txt',
                                          ExtraArgs={'ContentType': 'text/plain'})
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

        actual = aws._aws_parameters(use_localstack, localstack_host, region)
        self.assertDictEqual(expected, actual)

    def test_when_not_using_localstack_it_ignores_localstack_host(self):
        use_localstack = False
        localstack_host = 'localstack'
        region = 'westeros-north-3'

        expected = {
            'region_name': f'{region}'
        }

        actual = aws._aws_parameters(use_localstack, localstack_host, region)

        self.assertDictEqual(expected, actual)


class TestSQSReadHealthUpdate(unittest.TestCase):
    def setUp(self):
        self.config = util.config(validate=False)

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
                    mock_receive(self.config, client, parser, MockAdapter, *messages)
                except Exception:
                    pass
                mock_path.return_value.touch.assert_called()


class TestGenerateOutputFilename(unittest.TestCase):
    def test_includes_provided_regridded_subsetted_ext(self):
        url = 'https://example.com/fake-path/abc.123.nc/?query=true'
        ext = 'zarr'

        # Basic cases
        variables = []
        self.assertEqual(
            util.generate_output_filename(url, ext),
            'abc.123.zarr'
        )
        self.assertEqual(
            util.generate_output_filename(url, ext, is_subsetted=True),
            'abc.123_subsetted.zarr'
        )
        self.assertEqual(
            util.generate_output_filename(url, ext, is_regridded=True),
            'abc.123_regridded.zarr'
        )
        self.assertEqual(
            util.generate_output_filename(url, ext, is_subsetted=True, is_regridded=True),
            'abc.123_regridded_subsetted.zarr'
        )
        self.assertEqual(
            util.generate_output_filename(url, ext, variable_subset=variables, is_subsetted=True, is_regridded=True),
            'abc.123_regridded_subsetted.zarr'
        )

    def test_includes_single_variable_name_replacing_slashes(self):
        url = 'https://example.com/fake-path/abc.123.nc/?query=true'
        ext = 'zarr'

        # Variable name contains full path with '/' ('/' replaced with '_')
        variables = ['/path/to/VarB']
        self.assertEqual(
            util.generate_output_filename(url, ext, variable_subset=variables, is_subsetted=True, is_regridded=True),
            'abc.123__path_to_VarB_regridded_subsetted.zarr'
        )

    def test_includes_single_variable(self):
        url = 'https://example.com/fake-path/abc.123.nc/?query=true'
        ext = 'zarr'

        # Single variable cases
        variables = ['VarA']
        self.assertEqual(
            util.generate_output_filename(url, ext),
            'abc.123.zarr'
        )
        self.assertEqual(
            util.generate_output_filename(url, ext, is_subsetted=True, is_regridded=True),
            'abc.123_regridded_subsetted.zarr'
        )
        self.assertEqual(
            util.generate_output_filename(url, ext, variable_subset=variables),
            'abc.123_VarA.zarr'
        )
        self.assertEqual(
            util.generate_output_filename(url, ext, variable_subset=variables, is_subsetted=True, is_regridded=True),
            'abc.123_VarA_regridded_subsetted.zarr'
        )

    def test_excludes_multiple_variable(self):
        url = 'https://example.com/fake-path/abc.123.nc/?query=true'
        ext = 'zarr'

        # Multiple variable cases (no variable name in suffix)
        variables = ['VarA', 'VarB']
        self.assertEqual(
            util.generate_output_filename(url, ext, is_subsetted=True, is_regridded=True),
            'abc.123_regridded_subsetted.zarr'
        )
        self.assertEqual(
            util.generate_output_filename(url, ext, variable_subset=variables, is_subsetted=True, is_regridded=True),
            'abc.123_regridded_subsetted.zarr'
        )

    def test_avoids_overwriting_single_suffixes(self):
        ext = 'zarr'

        # URL already containing a suffix
        variables = ['VarA']
        url = 'https://example.com/fake-path/abc.123_regridded.zarr'
        self.assertEqual(
            util.generate_output_filename(url, ext, is_subsetted=True),
            'abc.123_regridded_subsetted.zarr'
        )
        self.assertEqual(
            util.generate_output_filename(url, ext, variable_subset=variables, is_subsetted=True, is_regridded=True),
            'abc.123_VarA_regridded_subsetted.zarr'
        )

    def test_avoids_overwriting_multiple_suffixes(self):
        ext = 'zarr'
        # URL already containing all suffixes
        variables = ['VarA']
        url = 'https://example.com/fake-path/abc.123_VarA_regridded_subsetted.zarr'
        self.assertEqual(
            util.generate_output_filename(url, ext, variable_subset=variables, is_subsetted=True, is_regridded=True),
            'abc.123_VarA_regridded_subsetted.zarr'
        )

    def test_allows_variable_objects(self):
        ext = 'zarr'
        # URL already containing all suffixes
        variables = [Variable({'name': 'VarA'})]
        url = 'https://example.com/fake-path/abc.123.zarr'
        self.assertEqual(
            util.generate_output_filename(url, ext, variable_subset=variables),
            'abc.123_VarA.zarr'
        )


class TestBboxToGeometry(unittest.TestCase):
    def test_provides_a_single_polygon_for_bboxes_not_crossing_the_antimeridian(self):
        self.assertEqual(
            util.bbox_to_geometry([100, 0, -100, 50]),
            {
                'type': 'MultiPolygon',
                'coordinates': [
                    [[[-180, 0], [-180, 50], [-100, 50], [-100, 0], [-180, 0]]],
                    [[[100, 0], [100, 50], [180, 50], [180, 0], [100, 0]]]
                ]
            })

    def test_splits_bboxes_that_cross_the_antimeridian(self):
        self.assertEqual(
            util.bbox_to_geometry([-100, 0, 100, 50]),
            {
                'type': 'Polygon',
                'coordinates': [
                    [[-100, 0], [-100, 50], [100, 50], [100, 0], [-100, 0]]
                ]
            })
