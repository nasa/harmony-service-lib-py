"""
Tests deprecated methods and non-STAC invocation styles on BaseHarmonyAdapter
"""

import unittest
from unittest.mock import patch, MagicMock
from tempfile import NamedTemporaryFile, mkdtemp
import os
from os import path, remove
import pathlib
from shutil import rmtree
from urllib.error import HTTPError

from harmony.adapter import BaseHarmonyAdapter
from harmony.message import Message, Granule, Variable, Temporal
import harmony.util
from harmony.exceptions import CanceledException
from .example_messages import minimal_message, full_message


# BaseHarmonyAdapter is abstract, so tests need a minimal concrete class
class AdapterTester(BaseHarmonyAdapter):
    def __init__(self, message_str, config):
        super().__init__(Message(message_str), config=config)

    def invoke(self):
        pass


class MockHTTPError(HTTPError):
    def __init__(self, url='http://example.com', code=409, msg='Harmony canceled request', hdrs=[], fp=None):
        super().__init__(url, code, msg, hdrs, fp)

    def read(self):
        return MagicMock(return_value='request body')


class TestBaseHarmonyAdapter(unittest.TestCase):
    def setUp(self):
        self.config = harmony.util.config(validate=False)

    def test_cleanup_deletes_temporary_file_paths(self):
        adapter = AdapterTester(minimal_message, self.config)
        f = NamedTemporaryFile(delete=False)
        try:
            f.close()
            adapter.temp_paths += [f.name]
            self.assertTrue(path.exists(f.name))

            adapter.cleanup()

            self.assertFalse(path.exists(f.name))
        finally:
            if path.exists(f.name):
                remove(f.name)

    def test_cleanup_deletes_temporary_directory_paths(self):
        adapter = AdapterTester(minimal_message, self.config)
        dirname = mkdtemp()
        try:
            adapter.temp_paths += [dirname]
            self.assertTrue(path.exists(dirname))

            adapter.cleanup()

            self.assertFalse(path.exists(dirname))
        finally:
            if path.exists(dirname):
                rmtree(dirname)

    def test_download_granules_fetches_remote_granules_and_stores_their_path(self):
        adapter = AdapterTester(full_message, self.config)
        try:
            adapter.download_granules()
            granules = adapter.message.granules
            self.assertEqual(granules[0].local_filename, 'example/example_granule_1.txt')
            self.assertEqual(granules[1].local_filename, 'example/example_granule_2.txt')
            self.assertEqual(granules[2].local_filename, 'example/example_granule_3.txt')
            self.assertEqual(granules[3].local_filename, 'example/example_granule_4.txt')
        finally:
            adapter.cleanup()

    def test_download_granules_adds_granule_temp_dir_to_temp_paths(self):
        adapter = AdapterTester(full_message, self.config)
        try:
            self.assertEqual(len(adapter.temp_paths), 0)
            adapter.download_granules()
            self.assertEqual(len(adapter.temp_paths), 1)
        finally:
            adapter.cleanup()

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    def test_completed_with_error_when_no_callback_has_been_made_it_posts_the_error(self, _callback_post):
        adapter = AdapterTester(full_message, self.config)
        adapter.completed_with_error('ohai there')
        _callback_post.assert_called_with('/response?error=ohai%20there')

    def test_completed_with_error_when_a_callback_has_been_made_it_throws_an_exception(self):
        adapter = AdapterTester(full_message, self.config)
        adapter.completed_with_error('ohai there')
        self.assertRaises(Exception, adapter.completed_with_error, 'ohai there again')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    def test_completed_with_redirect_when_no_callback_has_been_made_it_posts_the_redirect(self, _callback_post):
        adapter = AdapterTester(full_message, self.config)
        adapter.completed_with_redirect('https://example.com')
        _callback_post.assert_called_with('/response'
                                          '?item[href]=https%3A//example.com'
                                          '&item[type]=image/tiff'
                                          '&status=successful')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    def test_completed_with_redirect_accepts_item_metadata(self, _callback_post):
        adapter = AdapterTester(full_message, self.config)
        granule = adapter.message.sources[0].granules[0]
        temporal = Temporal(start='2011-11-11T11:11:11Z', end='2011-11-11T11:11:12Z')
        adapter.completed_with_redirect('https://example.com', title='hi', mime='image/tiff', source_granule=granule,
                                        temporal=temporal, bbox=[1.1, 2.2, 3.3, 4.4])
        _callback_post.assert_called_with('/response'
                                          '?item[href]=https%3A//example.com'
                                          '&item[type]=image/tiff'
                                          '&item[temporal]=2001-01-01T01%3A01%3A01Z%2C2002-02-02T02%3A02%3A02Z'
                                          '&item[bbox]=-1%2C-2%2C3%2C4'
                                          '&status=successful')

    def test_completed_with_redirect_when_a_callback_has_been_made_it_throws_an_exception(self):
        adapter = AdapterTester(full_message, self.config)
        adapter.completed_with_redirect('https://example.com/1')
        self.assertRaises(Exception, adapter.completed_with_error, 'https://example.com/2')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    @patch.object(harmony.util, 'stage', return_value='https://example.com/out')
    def test_completed_with_local_file_stages_the_local_file_and_redirects_to_it(self, stage, _callback_post):
        adapter = AdapterTester(full_message, self.config)
        adapter.completed_with_local_file('tmp/output.tif', remote_filename='out.tif')
        stage.assert_called_with('tmp/output.tif',
                                 'out.tif',
                                 'image/tiff',
                                 location='s3://example-bucket/public/some-org/some-service/some-uuid/',
                                 logger=adapter.logger,
                                 cfg=self.config)
        _callback_post.assert_called_with('/response'
                                          '?item[href]=https%3A//example.com/out'
                                          '&item[type]=image/tiff'
                                          '&status=successful')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    @patch.object(harmony.util, 'stage', return_value='https://example.com/out')
    def test_completed_with_local_file_uses_granule_file_naming(self, stage, _callback_post):
        adapter = AdapterTester(full_message, self.config)
        granule = adapter.message.sources[0].granules[0]
        adapter.completed_with_local_file('tmp/output.tif', source_granule=granule,
                                          is_variable_subset=True, is_regridded=True, is_subsetted=True)
        stage.assert_called_with('tmp/output.tif',
                                 'example_granule_1_ExampleVar1_regridded_subsetted.tif',
                                 'image/tiff',
                                 location='s3://example-bucket/public/some-org/some-service/some-uuid/',
                                 logger=adapter.logger,
                                 cfg=self.config)
        _callback_post.assert_called_with('/response'
                                          '?item[href]=https%3A//example.com/out'
                                          '&item[type]=image/tiff'
                                          '&item[temporal]=2001-01-01T01%3A01%3A01Z%2C2002-02-02T02%3A02%3A02Z'
                                          '&item[bbox]=-1%2C-2%2C3%2C4'
                                          '&status=successful')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    def test_async_add_url_partial_result_for_async_incomplete_requests_posts_the_url(self, _callback_post):
        adapter = AdapterTester(full_message, self.config)
        adapter.message.isSynchronous = False
        adapter.async_add_url_partial_result('https://example.com')
        _callback_post.assert_called_with('/response?item[href]=https%3A//example.com&item[type]=image/tiff')

    def test_async_add_url_partial_result_for_sync_requests_throws_an_error(self):
        adapter = AdapterTester(full_message, self.config)
        adapter.message.isSynchronous = True
        self.assertRaises(Exception, adapter.async_add_url_partial_result, 'https://example.com/2')

    def test_async_add_url_partial_result_for_complete_requests_throws_an_error(self):
        adapter = AdapterTester(full_message, self.config)
        adapter.message.isSynchronous = False
        adapter.completed_with_redirect('https://example.com/1')
        self.assertRaises(Exception, adapter.async_add_url_partial_result, 'https://example.com/2')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    def test_async_add_url_partial_result_supplies_bbox_and_temporal_when_provided(self, _callback_post):
        adapter = AdapterTester(full_message, self.config)
        adapter.message.isSynchronous = False
        temporal = Temporal(start='2011-11-11T11:11:11Z', end='2011-11-11T11:11:12Z')
        adapter.async_add_url_partial_result('https://example.com', temporal=temporal, bbox=[1.1, 2.2, 3.3, 4.4])
        _callback_post.assert_called_with('/response'
                                          '?item[href]=https%3A//example.com'
                                          '&item[type]=image/tiff'
                                          '&item[temporal]=2011-11-11T11%3A11%3A11Z%2C2011-11-11T11%3A11%3A12Z'
                                          '&item[bbox]=1.1%2C2.2%2C3.3%2C4.4')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    def test_async_add_url_partial_result_uses_granule_bbox_and_temporal_when_provided(self, _callback_post):
        adapter = AdapterTester(full_message, self.config)
        adapter.message.isSynchronous = False
        granule = adapter.message.sources[0].granules[0]
        adapter.async_add_url_partial_result('https://example.com', source_granule=granule)
        _callback_post.assert_called_with('/response'
                                          '?item[href]=https%3A//example.com'
                                          '&item[type]=image/tiff'
                                          '&item[temporal]=2001-01-01T01%3A01%3A01Z%2C2002-02-02T02%3A02%3A02Z'
                                          '&item[bbox]=-1%2C-2%2C3%2C4')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    def test_async_add_url_partial_result_prefers_explicit_bbox_and_temporal_over_granule_values(self, _callback_post):
        adapter = AdapterTester(full_message, self.config)
        adapter.message.isSynchronous = False
        granule = adapter.message.sources[0].granules[0]
        temporal = Temporal(start='2011-11-11T11:11:11Z', end='2011-11-11T11:11:12Z')
        adapter.async_add_url_partial_result('https://example.com', source_granule=granule,
                                             temporal=temporal, bbox=[1.1, 2.2, 3.3, 4.4])
        _callback_post.assert_called_with('/response'
                                          '?item[href]=https%3A//example.com'
                                          '&item[type]=image/tiff'
                                          '&item[temporal]=2011-11-11T11%3A11%3A11Z%2C2011-11-11T11%3A11%3A12Z'
                                          '&item[bbox]=1.1%2C2.2%2C3.3%2C4.4')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    def test_async_completed_successfully_for_async_incomplete_requests_posts_the_completion_status(self, _callback_post):
        adapter = AdapterTester(full_message, self.config)
        adapter.message.isSynchronous = False
        adapter.async_completed_successfully()
        _callback_post.assert_called_with('/response?status=successful')

    def test_async_completed_successfully_for_sync_requests_throws_an_error(self):
        adapter = AdapterTester(full_message, self.config)
        adapter.message.isSynchronous = True
        self.assertRaises(Exception, adapter.async_completed_successfully)

    def test_async_completed_successfully_for_complete_requests_throws_an_error(self):
        adapter = AdapterTester(full_message, self.config)
        adapter.message.isSynchronous = False
        adapter.async_completed_successfully()
        self.assertRaises(Exception, adapter.async_completed_successfully)

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    @patch.object(harmony.util, 'stage', return_value='https://example.com/out')
    def test_async_add_local_file_partial_result_stages_the_local_file_and_updates_progress(self, stage, _callback_post):
        adapter = AdapterTester(full_message, self.config)
        adapter.message.isSynchronous = False
        adapter.async_add_local_file_partial_result('tmp/output.tif', remote_filename='out.tif', title='my file',
                                                    progress=50)
        stage.assert_called_with('tmp/output.tif',
                                 'out.tif',
                                 'image/tiff',
                                 location='s3://example-bucket/public/some-org/some-service/some-uuid/',
                                 logger=adapter.logger,
                                 cfg=self.config)
        _callback_post.assert_called_with('/response'
                                          '?item[href]=https%3A//example.com/out'
                                          '&item[type]=image/tiff'
                                          '&item[title]=my%20file'
                                          '&progress=50')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    @patch.object(harmony.util, 'stage', return_value='https://example.com/out')
    def test_async_add_local_file_partial_result_uses_granule_file_naming(self, stage, _callback_post):
        adapter = AdapterTester(full_message, self.config)
        adapter.message.isSynchronous = False
        granule = adapter.message.sources[0].granules[0]
        adapter.async_add_local_file_partial_result('tmp/output.tif', source_granule=granule,
                                                    is_variable_subset=True, is_regridded=True, is_subsetted=True,
                                                    title='my file', progress=50)
        stage.assert_called_with('tmp/output.tif',
                                 'example_granule_1_ExampleVar1_regridded_subsetted.tif',
                                 'image/tiff',
                                 location='s3://example-bucket/public/some-org/some-service/some-uuid/',
                                 logger=adapter.logger,
                                 cfg=self.config)
        _callback_post.assert_called_with('/response'
                                          '?item[href]=https%3A//example.com/out'
                                          '&item[type]=image/tiff'
                                          '&item[title]=my%20file'
                                          '&item[temporal]=2001-01-01T01%3A01%3A01Z%2C2002-02-02T02%3A02%3A02Z'
                                          '&item[bbox]=-1%2C-2%2C3%2C4'
                                          '&progress=50')

    def test_filename_for_granule(self):
        adapter = AdapterTester(minimal_message, self.config)
        granule = Granule({'url': 'https://example.com/fake-path/abc.123.nc/?query=true'})
        ext = 'zarr'

        # Basic cases
        self.assertEqual(adapter.filename_for_granule(granule, ext), 'abc.123.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_subsetted=True), 'abc.123_subsetted.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_regridded=True), 'abc.123_regridded.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_subsetted=True, is_regridded=True),
                         'abc.123_regridded_subsetted.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True, is_subsetted=True,
                                                      is_regridded=True),
                         'abc.123_regridded_subsetted.zarr')

        # Variable name contains full path with '/' ('/' replaced with '_')
        granule.variables.append(Variable({'name': '/path/to/VarB'}))
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True, is_subsetted=True,
                                                      is_regridded=True),
                         'abc.123__path_to_VarB_regridded_subsetted.zarr')
        granule.variables.pop()

        # Single variable cases
        granule.variables.append(Variable({'name': 'VarA'}))
        self.assertEqual(adapter.filename_for_granule(granule, ext), 'abc.123.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_subsetted=True, is_regridded=True),
                         'abc.123_regridded_subsetted.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True), 'abc.123_VarA.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True, is_subsetted=True,
                                                      is_regridded=True),
                         'abc.123_VarA_regridded_subsetted.zarr')

        # Multiple variable cases (no variable name in suffix)
        granule.variables.append(Variable({'name': 'VarB'}))
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_subsetted=True, is_regridded=True),
                         'abc.123_regridded_subsetted.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True, is_subsetted=True,
                                                      is_regridded=True),
                         'abc.123_regridded_subsetted.zarr')
        granule.variables.pop()

        # URL already containing a suffix
        granule.url = 'https://example.com/fake-path/abc.123_regridded.zarr'
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_subsetted=True),
                         'abc.123_regridded_subsetted.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True, is_subsetted=True,
                                                      is_regridded=True),
                         'abc.123_VarA_regridded_subsetted.zarr')

        # URL already containing all suffixes
        granule.url = 'https://example.com/fake-path/abc.123_VarA_regridded_subsetted.zarr'
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True, is_subsetted=True,
                                                      is_regridded=True),
                         'abc.123_VarA_regridded_subsetted.zarr')

    @patch('urllib.request.urlopen')
    @patch.dict(os.environ, {'ENV': 'not_test_we_swear'})
    def test_cancel_request(self, urlopen):
        adapter = AdapterTester(minimal_message, self.config)
        urlopen.side_effect = MockHTTPError(url='http://example.com', code=409, msg='Harmony canceled request', hdrs=[], fp=None)
        self.assertRaises(CanceledException, adapter.async_add_url_partial_result, 'https://example.com/2')
        self.assertTrue(adapter.is_canceled)
        self.assertTrue(adapter.is_complete)
        self.assertEqual(adapter.is_failed, 1)


class TestCallbackPostHealthUpdate(unittest.TestCase):
    def setUp(self):
        self.config = harmony.util.config(validate=False)

    @patch.object(pathlib.Path, '__new__')
    def test_callback_post_updates_health_check_file(self, mock_path):
        adapter = AdapterTester(full_message, self.config)
        adapter.completed_with_error('ohai there')
        mock_path.return_value.touch.assert_called()
