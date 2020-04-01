import unittest
from unittest.mock import patch
from tempfile import NamedTemporaryFile, mkdtemp
from os import path, remove
from shutil import rmtree

from harmony.adapter import BaseHarmonyAdapter
from harmony.message import Message, Granule, Variable
from .example_messages import minimal_message, minimal_source_message, full_message
import harmony.util

# BaseHarmonyAdapter is abstract, so tests need a minimal concrete class
class TestAdapter(BaseHarmonyAdapter):
    def __init__(self, message_str):
        super().__init__(Message(message_str))

    def invoke(self):
        pass

class TestBaseHarmonyAdapter(unittest.TestCase):
    def test_cleanup_deletes_temporary_file_paths(self):
        adapter = TestAdapter(minimal_message)
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
        adapter = TestAdapter(minimal_message)
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
        adapter = TestAdapter(full_message)
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
        adapter = TestAdapter(full_message)
        try:
            self.assertEqual(len(adapter.temp_paths), 0)
            adapter.download_granules()
            self.assertEqual(len(adapter.temp_paths), 1)
        finally:
            adapter.cleanup()

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    def test_completed_with_error_when_no_callback_has_been_made_it_posts_the_error(self, _callback_post):
        adapter = TestAdapter(full_message)
        adapter.completed_with_error('ohai there')
        _callback_post.assert_called_with('/response?error=ohai%20there')

    def test_completed_with_error_when_a_callback_has_been_made_it_throws_an_exception(self):
        adapter = TestAdapter(full_message)
        adapter.completed_with_error('ohai there')
        self.assertRaises(Exception, adapter.completed_with_error, 'ohai there again')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    def test_completed_with_redirect_when_no_callback_has_been_made_it_posts_the_redirect(self, _callback_post):
        adapter = TestAdapter(full_message)
        adapter.completed_with_redirect('https://example.com')
        _callback_post.assert_called_with('/response?redirect=https%3A//example.com')

    def test_completed_with_redirect_when_a_callback_has_been_made_it_throws_an_exception(self):
        adapter = TestAdapter(full_message)
        adapter.completed_with_redirect('https://example.com/1')
        self.assertRaises(Exception, adapter.completed_with_error, 'https://example.com/2')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    @patch.object(harmony.util, 'stage', return_value='https://example.com/out')
    def test_completed_with_local_file_stages_the_local_file_and_redirects_to_it(self, stage, _callback_post):
        adapter = TestAdapter(full_message)
        adapter.completed_with_local_file('tmp/output.tif', remote_filename='out.tif')
        stage.assert_called_with('tmp/output.tif', 'out.tif', 'image/tiff', location='s3://example-bucket/public/some-org/some-service/some-uuid/', logger=adapter.logger)
        _callback_post.assert_called_with('/response?redirect=https%3A//example.com/out')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    @patch.object(harmony.util, 'stage', return_value='https://example.com/out')
    def test_completed_with_local_file_uses_granule_file_naming(self, stage, _callback_post):
        adapter = TestAdapter(full_message)
        granule = adapter.message.sources[0].granules[0]
        adapter.completed_with_local_file('tmp/output.tif', source_granule=granule, is_variable_subset=True, is_regridded=True, is_subsetted=True)
        stage.assert_called_with('tmp/output.tif', 'example_granule_1_ExampleVar1_regridded_subsetted.tif', 'image/tiff', location='s3://example-bucket/public/some-org/some-service/some-uuid/', logger=adapter.logger)
        _callback_post.assert_called_with('/response?redirect=https%3A//example.com/out')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    def test_async_add_url_partial_result_for_async_incomplete_requests_posts_the_url(self, _callback_post):
        adapter = TestAdapter(full_message)
        adapter.message.isSynchronous = False
        adapter.async_add_url_partial_result('https://example.com')
        _callback_post.assert_called_with('/response?item[href]=https%3A//example.com&item[type]=image/tiff')

    def test_async_add_url_partial_result_for_sync_requests_throws_an_error(self):
        adapter = TestAdapter(full_message)
        adapter.message.isSynchronous = True
        self.assertRaises(Exception, adapter.async_add_url_partial_result, 'https://example.com/2')

    def test_async_add_url_partial_result_for_complete_requests_throws_an_error(self):
        adapter = TestAdapter(full_message)
        adapter.message.isSynchronous = False
        adapter.completed_with_redirect('https://example.com/1')
        self.assertRaises(Exception, adapter.async_add_url_partial_result, 'https://example.com/2')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    def test_async_completed_successfully_for_async_incomplete_requests_posts_the_completion_status(self, _callback_post):
        adapter = TestAdapter(full_message)
        adapter.message.isSynchronous = False
        adapter.async_completed_successfully()
        _callback_post.assert_called_with('/response?status=successful')

    def test_async_completed_successfully_for_sync_requests_throws_an_error(self):
        adapter = TestAdapter(full_message)
        adapter.message.isSynchronous = True
        self.assertRaises(Exception, adapter.async_completed_successfully)

    def test_async_completed_successfully_for_complete_requests_throws_an_error(self):
        adapter = TestAdapter(full_message)
        adapter.message.isSynchronous = False
        adapter.async_completed_successfully()
        self.assertRaises(Exception, adapter.async_completed_successfully)

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    @patch.object(harmony.util, 'stage', return_value='https://example.com/out')
    def test_async_add_local_file_partial_result_stages_the_local_file_and_updates_progress(self, stage, _callback_post):
        adapter = TestAdapter(full_message)
        adapter.message.isSynchronous = False
        adapter.async_add_local_file_partial_result('tmp/output.tif', remote_filename='out.tif', title='my file', progress=50)
        stage.assert_called_with('tmp/output.tif', 'out.tif', 'image/tiff', location='s3://example-bucket/public/some-org/some-service/some-uuid/', logger=adapter.logger)
        _callback_post.assert_called_with('/response?item[href]=https%3A//example.com/out&item[type]=image/tiff&item[title]=my%20file&progress=50')

    @patch.object(BaseHarmonyAdapter, '_callback_post')
    @patch.object(harmony.util, 'stage', return_value='https://example.com/out')
    def test_async_add_local_file_partial_result_uses_granule_file_naming(self, stage, _callback_post):
        adapter = TestAdapter(full_message)
        adapter.message.isSynchronous = False
        granule = adapter.message.sources[0].granules[0]
        adapter.async_add_local_file_partial_result('tmp/output.tif', source_granule=granule, is_variable_subset=True, is_regridded=True, is_subsetted=True, title='my file', progress=50)
        stage.assert_called_with('tmp/output.tif', 'example_granule_1_ExampleVar1_regridded_subsetted.tif', 'image/tiff', location='s3://example-bucket/public/some-org/some-service/some-uuid/', logger=adapter.logger)
        _callback_post.assert_called_with('/response?item[href]=https%3A//example.com/out&item[type]=image/tiff&item[title]=my%20file&progress=50')

    def test_filename_for_granule(self):
        adapter = TestAdapter(minimal_message)
        granule = Granule({'url': 'https://example.com/fake-path/abc.123.nc/?query=true'})
        ext = 'zarr'

        # Basic cases
        self.assertEqual(adapter.filename_for_granule(granule, ext), 'abc.123.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_subsetted=True), 'abc.123_subsetted.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_regridded=True), 'abc.123_regridded.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_subsetted=True, is_regridded=True), 'abc.123_regridded_subsetted.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True, is_subsetted=True, is_regridded=True), 'abc.123_regridded_subsetted.zarr')

        # Single variable cases
        granule.variables.append(Variable({'name': 'VarA'}))
        self.assertEqual(adapter.filename_for_granule(granule, ext), 'abc.123.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_subsetted=True, is_regridded=True), 'abc.123_regridded_subsetted.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True), 'abc.123_VarA.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True, is_subsetted=True, is_regridded=True), 'abc.123_VarA_regridded_subsetted.zarr')

        # Multiple variable cases (no variable name in suffix)
        granule.variables.append(Variable({'name': 'VarB'}))
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_subsetted=True, is_regridded=True), 'abc.123_regridded_subsetted.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True, is_subsetted=True, is_regridded=True), 'abc.123_regridded_subsetted.zarr')
        granule.variables.pop()

        # URL already containing a suffix
        granule.url = 'https://example.com/fake-path/abc.123_regridded.zarr'
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_subsetted=True), 'abc.123_regridded_subsetted.zarr')
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True, is_subsetted=True, is_regridded=True), 'abc.123_VarA_regridded_subsetted.zarr')

        # URL already containing all suffixes
        granule.url = 'https://example.com/fake-path/abc.123_VarA_regridded_subsetted.zarr'
        self.assertEqual(adapter.filename_for_granule(granule, ext, is_variable_subset=True, is_subsetted=True, is_regridded=True), 'abc.123_VarA_regridded_subsetted.zarr')

