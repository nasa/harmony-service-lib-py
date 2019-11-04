import unittest
from unittest.mock import patch
from tempfile import NamedTemporaryFile, mkdtemp
from os import path, remove
from shutil import rmtree

from harmony.adapter import BaseHarmonyAdapter
from harmony.message import Message
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

    @patch.object(BaseHarmonyAdapter, '_completed_with_post')
    def test_completed_with_error_when_no_callback_has_been_made_it_posts_the_error(self, _completed_with_post):
        adapter = TestAdapter(full_message)
        adapter.completed_with_error('ohai there')
        _completed_with_post.assert_called_with('/response?error=ohai%20there')

    def test_completed_with_error_when_a_callback_has_been_made_it_throws_an_exception(self):
        adapter = TestAdapter(full_message)
        adapter.completed_with_error('ohai there')
        self.assertRaises(Exception, adapter.completed_with_error, 'ohai there again')

    @patch.object(BaseHarmonyAdapter, '_completed_with_post')
    def test_completed_with_redirect_when_no_callback_has_been_made_it_posts_the_redirect(self, _completed_with_post):
        adapter = TestAdapter(full_message)
        adapter.completed_with_redirect('https://example.com')
        _completed_with_post.assert_called_with('/response?redirect=https%3A//example.com')

    def test_completed_with_redirect_when_a_callback_has_been_made_it_throws_an_exception(self):
        adapter = TestAdapter(full_message)
        adapter.completed_with_redirect('https://example.com/1')
        self.assertRaises(Exception, adapter.completed_with_error, 'https://example.com/2')

    @patch.object(BaseHarmonyAdapter, '_completed_with_post')
    @patch.object(harmony.util, 'stage', return_value='https://example.com/out')
    def test_completed_with_local_file_stages_the_local_file_and_redirects_to_it(self, stage, _completed_with_post):
        adapter = TestAdapter(full_message)
        adapter.completed_with_local_file('tmp/output.tif', 'out.tif')
        stage.assert_called_with('tmp/output.tif', 'out.tif', 'image/tiff', adapter.logger)
        _completed_with_post.assert_called_with('/response?redirect=https%3A//example.com/out')

