import os
from tempfile import mkdtemp
import shutil
import unittest

from pystac import Catalog, CatalogType

from harmony import cli, BaseHarmonyAdapter
from harmony.exceptions import ForbiddenException
from tests.util import cli_parser, config_fixture


class MockAdapter(BaseHarmonyAdapter):
    message = None
    """
    Dummy class to mock adapter calls, performing a no-op service
    """
    def invoke(self):
        MockAdapter.message = self.message
        return (self.message, self.catalog)


class TestCliInvokeAction(unittest.TestCase):
    def setUp(self):
        self.workdir = mkdtemp()
        self.inputdir = mkdtemp()
        self.catalog = Catalog('test-id', 'test catalog')
        self.catalog.normalize_and_save(self.inputdir, CatalogType.SELF_CONTAINED)
        self.config = config_fixture()
        print(self.config)

    def tearDown(self):
        MockAdapter.messages = []
        shutil.rmtree(self.workdir)

    def test_when_a_service_completes_it_writes_a_output_catalog_to_the_output_dir(self):
        with cli_parser(
                '--harmony-action', 'invoke',
                '--harmony-input', '{"test": "input"}',
                '--harmony-sources', 'example/source/catalog.json',
                '--harmony-metadata-dir', self.workdir) as parser:
            args = parser.parse_args()
            cli.run_cli(parser, args, MockAdapter, cfg=self.config)
            output = Catalog.from_file(os.path.join(self.workdir, 'catalog.json'))
            self.assertTrue(output.validate)

    def test_when_a_service_completes_it_writes_the_output_message_to_the_output_dir(self):
        with cli_parser(
                '--harmony-action', 'invoke',
                '--harmony-input', '{"test": "input"}',
                '--harmony-sources', 'example/source/catalog.json',
                '--harmony-metadata-dir', self.workdir) as parser:
            args = parser.parse_args()
            cli.run_cli(parser, args, MockAdapter, cfg=self.config)
            with open(os.path.join(self.workdir, 'message.json')) as file:
                self.assertEqual(file.read(), '{"test": "input"}')

    def test_when_the_cli_has_a_staging_location_it_overwites_the_message_staging_location(self):
        with cli_parser(
                '--harmony-action', 'invoke',
                '--harmony-input', '{"test": "input"}',
                '--harmony-sources', 'example/source/catalog.json',
                '--harmony-metadata-dir', self.workdir,
                '--harmony-data-location', 's3://fake-location/') as parser:
            args = parser.parse_args()
            cli.run_cli(parser, args, MockAdapter, cfg=self.config)
            self.assertEqual(MockAdapter.message.stagingLocation, 's3://fake-location/')
            # Does not output the altered staging location
            with open(os.path.join(self.workdir, 'message.json')) as file:
                self.assertEqual(file.read(), '{"test": "input"}')

    def test_when_the_backend_service_throws_a_known_error_it_writes_the_error_to_the_output_dir(self):
        with cli_parser(
                '--harmony-action', 'invoke',
                '--harmony-input', '{"test": "input"}',
                '--harmony-sources', 'example/source/catalog.json',
                '--harmony-metadata-dir', self.workdir) as parser:

            class MockImpl(MockAdapter):
                def invoke(self):
                    self.is_complete = False
                    raise ForbiddenException('Something bad happened')

            args = parser.parse_args()
            with self.assertRaises(Exception) as context:
                cli.run_cli(parser, args, MockImpl, cfg=self.config)

            self.assertTrue('Something bad happened' in str(context.exception))
            with open(os.path.join(self.workdir, 'error.json')) as file:
                self.assertEqual(file.read(), '{"error": "Something bad happened", "category": "Forbidden"}')

    def test_when_the_backend_service_throws_an_unknown_error_it_writes_a_generic_error_to_the_output_dir(self):
        with cli_parser(
                '--harmony-action', 'invoke',
                '--harmony-input', '{"test": "input"}',
                '--harmony-sources', 'example/source/catalog.json',
                '--harmony-metadata-dir', self.workdir) as parser:

            class MockImpl(MockAdapter):
                def invoke(self):
                    self.is_complete = False
                    raise Exception('Something bad happened')

            args = parser.parse_args()
            with self.assertRaises(Exception) as context:
                cli.run_cli(parser, args, MockImpl, cfg=self.config)

            self.assertTrue('Something bad happened' in str(context.exception))
            with open(os.path.join(self.workdir, 'error.json')) as file:
                self.assertEqual(file.read(), '{"error": "Service request failed with an unknown error", "category": "Unknown"}')


if __name__ == '__main__':
    unittest.main()
