import unittest
from unittest.mock import patch

from harmony.message import Message
from .example_messages import minimal_message, minimal_source_message, full_message

class TestMessage(unittest.TestCase):
    def test_when_provided_a_full_message_it_parses_it_into_objects(self):
        message = Message(full_message)

        self.assertEqual(message.version, '0.7.0')
        self.assertEqual(message.callback, 'http://localhost/some-path')
        self.assertEqual(message.stagingLocation, 's3://example-bucket/public/some-org/some-service/some-uuid/')
        self.assertEqual(message.user, 'jdoe')
        self.assertEqual(message.client, 'curl')
        self.assertEqual(message.requestId, '00001111-2222-3333-4444-555566667777')
        self.assertEqual(message.isSynchronous, True)
        self.assertEqual(message.sources[0].collection, 'C0001-EXAMPLE')
        self.assertEqual(message.sources[0].variables[0].id, 'V0001-EXAMPLE')
        self.assertEqual(message.sources[0].variables[0].name, 'ExampleVar1')
        self.assertEqual(message.sources[0].granules[1].id, 'G0002-EXAMPLE')
        self.assertEqual(message.sources[0].granules[1].name, 'Example2')
        self.assertEqual(message.sources[0].granules[1].url, 'file://example/example_granule_2.txt')
        self.assertEqual(message.sources[0].granules[1].temporal.start, '2003-03-03T03:03:03Z')
        self.assertEqual(message.sources[0].granules[1].temporal.end, '2004-04-04T04:04:04Z')
        self.assertEqual(message.sources[0].granules[1].bbox, [-5, -6, 7, 8])
        self.assertEqual(message.sources[1].collection, 'C0002-EXAMPLE')
        self.assertEqual(message.format.crs, 'CRS:84')
        self.assertEqual(message.format.isTransparent, True)
        self.assertEqual(message.format.mime, 'image/tiff')
        self.assertEqual(message.format.width, 800)
        self.assertEqual(message.format.height, 600)
        self.assertEqual(message.format.dpi, 72)
        self.assertEqual(message.format.interpolation, 'near')
        self.assertEqual(message.format.scaleExtent.x.min, 0.5)
        self.assertEqual(message.format.scaleExtent.x.max, 125)
        self.assertEqual(message.format.scaleExtent.y.min, 52)
        self.assertEqual(message.format.scaleExtent.y.max, 75.22)
        self.assertEqual(message.temporal.start, '1999-01-01T10:00:00Z')
        self.assertEqual(message.temporal.end, '2020-02-20T15:00:00Z')
        self.assertEqual(message.subset.bbox, [-91.1, -45.0, 91.1, 45.0])
        self.assertEqual(message.subset.shape.href, 's3://example-bucket/shapefiles/abcd.json')
        self.assertEqual(message.subset.shape.type, 'application/geo+json')


    def test_when_provided_a_minimal_message_it_parses_it_into_objects(self):
        message = Message(minimal_message)

        self.assertEqual(message.version, '0.7.0')
        self.assertEqual(message.callback, 'http://localhost/some-path')
        self.assertEqual(message.stagingLocation, 's3://example-bucket/public/some-org/some-service/some-uuid/')
        self.assertEqual(message.user, 'jdoe')
        self.assertEqual(message.client, 'curl')
        self.assertEqual(message.requestId, '00001111-2222-3333-4444-555566667777')
        self.assertEqual(message.sources, [])
        self.assertEqual(message.format.crs, None)
        self.assertEqual(message.format.isTransparent, None)
        self.assertEqual(message.format.mime, None)
        self.assertEqual(message.format.width, None)
        self.assertEqual(message.format.height, None)
        self.assertEqual(message.format.dpi, None)
        self.assertEqual(message.subset.bbox, None)

    def test_when_provided_a_message_with_minimal_source_it_parses_it_into_objects(self):
        message = Message(minimal_source_message)

        self.assertEqual(message.version, '0.7.0')
        self.assertEqual(message.callback, 'http://localhost/some-path')
        self.assertEqual(message.user, 'jdoe')
        self.assertEqual(message.sources[0].collection, 'C0001-EXAMPLE')
        self.assertEqual(message.sources[0].variables, [])
        self.assertEqual(message.sources[0].granules, [])

    def test_granules_attribute_returns_all_child_granules(self):
        message = Message(full_message)

        self.assertEqual(len(message.granules), 4)
        self.assertEqual(message.granules[0].id, 'G0001-EXAMPLE')
        self.assertEqual(message.granules[1].id, 'G0002-EXAMPLE')
        self.assertEqual(message.granules[2].id, 'G0003-EXAMPLE')
        self.assertEqual(message.granules[3].id, 'G0004-EXAMPLE')

    def test_granules_link_to_their_parent_collection(self):
        message = Message(full_message)

        self.assertEqual(message.granules[0].collection, 'C0001-EXAMPLE')
        self.assertEqual(message.granules[1].collection, 'C0001-EXAMPLE')
        self.assertEqual(message.granules[2].collection, 'C0002-EXAMPLE')
        self.assertEqual(message.granules[3].collection, 'C0002-EXAMPLE')

    def test_granules_link_to_their_subset_variables(self):
        message = Message(full_message)

        self.assertEqual(message.granules[0].variables[0].id, 'V0001-EXAMPLE')
        self.assertEqual(message.granules[1].variables[0].id, 'V0001-EXAMPLE')
        self.assertEqual(message.granules[2].variables[0].id, 'V0002-EXAMPLE')
        self.assertEqual(message.granules[3].variables[0].id, 'V0002-EXAMPLE')

    def test_digest_returns_a_unique_string_per_message(self):
        message1 = Message(full_message)
        message2 = Message(minimal_source_message)
        message3 = Message(minimal_message)

        self.assertNotEqual(message1, message2)
        self.assertNotEqual(message2, message3)
        self.assertNotEqual(message3, message1)
