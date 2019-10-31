import unittest
from unittest.mock import patch

from harmony.message import Message



minimal_message =  """
    {
        "$schema": "../../harmony/app/schemas/data-operation/0.1.0/data-operation-v0.1.0.json",
        "version": "0.1.0",
        "callback": "http://localhost/some-path",
        "user": "jdoe",
        "sources": [
        ],
        "format": {
        },
        "subset": {
        }
    }
"""

minimal_source_message =  """
    {
        "$schema": "../../harmony/app/schemas/data-operation/0.1.0/data-operation-v0.1.0.json",
        "version": "0.1.0",
        "callback": "http://localhost/some-path",
        "user": "jdoe",
        "sources": [
         [
            {
            "collection": "C0001-EXAMPLE",
            "variables": [],
            "granules": []
            }
        ],
        "format": {
        },
        "subset": {
        }
    }
"""

full_message =  """
    {
        "$schema": "../../harmony/app/schemas/data-operation/0.1.0/data-operation-v0.1.0.json",
        "version": "0.1.0",
        "callback": "http://localhost/some-path",
        "user": "jdoe",
        "sources": [
            {
            "collection": "C0001-EXAMPLE",
            "variables": [
                {
                "id": "V0001-EXAMPLE",
                "name": "ExampleVar"
                }
            ],
            "granules": [
                {
                "id": "G0001-EXAMPLE",
                "name": "Example1",
                "url": "file://example/example_granule_1.txt"
                },
                {
                "id": "G0002-EXAMPLE",
                "name": "Example2",
                "url": "file://example/example_granule_2.txt"
                }
            ]
            }
        ],
        "format": {
            "crs": "CRS:84",
            "isTransparent": true,
            "mime": "image/tiff",
            "width": 800,
            "height": 600,
            "dpi": 72
        },
        "subset": {
            "bbox": [
            -91.1,
            -45.0,
            91.1,
            45.0
            ]
        }
    }
"""

class MessageClass(unittest.TestCase):
    def test_when_provided_a_full_message_it_parses_it_into_objects(self):
        message = Message(full_message)

        self.assertEqual(message.version, '0.1.0')
        self.assertEqual(message.callback, 'http://localhost/some-path')
        self.assertEqual(message.user, 'jdoe')
        self.assertEqual(message.sources[0].collection, 'C0001-EXAMPLE')
        self.assertEqual(message.sources[0].variables[0].id, 'V0001-EXAMPLE')
        self.assertEqual(message.sources[0].variables[0].name, 'ExampleVar')
        self.assertEqual(message.sources[0].granules[1].id, 'G0002-EXAMPLE')
        self.assertEqual(message.sources[0].granules[1].name, 'Example2')
        self.assertEqual(message.sources[0].granules[1].url, 'file://example/example_granule_2.txt')
        self.assertEqual(message.format.crs, 'CRS:84')
        self.assertEqual(message.format.isTransparent, True)
        self.assertEqual(message.format.mime, 'image/tiff')
        self.assertEqual(message.format.width, 800)
        self.assertEqual(message.format.height, 600)
        self.assertEqual(message.format.dpi, 72)
        self.assertEqual(message.subset.bbox, [-91.1, -45.0, 91.1, 45.0])

    def test_when_provided_a_minimal_message_it_parses_it_into_objects(self):
        message = Message(minimal_message)
        pass

    def test_when_provided_a_message_with_minimal_source_it_parses_it_into_objects(self):
        message = Message(minimal_source_message)

        self.assertEqual(message.version, '0.1.0')
        self.assertEqual(message.callback, 'http://localhost/some-path')
        self.assertEqual(message.user, 'jdoe')
        self.assertEqual(message.sources[0].collection, 'C0001-EXAMPLE')
        self.assertEqual(message.sources[0].variables[0].id, 'V0001-EXAMPLE')
        self.assertEqual(message.sources[0].variables[0].name, 'ExampleVar')
        self.assertEqual(message.sources[0].granules[1].id, 'G0002-EXAMPLE')
        self.assertEqual(message.sources[0].granules[1].name, 'Example2')
        self.assertEqual(message.sources[0].granules[1].url, 'file://example/example_granule_2.txt')
        self.assertEqual(message.format.crs, 'CRS:84')
        self.assertEqual(message.format.isTransparent, True)
        self.assertEqual(message.format.mime, 'image/tiff')
        self.assertEqual(message.format.width, 800)
        self.assertEqual(message.format.height, 600)
        self.assertEqual(message.format.dpi, 72)
        self.assertEqual(message.subset.bbox, [-91.1, -45.0, 91.1, 45.0])