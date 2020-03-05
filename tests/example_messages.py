
minimal_message =  """
    {
        "$schema": "../../harmony/app/schemas/data-operation/0.5.0/data-operation-v0.5.0.json",
        "version": "0.5.0",
        "callback": "http://localhost/some-path",
        "user": "jdoe",
        "client": "curl",
        "requestId": "00001111-2222-3333-4444-555566667777",
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
        "$schema": "../../harmony/app/schemas/data-operation/0.5.0/data-operation-v0.5.0.json",
        "version": "0.5.0",
        "callback": "http://localhost/some-path",
        "user": "jdoe",
        "client": "curl",
        "requestId": "00001111-2222-3333-4444-555566667777",
        "sources": [
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
        "$schema": "../../harmony/app/schemas/data-operation/0.5.0/data-operation-v0.5.0.json",
        "version": "0.5.0",
        "callback": "http://localhost/some-path",
        "user": "jdoe",
        "client": "curl",
        "requestId": "00001111-2222-3333-4444-555566667777",
        "isSynchronous": true,
        "sources": [
            {
            "collection": "C0001-EXAMPLE",
            "variables": [
                {
                "id": "V0001-EXAMPLE",
                "name": "ExampleVar1"
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
            ]}, {
            "collection": "C0002-EXAMPLE",
            "variables": [
                {
                "id": "V0002-EXAMPLE",
                "name": "ExampleVar2"
                }
            ],
            "granules": [
                {
                "id": "G0003-EXAMPLE",
                "name": "Example3",
                "url": "file://example/example_granule_3.txt"
                },
                {
                "id": "G0004-EXAMPLE",
                "name": "Example4",
                "url": "file://example/example_granule_4.txt"
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
            "dpi": 72,
            "interpolation": "near",
            "scaleExtent": { "x": { "min": 0.5, "max": 125 }, "y": { "min": 52, "max": 75.22 } },
            "scaleSize": { "x": 14.2, "y": 35 }
        },
        "temporal": {
            "start": "1999-01-01T10:00:00Z",
            "end": "2020-02-20T15:00:00Z"
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