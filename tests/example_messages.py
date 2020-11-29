minimal_message = """
    {
        "$schema": "../../harmony/app/schemas/data-operation/0.7.0/data-operation-v0.7.0.json",
        "version": "0.7.0",
        "callback": "http://localhost/some-path",
        "stagingLocation": "s3://example-bucket/public/some-org/some-service/some-uuid/",
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

minimal_source_message = """
    {
        "$schema": "../../harmony/app/schemas/data-operation/0.7.0/data-operation-v0.7.0.json",
        "version": "0.7.0",
        "callback": "http://localhost/some-path",
        "stagingLocation": "s3://example-bucket/public/some-org/some-service/some-uuid/",
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

full_message = """
    {
        "$schema": "../../harmony/app/schemas/data-operation/0.7.0/data-operation-v0.7.0.json",
        "version": "0.7.0",
        "callback": "http://localhost/some-path",
        "stagingLocation": "s3://example-bucket/public/some-org/some-service/some-uuid/",
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
                "name": "ExampleVar1",
                "fullPath": "example/path/ExampleVar1"
                }
            ],
            "granules": [
                {
                "id": "G0001-EXAMPLE",
                "name": "Example1",
                "url": "file://example/example_granule_1.txt",
                "temporal": {
                    "start": "2001-01-01T01:01:01Z",
                    "end": "2002-02-02T02:02:02Z"
                },
                "bbox": [-1, -2, 3, 4]
                },
                {
                "id": "G0002-EXAMPLE",
                "name": "Example2",
                "url": "file://example/example_granule_2.txt",
                "temporal": {
                    "start": "2003-03-03T03:03:03Z",
                    "end": "2004-04-04T04:04:04Z"
                },
                "bbox": [-5, -6, 7, 8]
                }
            ]}, {
            "collection": "C0002-EXAMPLE",
            "variables": [
                {
                "id": "V0002-EXAMPLE",
                "name": "ExampleVar2",
                "fullPath": "example/path/ExampleVar2"
                }
            ],
            "granules": [
                {
                "id": "G0003-EXAMPLE",
                "name": "Example3",
                "url": "file://example/example_granule_3.txt",
                "temporal": {
                    "start": "2005-05-05T05:05:05Z",
                    "end": "2006-06-06T06:06:06Z"
                },
                "bbox": [-9, -10, 11, 12]
                },
                {
                "id": "G0004-EXAMPLE",
                "name": "Example4",
                "url": "file://example/example_granule_4.txt",
                "temporal": {
                    "start": "2007-07-07T07:07:07Z",
                    "end": "2008-08-08T08:08:08Z"
                },
                "bbox": [-13, -14, 15, 16]
                }
            ]
            }
        ],
        "format": {
            "crs": "CRS:84",
            "srs": {
                "proj4": "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
                "wkt": "PROJCS[ ... ]",
                "epsg": "EPSG:7030"
            },
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
            ],
            "shape": {
                "href": "s3://example-bucket/shapefiles/abcd.json",
                "type": "application/geo+json"
            }
        }
    }
"""
