import hashlib
import json
import os
import urllib.parse

from functools import lru_cache
from harmony.util.data_transfer import download

class JsonObject(object):
    reprdepth = 0

    def __init__(self, data, properties=[], list_properties={}):
        self.data = data or {}
        self.properties = properties + list(list_properties.keys())
        for prop in properties:
            setattr(self, prop, data.get(prop))
        for prop in list_properties:
            Class = list_properties[prop]
            items = data.get(prop) or []
            value = [Class(item) for item in items]
            setattr(self, prop, value)

    def __repr__(self):
        result = ''
        JsonObject.reprdepth += 1
        try:
            spaces = '    ' * JsonObject.reprdepth
            result += '<' + self.__class__.__name__ + '\n'
            result += '\n'.join(["%s%s = %s" % (spaces, p, repr(getattr(self, p))) for p in self.properties])
            result += '>'
        finally:
            JsonObject.reprdepth -= 1
        return result

class Source(JsonObject):
    def __init__(self, message_data):
        super().__init__(message_data,
            properties=['collection'],
            list_properties={'variables': Variable, 'granules': Granule}
        )
        for granule in self.granules:
            granule.collection = self.collection
            granule.variables = self.variables

class Variable(JsonObject):
    def __init__(self, message_data):
        super().__init__(message_data, properties=['id', 'name'])

class Granule(JsonObject):
    def __init__(self, message_data):
        super().__init__(message_data, properties=['id', 'name', 'url'])
        self.local_filename = None
        self.collection = None
        self.variables = []

class Format(JsonObject):
    def __init__(self, message_data):
        super().__init__(message_data, properties=[
            'crs',
            'isTransparent',
            'mime',
            'width',
            'height',
            'dpi'
        ])

class Subset(JsonObject):
    def __init__(self, message_data):
        super().__init__(message_data, properties=['bbox'])

class Message(JsonObject):
    def __init__(self, json_str):
        self.json = json_str
        super().__init__(json.loads(json_str),
            properties=['version', 'callback', 'user', 'format', 'subset'],
            list_properties={'sources': Source}
        )
        self.format = Format(self.format)
        self.subset = Subset(self.subset)

    def digest(self):
        return hashlib.sha256(self.json.encode('utf-8')).hexdigest()

    @property
    @lru_cache(maxsize=None)
    def granules(self):
        result = []
        for source in self.sources:
            result += source.granules
        return result
