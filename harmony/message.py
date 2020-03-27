"""
==========
message.py
==========

Harmony message parsing and helper objects.  Callers should generally
only construct the 'Message' object and allow its children to be built
from the message JSON.
"""

import hashlib
import json

class JsonObject(object):
    """
    Base class for deserialized Harmony message objects

    Attributes
    ----------
    data : dictionary
        The JSON data object / dictionary used to build this object

    properties: list
        A list of properties that are included in string representations
    """
    reprdepth = 0

    def __init__(self, data, properties=[], list_properties={}):
        """
        Constructor

        Parameters
        ----------
        data : dictionary
            The JSON dictionary created by json.loads at the root of this object
        properties : list, optional
            A list of properties that should be extracted to attributes, by default []
        list_properties : dict, optional
            A dictionary of property name to type for properties that are lists of
            JSONObject classes, by default {}
        """
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
        """
        Returns
        -------
        string
            A string representation of the object
        """
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
    """
    A collection / granule / variable data source as found in the Harmony message
    "sources" list.

    Attributes
    ----------
    collection : string
        The id of the collection the data source's variables and granules are in
    variables : list
        A list of Variable objects for the variables which should be transformed
    granules : list
        A list of Granule objects for the granules which should be operated on
    """
    def __init__(self, message_data):
        """
        Constructor

        Parameters
        ----------
        message_data : dictionary
            The Harmony message "sources" item to deserialize
        """
        super().__init__(message_data,
            properties=['collection'],
            list_properties={'variables': Variable, 'granules': Granule}
        )
        for granule in self.granules:
            granule.collection = self.collection
            granule.variables = self.variables

class Variable(JsonObject):
    """
    A data variable as found in a Harmony source's "variables" list

    Attributes
    ----------
    id : string
        The UMM-Var ID of the variable
    name : string
        The UMM-Var short name of the variable, typically identifies layer name found in the science data file
    """
    def __init__(self, message_data):
        """
        Constructor

        Parameters
        ----------
        message_data : dictionary
            The Harmony message "variables" item to deserialize
        """
        super().__init__(message_data, properties=['id', 'name'])

class Granule(JsonObject):
    """
    A science granule as found in a Harmony source's "granules" list

    Attributes
    ----------
    id : string
        The CMR Granule ID of the granule
    name: string
        The granule's short name
    url: string
        The URL to the granule, preferentially an S3 URL.  Potentially behind EDL
    """
    def __init__(self, message_data):
        """
        Constructor

        Parameters
        ----------
        message_data : dictionary
            The Harmony message "granules" item to deserialize
        """
        super().__init__(message_data, properties=['id', 'name', 'url'])
        self.local_filename = None
        self.collection = None
        self.variables = []

class MinMax(JsonObject):
    """
    Min and max parameters as found in a Harmony message's "format.scaleExtent.[x|y]" objects

    Attributes
    ----------
    min: float
        The min value for the attribute
    max: float
        The max value for the attribute
    """
    def __init__(self, message_data):
        """
        Constructor

        Parameters
        ----------
        message_data : dictionary
            The Harmony message "format.scaleExtent.[x|y]" object to deserialize
        """
        super().__init__(message_data, properties=['min','max'])

class ScaleExtent(JsonObject):
    """
    Scale extent parameters as found in a Harmony message's "format.scaleExtent" object

    Attributes
    ----------
    x: message.MinMax
        The min and max values for the scale extent for the X dimension
    y: message.MinMax
        The min and max values for the scale extent for the Y dimension
    """
    def __init__(self, message_data):
        """
        Constructor

        Parameters
        ----------
        message_data : dictionary
            The Harmony message "format.scaleExtent" object to deserialize
        """
        super().__init__(message_data, properties=['x', 'y'])
        if self.x is not None:
            self.x = MinMax(self.x)
        if self.y is not None:
            self.y = MinMax(self.y)

class ScaleSize(JsonObject):
    """
    Scale size parameters as found in a Harmony message's "format.scaleSize" object

    Attributes
    ----------
    x: float
        The scale size for the X dimension
    y: float
        The scale size for the Y dimension
    """
    def __init__(self, message_data):
        """
        Constructor

        Parameters
        ----------
        message_data : dictionary
            The Harmony message "format.scaleExtent" object to deserialize
        """
        super().__init__(message_data, properties=['x', 'y'])

class Format(JsonObject):
    """
    Output format parameters as found in a Harmony message's "format" object

    Attributes
    ----------
    crs: string
        A proj4 string or EPSG code corresponding to the desired output projection
    isTransparent: boolean
        A boolean corresponding to whether or not nodata values should be set to transparent
        in the output if the file format allows it
    mime: string
        The mime type of the desired output file
    width: integer
        The pixel width of the desired output
    height: integer
        The pixel height of the desired output
    dpi: integer
        The number of pixels per inch in the desired output file, for image output formats
        that support it
    interpolation: string
        The interpolation method
    scaleExtent: message.ScaleExtent
        The scale extent in the x and y dimensions
    scaleSize: message.ScaleSize
        The scale size in the x and y dimensions
    """
    def __init__(self, message_data):
        """
        Constructor

        Parameters
        ----------
        message_data : dictionary
            The Harmony message "format" object to deserialize
        """
        super().__init__(message_data, properties=[
            'crs',
            'isTransparent',
            'mime',
            'width',
            'height',
            'dpi',
            'interpolation',
            'scaleExtent',
            'scaleSize'
        ])
        if self.scaleExtent is not None:
            self.scaleExtent = ScaleExtent(self.scaleExtent)
        if self.scaleSize is not None:
            self.scaleSize = ScaleSize(self.scaleSize)

class RemoteResource(JsonObject):
    """
    Remote resource

    Attributes
    ----------
    uri : string
        A string of the remote resource location
    type : string
        The resource's content type
    """
    def __init__(self, message_data):
        """
        Constructor

        Parameters
        ----------
        message_data : dictionary
            The Harmony message remote resource object to deserialize
        """
        super().__init__(message_data, properties=['href', 'type'])

class Subset(JsonObject):
    """
    Subsetting parameters as found in a Harmony message's "subset" object

    Attributes
    ----------
    bbox : list
        A list of 4 floating point values corresponding to [West, South, East, North]
        coordinates
    """
    def __init__(self, message_data):
        """
        Constructor

        Parameters
        ----------
        message_data : dictionary
            The Harmony message "subset" object to deserialize
        """
        super().__init__(message_data, properties=['bbox', 'shape'])
        if self.shape is not None:
            self.shape = RemoteResource(self.shape)

class Temporal(JsonObject):
    """
    Temporal subsetting parameters as found in a Harmony message's "temporal" object

    Attributes
    ----------
    start : string
        An ISO 8601 datetime string for the earliest time for temporal subsetting
    end : string
        An ISO 8601 datetime string for the latest time for temporal subsetting
    """
    def __init__(self, message_data):
        """
        Constructor

        Parameters
        ----------
        message_data : dictionary
            The Harmony message "subset" object to deserialize
        """
        super().__init__(message_data, properties=['start', 'end'])

class Message(JsonObject):
    """
    Top-level object corresponding to an incoming Harmony message.  Constructing
    this with a JSON string will deserialize the message into native Python object,
    perform any necessary version interpretation, and add some helpers to make access
    easier.  Generally, this object should be created and allowed to produce its
    child objects rather than directly instantiating Subset, Format, etc objects.
    For maximum compatibility and ease of use, services should prefer using objects
    of this class and their children rather than parsing Harmony's JSON.

    Attributes
    ----------
    version : string
        The semantic version of the Harmony message contained in the provided JSON
    callback : string
        The URL that services must POST to when their execution is complete.  Services
        should use the `completed_with_*` methods of a Harmony Adapter to perform
        callbacks to ensure compatibility, rather than directly using this URL
    isSynchronous : bool
        True if a user is awaiting an immediate response, False if the user is expecting
        the service to be performed at a later point.  This may influence prioritization
        of the request and impacts ability to send multi-file responses
    user : string
        The username of the user requesting the service.  If the message is coming from
        Harmony, services can assume that the provided username has been authenticated
    client : string
        A string indicating the client accessing the service, usually the harmony
        environment, e.g. "harmony-sit"
    requestId : string
        A UUID identifying the originating user request.  This should only be used for
        logging and tracing purposes, as a single user request may produce multiple
        service invocations.
    format: message.Format
        The Harmony message's output parameters
    subset: message.Subset
        The Harmony message's subsetting parameters
    temporal: message.Temporal
        The Harmony message's temporal subsetting parameters
    """
    def __init__(self, json_str):
        """
        Builds a Message object and all of its child objects by deserializing the
        provided JSON string and performing any necessary version interpretation.

        Parameters
        ----------
        json_str : string
            The incoming Harmony message string
        """
        self.json = json_str
        super().__init__(json.loads(json_str),
            properties=[
                'version',
                'callback',
                'isSynchronous',
                'user',
                'client',
                'requestId',
                'format',
                'subset',
                'temporal'
                ],
            list_properties={'sources': Source}
        )
        if self.format is not None:
            self.format = Format(self.format)
        if self.subset is not None:
            self.subset = Subset(self.subset)
        if self.temporal is not None:
            self.temporal = Temporal(self.temporal)

    def digest(self):
        """
        Returns a shasum of the message, useful in providing unique output IDs

        Returns
        -------
        string
            The shasum of the message
        """
        return hashlib.sha256(self.json.encode('utf-8')).hexdigest()

    @property
    def granules(self):
        """
        A list of all the granules in all of the data sources.  Each granule
        links back to its source collection and requested variables, so it
        can be more convenient to use this granules list than to traverse
        the data sources themselves if services process granules individually

        Returns
        -------
        list
            A list of Granule objects for all of the granules in the message
        """
        result = []
        for source in self.sources:
            result += source.granules
        return result
