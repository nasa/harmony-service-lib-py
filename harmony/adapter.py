"""
==========
adapter.py
==========

Provides BaseHarmonyAdapter, an abstract base class for services to implement
the translation between Harmony messages and service calls and the translation
between service results and Harmony callbacks.
"""

import shutil
import os
import urllib
import logging

from abc import ABC, abstractmethod
from tempfile import mkdtemp, mktemp
from uuid import uuid4

from . import util

class BaseHarmonyAdapter(ABC):
    """
    Abstract base class for Harmony service adapters.  Service implementations
    should inherit from this class and implement the `#invoke(self)` method to
    adapt the Harmony message (`self.message`) into a service call and the
    output of the service call into a response to Harmony (`self.completed_with_*`)

    Services may choose to override methods that do data downloads and result
    staging as well, if they use a different mechanism

    Attributes
    ----------
    message : harmony.Message
        The Harmony input which needs acting upon
    temp_paths : list
        A list of string paths that should be cleaned up on exit
    is_complete : boolean
        True if the service has provided a result to Harmony (and therefore must
        not provide another)
    """
    def __init__(self, message):
        """
        Constructs the adapter

        Parameters
        ----------
        message : harmony.Message
            The Harmony input which needs acting upon
        """
        self.message = message
        self.temp_paths = []
        self.is_complete = False

        logger = logging.getLogger(self.__class__.__name__)
        syslog = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] [%(user)s] %(message)s")
        syslog.setFormatter(formatter)
        logger.addHandler(syslog)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        self.logger = logging.LoggerAdapter(logger, { 'user': message.user })

    @abstractmethod
    def invoke(self):
        """
        Invokes the service to process `self.message`.  Upon completion, the service must
        call one of the `self.completed_with_*` methods to inform Harmony of the completion
        status.
        """
        pass

    def cleanup(self):
        """
        Removes temporary files produced during execution
        """
        for temp_path in self.temp_paths:
            if os.path.isfile(temp_path):
                os.remove(temp_path)  # remove the file
            elif os.path.isdir(temp_path):
                shutil.rmtree(temp_path)  # remove dir and all contains

    def mktemp(self, **flags):
        """
        Calls `tempfile.mktemp`, but adds the file to the list to be cleaned up by `#cleanup`.
        Read the security warnings on `tempfile.mktemp` before use.

        Returns
        -------
        string
            A temporary filename
        """
        filename = mktemp(**flags)
        self.temp_paths += [filename] # Add it to the list of things to clean up
        return filename


    def download_granules(self, granules=None):
        """
        Downloads all of the granules contained in the message to the given temp directory, giving each
        a unique filename.

        Parameters
        ----------
        granules : list
            A list of harmony.message.Granule objects corresponding to the granules to download.  Default:
            all granules in the incoming message
        """
        temp_dir = mkdtemp()
        self.temp_paths += [temp_dir]

        granules = granules or self.message.granules

        # Download the remote file
        for granule in granules:
            granule.local_filename = util.download(granule.url, temp_dir, self.logger)

    def stage(self, local_file, remote_filename=None, mime=None):
        """
        Stages a file on the local filesystem to S3 with the given remote filename and mime type for
        user access.

        Parameters
        ----------
        local_file : string
            The path and name of the file to stage
        remote_filename : string, optional
            The name of the file when staged, by default a UUID with the same extension as the local file
        mime : string, optional
            The mime type of the file, by default the output mime type requested by Harmony

        Returns
        -------
        string
            A URI to the staged file
        """
        # If no remote filename is provided, generate one with a UUID and the same extension as the local file
        if remote_filename is None:
            remote_filename = str(uuid4()) + os.path.splitext(local_file)[1]

        if mime is None:
            mime = self.message.format.mime

        return util.stage(local_file, remote_filename, mime, self.logger)

    def completed_with_error(self, error_message):
        """
        Performs a callback instructing Harmony that there has been an error and providing a
        message to send back to the service user

        Parameters
        ----------
        error_message : string
            The error message to pass on to the service user

        Raises
        ------
        Exception
            If a callback has already been performed
        """

        if self.is_complete:
            raise Exception('Attempted to error an already-complete service call with message ' + error_message)
        self._completed_with_post('/response?error=%s' % (urllib.parse.quote(error_message)))

    def completed_with_redirect(self, url):
        """
        Performs a callback instructing Harmony to redirect the service user to the given URL

        Parameters
        ----------
        url : string
            The URL where the service user should be redirected

        Raises
        ------
        Exception
            If a callback has already been performed
        """

        if self.is_complete:
            raise Exception('Attempted to redirect an already-complete service call to ' + url)
        self._completed_with_post('/response?redirect=%s' % (urllib.parse.quote(url)))

    def completed_with_local_file(self, filename, remote_filename=None, mime=None):
        """
        Indicates that the service has completed with the given file as its result.  Stages the
        provided local file to a user-accessible S3 location and instructs Harmony to redirect
        to that location.

        Parameters
        ----------
        filename : string
            The path and name of the local file
        remote_filename : string, optional
            The name of the file when staged, which will be visible to the user requesting data.
            By default a UUID with the same extension as the local file
        mime : string, optional
            The mime type of the file, by default the output mime type requested by Harmony
        """
        url = self.stage(filename, remote_filename, mime)
        self.completed_with_redirect(url)

    def _completed_with_post(self, path):
        """
        POSTs to the Harmony callback URL at the given path, which may include query params

        Parameters
        ----------
        path : string
            The URL path relative to the Harmony callback URL which should be POSTed to

        Returns
        -------
        None
        """

        url = self.message.callback + path
        if os.environ.get('ENV') in ['dev', 'test']:
            self.logger.warn("ENV=" + os.environ['ENV'] + " so we will not reply to Harmony with POST " + url)
        else:
            self.logger.info('Starting response: %s', url)
            request = urllib.request.Request(url, method='POST')
            response = urllib.request.urlopen(request).read().decode('utf-8')
            self.logger.info('Remote response: %s', response)
            self.logger.info('Completed response: %s', url)
        self.is_complete = True

