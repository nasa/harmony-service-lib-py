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
from tempfile import mkdtemp

from . import util
from harmony.util import CanceledException, touch_health_check_file


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
    is_canceled: boolean
        True if the request has been canceled by a Harmony user or operator
    logger: Logger
        Logger specific to this request
    is_failed: boolean
        True if the request failed to execute successfully
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
        self.is_canceled = False
        self.is_failed = False

        logging_context = {
            'user': message.user,
            'requestId': message.requestId
        }
        self.logger = \
            logging.LoggerAdapter(util.default_logger, logging_context)

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
                shutil.rmtree(temp_path)  # remove dir and all contents
        self.temp_paths = []

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
            granule.local_filename = util.download(granule.url, temp_dir, logger=self.logger,
                                                   access_token=self.message.accessToken)

    def stage(self, local_file, source_granule=None, remote_filename=None, is_variable_subset=False,
              is_regridded=False, is_subsetted=False, mime=None):
        """
        Stages a file on the local filesystem to S3 with the given remote filename and mime type for
        user access.

        Parameters
        ----------
        local_file : string
            The path and name of the file to stage
        source_granule : message.Granule, optional
            The granule from which the file was derived, if it was derived from a single granule.  This
            will be used to produce a canonical filename
        remote_filename : string, optional
            The name of the file when staged, which will be visible to the user requesting data.
            Specify this if not providing a source granule.  If neither remote_filename nor source_granule
            is provided, the output file will use the file's basename
        is_variable_subset : bool, optional
            True if a variable subset operation has been performed (default: False)
        is_regridded : bool, optional
            True if a regridding operation has been performed (default: False)
        is_subsetted : bool, optional
            True if a subsetting operation has been performed (default: False)
        mime : string, optional
            The mime type of the file, by default the output mime type requested by Harmony

        Returns
        -------
        string
            A URI to the staged file
        """
        if remote_filename is None:
            if source_granule:
                remote_filename = self.filename_for_granule(source_granule, os.path.splitext(
                    local_file)[1], is_variable_subset, is_regridded, is_subsetted)
            else:
                remote_filename = os.path.basename(local_file)

        if mime is None:
            mime = self.message.format.mime

        return util.stage(local_file, remote_filename, mime, location=self.message.stagingLocation, logger=self.logger)

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
        self.is_failed = True
        if self.is_complete and not self.is_canceled:
            raise Exception(
                'Attempted to error an already-complete service call with message ' + error_message)
        self._callback_post('/response?error=%s' %
                            (urllib.parse.quote(error_message)))
        self.is_complete = True

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
            raise Exception(
                'Attempted to redirect an already-complete service call to ' + url)
        self._callback_post('/response?redirect=%s' %
                            (urllib.parse.quote(url)))
        self.is_complete = True

    def completed_with_local_file(
            self,
            filename,
            source_granule=None,
            remote_filename=None,
            is_variable_subset=False,
            is_regridded=False,
            is_subsetted=False,
            mime=None):
        """
        Indicates that the service has completed with the given file as its result.  Stages the
        provided local file to a user-accessible S3 location and instructs Harmony to redirect
        to that location.

        Parameters
        ----------
        filename : string
            The path and name of the local file
        source_granule : message.Granule, optional
            The granule from which the file was derived, if it was derived from a single granule.  This
            will be used to produce a canonical filename
        remote_filename : string, optional
            The name of the file when staged, which will be visible to the user requesting data.
            Specify this if not providing a source granule.  If neither remote_filename nor source_granule
            is provided, the output file will use the file's basename
        is_variable_subset : bool, optional
            True if a variable subset operation has been performed (default: False)
        is_regridded : bool, optional
            True if a regridding operation has been performed (default: False)
        is_subsetted : bool, optional
            True if a subsetting operation has been performed (default: False)
        mime : string, optional
            The mime type of the file, by default the output mime type requested by Harmony

        Raises
        ------
        Exception
            If a callback has already been performed
        """
        url = self.stage(filename, source_granule, remote_filename,
                         is_variable_subset, is_regridded, is_subsetted, mime)
        self.completed_with_redirect(url)

    def async_add_local_file_partial_result(
            self,
            filename,
            source_granule=None,
            remote_filename=None,
            is_variable_subset=False,
            is_regridded=False,
            is_subsetted=False,
            title=None,
            mime=None,
            progress=None,
            temporal=None,
            bbox=None):
        """
        For service requests that are asynchronous, stages the given filename and sends the staged
        URL as a progress update to Harmony.  Optionally also provides a numeric progress indicator.
        Synchronous requests may not call this method and will throw an exeception.

        Parameters
        ----------
        filename : string
            The path and name of the local file
        source_granule : message.Granule, optional
            The granule from which the file was derived, if it was derived from a single granule.  This
            will be used to produce a canonical filename and assist when temporal and bbox are not specified
        remote_filename : string, optional
            The name of the file when staged, which will be visible to the user requesting data.
            Specify this if not providing a source granule.  If neither remote_filename nor source_granule
            is provided, the output file will use the file's basename
        is_variable_subset : bool, optional
            True if a variable subset operation has been performed (default: False)
        is_regridded : bool, optional
            True if a regridding operation has been performed (default: False)
        is_subsetted : bool, optional
            True if a subsetting operation has been performed (default: False)
        title : string, optional
            Textual information to provide users along with the link
        mime : string, optional
            The mime type of the file, by default the output mime type requested by Harmony
        progress : integer, optional
            Numeric progress of the total request, 0-100
        temporal : harmony.message.Temporal, optional
            The temporal extent of the provided file.  If not provided, the source granule's temporal will be
            used when a source granule is provided
        bbox : list, optional
            List of [West, South, East, North] for the MBR of the provided result.  If not provided, the source
            granule's bbox will be used when a source granule is provided

        Raises
        ------
        Exception
            If the request is synchronous or the request has already been marked complete
        """
        url = self.stage(filename, source_granule, remote_filename,
                         is_variable_subset, is_regridded, is_subsetted, mime)
        self.async_add_url_partial_result(url, title, mime, progress, source_granule,
                                          temporal, bbox)

    def async_add_url_partial_result(self, url, title=None, mime=None, progress=None, source_granule=None,
                                     temporal=None, bbox=None):
        """
        For service requests that are asynchronous, stages the provides the given URL as a partial result.
        Optionally also provides a numeric progress indicator.
        Synchronous requests may not call this method and will throw an exeception.

        Parameters
        ----------
        url : string
            The URL where the service user should be redirected
        title : string, optional
            Textual information to provide users along with the link
        mime : string, optional
            The mime type of the file, by default the output mime type requested by Harmony
        progress : integer, optional
            Numeric progress of the total request, 0-100
        source_granule : message.Granule, optional
            The granule from which the file was derived, if it was derived from a single granule.  This
            will be used to produce a canonical filename and assist when temporal and bbox are not specified
        temporal : harmony.message.Temporal, optional
            The temporal extent of the provided file
        bbox : list, optional
            List of [West, South, East, North] for the MBR of the provided result

        Raises
        ------
        Exception
            If the request is synchronous or the request has already been marked complete
        """
        if self.message.isSynchronous:
            raise Exception(
                'Attempted to call back asynchronously to a synchronous request')
        if self.is_complete:
            raise Exception(
                'Attempted to add a result to an already-completed request: ' + url)
        if mime is None:
            mime = self.message.format.mime
        if source_granule is not None:
            temporal = temporal or source_granule.temporal
            bbox = bbox or source_granule.bbox

        params = {'item[href]': url, 'item[type]': mime}
        if title is not None:
            params['item[title]'] = title
        if progress is not None:
            params['progress'] = progress
        if temporal is not None:
            params['item[temporal]'] = ','.join([temporal.start, temporal.end])
        if bbox is not None:
            params['item[bbox]'] = ','.join([str(c) for c in bbox])

        param_strs = ['%s=%s' % (k, urllib.parse.quote(str(v)))
                      for k, v in params.items()]
        callback_url = '/response?' + '&'.join(param_strs)
        self._callback_post(callback_url)

    def async_completed_successfully(self):
        """
        For service requests that are asynchronous, sends a progress update indicating
        that a service request is complete.
        Synchronous requests may not call this method and will throw an exeception.

        Raises
        ------
        Exception
            If the request is synchronous or the request has already been marked complete
        """
        if self.message.isSynchronous:
            raise Exception(
                'Attempted to call back asynchronously to a synchronous request')
        if self.is_complete:
            raise Exception(
                'Attempted to call back for an already-completed request.')
        self._callback_post('/response?status=successful')
        self.is_complete = True

    def filename_for_granule(self, granule, ext, is_variable_subset=False, is_regridded=False, is_subsetted=False):
        """
        Return an output filename for the given granules according to our naming conventions:
        {original filename without suffix}(_{single var})?(_regridded)?(_subsetted)?.<ext>

        Parameters
        ----------
            granule : message.Granule
                The source granule for the output file
            ext: string
                The destination file extension
            is_variable_subset : bool, optional
                True if a variable subset operation has been performed (default: False)
            is_regridded : bool, optional
                True if a regridding operation has been performed (default: False)
            is_subsetted : bool, optional
                True if a subsetting operation has been performed (default: False)

        Returns
        -------
            string
                The output filename
        """
        url = granule.url
        # Get everything between the last non-trailing '/' before the query and the first '?'
        # Do this instead of using a URL parser, because our URLs are not complex in practice and
        # it is useful to allow relative file paths to work for local testing.
        original_filename = url.split('?')[0].rstrip('/').split('/')[-1]
        original_basename = os.path.splitext(original_filename)[0]
        if not ext.startswith('.'):
            ext = '.' + ext

        suffixes = []
        if is_variable_subset and len(granule.variables) == 1:
            suffixes.append('_' + granule.variables[0].name.replace('/', '_'))
        if is_regridded:
            suffixes.append('_regridded')
        if is_subsetted:
            suffixes.append('_subsetted')
        suffixes.append(ext)

        result = original_basename
        # Iterate suffixes in reverse, removing them from the result if they're at the end of the string
        # This supports the case of chaining where one service regrids and another subsets but we don't
        # want names to get mangled
        for suffix in suffixes[::-1]:
            if result.endswith(suffix):
                result = result[:-len(suffix)]

        return result + "".join(suffixes)

    def _callback_post(self, path):
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
        touch_health_check_file()
        if os.environ.get('ENV') in ['dev', 'test']:
            self.logger.warning(
                'ENV=' + os.environ['ENV'] + ' so we will not reply to Harmony with POST ' + url)
        elif self.is_canceled:
            msg = 'Ignoring making callback request because the request has been canceled.'
            self.logger.info(msg)
        else:
            self.logger.info('Starting response: %s', url)
            request = urllib.request.Request(url, method='POST')
            try:
                response = \
                    urllib.request.urlopen(request).read().decode('utf-8')
                self.logger.info('Remote response: %s', response)
                self.logger.info('Completed response: %s', url)
            except Exception as e:
                self.is_failed = True
                body = e.read().decode()
                msg = f'Harmony returned an error when updating the job: {body}'
                self.logger.error(msg)
                if e.code == 409:
                    self.logger.warning('Harmony request was canceled.')
                    self.is_canceled = True
                    self.is_complete = True
                    raise CanceledException
                else:
                    raise e
