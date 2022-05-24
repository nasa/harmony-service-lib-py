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
import uuid
from abc import ABC
from tempfile import mkdtemp
from warnings import warn

from deprecation import deprecated
from pystac import Catalog, Item, Asset, read_file

from harmony.exceptions import CanceledException
from harmony.logging import build_logger
from harmony.message import Temporal
from harmony.util import touch_health_check_file
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
    is_canceled: boolean
        True if the request has been canceled by a Harmony user or operator
    logger: Logger
        Logger specific to this request
    is_failed: boolean
        True if the request failed to execute successfully
    """

    def __init__(self, message, catalog=None, config=None):
        """
        Constructs the adapter

        Parameters
        ----------
        message : harmony.Message
            The Harmony input which needs acting upon
        catalog : pystac.Catalog
            A STAC catalog containing the files on which to act
        config : harmony.util.Config
            The configuration values for this runtime environment.
        """
        if catalog is None:
            warn('Invoking adapter.BaseHarmonyAdapter without a STAC catalog is deprecated',
                 DeprecationWarning, stacklevel=2)

        self.message = message
        self.catalog = catalog
        self.config = config

        if self.config is not None:
            self.init_logging()
        else:
            self.logger = logging.getLogger()

        # Properties that will be deprecated
        self.temp_paths = []
        self.is_complete = False
        self.is_canceled = False
        self.is_failed = False

    def set_config(self, config):
        self.config = config
        if self.config is not None:
            self.init_logging()

    def init_logging(self):
        user = self.message.user if hasattr(self.message, 'user') else None
        req_id = self.message.requestId if hasattr(self.message, 'requestId') else None
        logging_context = {
            'user': user,
            'requestId': req_id
        }
        self.logger = logging.LoggerAdapter(build_logger(self.config), logging_context)

    def invoke(self):
        """
        Invokes the service to process `self.message`.  By default, this will call process_item
        on all items in the input catalog

        Returns
        -------
        (harmony.Message, pystac.Catalog)
            A tuple of the Harmony message, with any processed fields marked as such and
            a STAC catalog describing the output
        """
        # New-style processing using STAC
        if self.catalog:
            return (self.message, self._process_catalog_recursive(self.catalog))

        # Current processing using callbacks
        self._process_with_callbacks()

    def get_all_catalog_items(self, catalog: Catalog, follow_page_links=True):
        """
        Returns a lazy sequence of all the items (including from child catalogs) in the catalog.
        Can handle paged catalogs (catalogs with next/prev).

        Parameters
        ----------
        catalog : pystac.Catalog
            The catalog from which to get items
        follow_page_links : boolean
            Whether or not to follow 'next' links - defaults to True

        Returns
        -------
        A generator that can be iterated to provide a lazy sequence of `pystac.Item`s
        """
        # Return immediate items and items from sub-catalogs
        for item in catalog.get_all_items():
            yield item

        # process 'next' link if present
        if follow_page_links:
            link = catalog.get_single_link(rel='next')
            if link:
                next_catalog = read_file(link.get_href())
                next_items = self.get_all_catalog_items(next_catalog, True)
                for item in next_items:
                    yield item

    def _process_catalog_recursive(self, catalog):
        """
        Helper method to recursively process a catalog and all of its children, producing a new
        output catalog of the results

        Parameters
        ----------
        catalog : pystac.Catalog
            The catalog to process

        Returns
        -------
        pystac.Catalog
            A new catalog containing all of the processed results
        """
        result = catalog.clone()
        result.id = str(uuid.uuid4())

        # Recursively process all sub-catalogs
        children = catalog.get_children()
        result.clear_children()
        result.add_children([self._process_catalog_recursive(child) for child in children])

        # Process immediate child items
        items = catalog.get_items()
        item_count = 0
        result.clear_items()
        source = None
        for item in items:
            item_count = item_count + 1
            source = source or self._get_item_source(item)
            output_item = self.process_item(item.clone(), source)
            if output_item:
                # Ensure the item gets a new ID
                if output_item.id == item.id:
                    output_item.id = str(uuid.uuid4())
                result.add_item(output_item)
        self.logger.info(f'Processed {item_count} granule(s)')

        # process 'next' link if present
        link = catalog.get_single_link(rel='next')
        if link:
            next_catalog = read_file(link.get_href())
            result.add_child(self._process_catalog_recursive(next_catalog))

        return result

    def _process_with_callbacks(self):
        """
        Method for backward compatibility with non-chaining workflows.  Takes an incoming message
        containing granules, translates the granules into STAC items, and passes them individually
        to process_item
        """
        item_count = sum([len(source.granules) for source in self.message.sources])
        completed = 0
        for source in self.message.sources:
            for granule in source.granules:
                item = Item(granule.id, None, granule.bbox, None, {
                    'start_datetime': granule.temporal.start,
                    'end_datetime': granule.temporal.end
                })
                item.add_asset('data', Asset(granule.url, granule.name, roles=['data']))
                result = self.process_item(item, source)
                if not result:
                    continue
                assets = [v for k, v in result.assets.items() if 'data' in (v.roles or [])]
                completed += 1
                progress = int(100 * completed / item_count)
                for asset in assets:
                    temporal = Temporal({}, result.properties['start_datetime'], result.properties['end_datetime'])
                    common_args = dict(
                        title=asset.title,
                        mime=asset.media_type,
                        source_granule=granule,
                        temporal=temporal,
                        bbox=result.bbox
                    )
                    if self.message.isSynchronous:
                        self.completed_with_redirect(asset.href, **common_args)
                        return
                    self.async_add_url_partial_result(asset.href, progress=progress, **common_args)
        self.async_completed_successfully()

    def process_item(self, item, source):
        """
        Given a pystac.Item and a message.Source (collection and variables to subset), processes the
        item, returning a new pystac.Item that describes the output location and metadata

        Optional abstract method. Required if the default #invoke implementation is used.  Services
        processing one input file at a time can simplify adapter code by overriding this method.


        Parameters
        ----------
        item : pystac.Item
            the item that should be processed
        source : harmony.message.Source
            the input source defining the variables, if any, to subset from the item

        Returns
        -------
        pystac.Item
            a STAC item whose metadata and assets describe the service output
        """
        raise NotImplementedError('subclasses must implement #process_item or override #invoke')

    def _get_item_source(self, item):
        """
        Given a STAC item, finds and returns the item's data source in this.message.  It
        specifically looks for a link with relation "harmony_source" in the item and all
        parent catalogs.  The href on that link is the source collection landing page, which
        can identify a source.  If no relation exists and there is only one source in the
        message, returns the message source.

        Parameters
        ----------
        item : pystac.Item
            the item whose source is needed

        Raises
        ------
        RuntimeError
            if no input source could be unambiguously determined, which indiciates a
            misconfiguration or bad input message

        Returns
        -------
        harmony.message.Source
            The source of the input item
        """
        parent = item
        sources = parent.get_links('harmony_source')
        while len(sources) == 0 and parent.get_parent() is not None:
            parent = parent.get_parent()
            sources = parent.get_links('harmony_source')
        if len(sources) == 0:
            if len(self.message.sources) == 1:
                return self.message.sources[0]
            else:
                raise RuntimeError('Could not match STAC catalog to an input source')
        href = sources[0].target
        collection = href.split('/').pop()
        return next(source for source in self.message.sources if source.collection == collection)

    # All methods below are deprecated as we move to STAC-based chaining workflows without callbacks

    @deprecated(details='Services must update to process and output STAC catalogs')
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

    @deprecated(details='Services must update to process and output STAC catalogs')
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
                                                   access_token=self.message.accessToken, cfg=self.config)

    @deprecated(details='Services must update to process and output STAC catalogs')
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

        return util.stage(local_file, remote_filename, mime, location=self.message.stagingLocation,
                          logger=self.logger, cfg=self.config)

    @deprecated(details='Services must update to process and output STAC catalogs')
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
        self._callback_response({'error': error_message})
        self.is_complete = True

    @deprecated(details='Services must update to process and output STAC catalogs')
    def completed_with_redirect(
            self,
            url,
            title=None,
            mime=None,
            source_granule=None,
            temporal=None,
            bbox=None):
        """
        Performs a callback instructing Harmony to redirect the service user to the given URL

        Parameters
        ----------
        url : string
            The URL where the service user should be redirected
        mime : string, optional
            The mime type of the file, by default the output mime type requested by Harmony
        title : string, optional
            Textual information to provide users along with the link
        temporal : harmony.message.Temporal, optional
            The temporal extent of the provided file.  If not provided, the source granule's
            temporal will be used when a source granule is provided
        bbox : list, optional
            List of [West, South, East, North] for the MBR of the provided result.  If not provided,
            the source granule's bbox will be used when a source granule is provided

        Raises
        ------
        Exception
            If a callback has already been performed
        """

        if self.is_complete:
            raise Exception(
                'Attempted to redirect an already-complete service call to ' + url)
        params = self._build_callback_item_params(url, mime=mime, source_granule=source_granule)
        params['status'] = 'successful'
        self._callback_response(params)
        self.is_complete = True

    @deprecated(details='Services must update to process and output STAC catalogs')
    def completed_with_local_file(
            self,
            filename,
            source_granule=None,
            remote_filename=None,
            is_variable_subset=False,
            is_regridded=False,
            is_subsetted=False,
            mime=None,
            title=None,
            temporal=None,
            bbox=None):
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
        title : string, optional
            Textual information to provide users along with the link
        temporal : harmony.message.Temporal, optional
            The temporal extent of the provided file.  If not provided, the source granule's
            temporal will be used when a source granule is provided
        bbox : list, optional
            List of [West, South, East, North] for the MBR of the provided result.  If not provided,
            the source granule's bbox will be used when a source granule is provided

        Raises
        ------
        Exception
            If a callback has already been performed
        """
        url = self.stage(filename, source_granule, remote_filename,
                         is_variable_subset, is_regridded, is_subsetted, mime)
        self.completed_with_redirect(url, title, mime, source_granule, temporal, bbox)

    @deprecated(details='Services must update to process and output STAC catalogs')
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
            The temporal extent of the provided file.  If not provided, the source granule's
            temporal will be used when a source granule is provided
        bbox : list, optional
            List of [West, South, East, North] for the MBR of the provided result.  If not provided,
            the source granule's bbox will be used when a source granule is provided

        Raises
        ------
        Exception
            If the request is synchronous or the request has already been marked complete
        """
        url = self.stage(filename, source_granule, remote_filename,
                         is_variable_subset, is_regridded, is_subsetted, mime)
        self.async_add_url_partial_result(url, title, mime, progress, source_granule,
                                          temporal, bbox)

    @deprecated(details='Services must update to process and output STAC catalogs')
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
            The temporal extent of the provided file.  If not provided, the source granule's
            temporal will be used when a source granule is provided
        bbox : list, optional
            List of [West, South, East, North] for the MBR of the provided result.  If not provided,
            the source granule's bbox will be used when a source granule is provided

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
        params = self._build_callback_item_params(url, title, mime, source_granule, temporal, bbox)
        if progress is not None:
            params['progress'] = progress

        self._callback_response(params)

    @deprecated(details='Services must update to process and output STAC catalogs')
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
        self._callback_response({'status': 'successful'})
        self.is_complete = True

    @deprecated(details='Services must update to process and output STAC catalogs')
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

    # Deprecated internal methods below

    def _build_callback_item_params(
            self,
            url,
            title=None,
            mime=None,
            source_granule=None,
            temporal=None,
            bbox=None):
        """
        Builds the "item[...]" parameters required for a callback to Harmony for the given
        params, returning them as a string param / string value dict.

        Parameters
        ----------
        url : string
            The URL where the service user should be redirected
        title : string, optional
            Textual information to provide users along with the link
        mime : string, optional
            The mime type of the file, by default the output mime type requested by Harmony
        source_granule : message.Granule, optional
            The granule from which the file was derived, if it was derived from a single granule.  This
            will be used to produce a canonical filename and assist when temporal and bbox are not specified
        temporal : harmony.message.Temporal, optional
            The temporal extent of the provided file.  If not provided, the source granule's
            temporal will be used when a source granule is provided
        bbox : list, optional
            List of [West, South, East, North] for the MBR of the provided result.  If not provided,
            the source granule's bbox will be used when a source granule is provided

        Returns
        -------
        dict
            A dictionary containing a mapping of query parameters to value for the given params
        """
        if mime is None:
            mime = self.message.format.mime
        if source_granule is not None:
            temporal = temporal or source_granule.temporal
            bbox = bbox or source_granule.bbox

        params = {'item[href]': url, 'item[type]': mime}
        if title is not None:
            params['item[title]'] = title
        if temporal is not None:
            params['item[temporal]'] = ','.join([temporal.start, temporal.end])
        if bbox is not None:
            params['item[bbox]'] = ','.join([str(c) for c in bbox])
        return params

    def _callback_response(self, query_params):
        """
        POSTs to the Harmony callback URL at the given path with the given params

        Parameters
        ----------
        query_params : dict
            A mapping of string key to string value query params to send to the callback

        Returns
        -------
        None
        """

        param_strs = ['%s=%s' % (k, urllib.parse.quote(str(v)))
                      for k, v in query_params.items()]
        self._callback_post('/response?' + '&'.join(param_strs))

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
        touch_health_check_file(self.config.health_check_path)
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
            except urllib.error.HTTPError as e:
                self.is_failed = True
                body = e.read().decode()
                msg = f'Harmony returned an error when updating the job: {body}'
                self.logger.error(msg, exc_info=e)
                if e.code == 409:
                    self.logger.warning('Harmony request was canceled.')
                    self.is_canceled = True
                    self.is_complete = True
                    raise CanceledException
                raise
            except Exception as e:
                self.is_failed = True
                self.logger.error('Error when updating the job', exc_info=e)
                raise
