"""
Utility functions to download data from backend data sources so it can be operated on
locally.

When downloading from an EDL-token aware data source, this module uses EDL shared /
federated token authentication. It includes an optional fallback authentication that
uses an EDL user to download data when the feature is enabled.

This module relies on the harmony.util.config and its environment variables to be
set for correct operation. See that module and the project README for details.
"""

import hashlib
import json
import logging
from pathlib import Path, PurePath
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse

from harmony.earthdata import EarthdataAuth, EarthdataSession


def is_http(url: str) -> bool:
    """Predicate to determine if the url is an http endpoint.

    Parameters
    ----------
    url : str
        The URL to check

    Returns
    -------
    bool
        Whether the URL is an http endpoint.

    """
    return url is not None and urlparse(url).scheme in ['http', 'https']


def filename(directory_path: str, url: str) -> Path:
    """Constructs a filename from the url using the specified directory
    as its path. The constructed filename will be a sha256 hash
    (converted to a hex digest) of the url, and the file's extension
    will be the same as that of the filename in the url.

    Parameters
    ----------
    directory_path : str
        The url to use when constructing the filename and extension.
    Returns
    -------

    """
    return Path(
        directory_path,
        hashlib.sha256(url.encode('utf-8')).hexdigest()
    ).with_suffix(PurePath(url).suffix)


def optimized_url(url, local_hostname):
    """Return a version of the url optimized for local development.

    1. If the url includes the string `localhost`, it will be replaced by
    the `local_hostname`.

    2. If the url is a `file://` url, it will return the remaining
    part of the url so it can be used as a local file path.

    If neither of the above, then the url is returned unchanged.

    Parameters
    ----------
    url : str
        The url to check and optimize.
    Returns
    -------
    str : The url, possibly converted to a localhost reference or filename.
    """
    return url \
        .replace('localhost', local_hostname) \
        .replace('file://', '')


def _handle_possible_eula_error(http_error, body):
    """
    Tries to determine if the exception is due to a EULA that the user needs to
    approve, and if so, returns a response with the url where they can do so.

    Parameters
    ----------
    http_error : urllib.error.HTTPError
        An error response that may indicate a EULA issue.
    body : str
        The body JSON string that may contain the EULA details.

    Returns
    -------
    str
        A message indicating that the user needs to approve a EULA.
    """
    try:
        # Try to determine if this is a EULA error
        json_object = json.loads(body)
        eula_error = "error_description" in json_object and "resolution_url" in json_object
        if eula_error:
            body = (f"Request could not be completed because you need to agree to the EULA "
                    f"at {json_object['resolution_url']}")
    finally:
        raise Exception(body) from http_error


def download(config, url: str, access_token: str, data, destination_file):
    """.

    Parameters
    ----------
    config : harmony.util.Config
        The configuration for the current runtime environment.
    url : str
        The url for the resource to download
    access_token : str
        A shared EDL access token created from the user's access token
        and the app identity.
    data : dict or Tuple[str, str]
        Optional parameter for additional data to send to the server
        when making an HTTP POST request. These data will be URL
        encoded to a query string containing a series of `key=value`
        pairs, separated by ampersands. If None (the default), the
        request will be sent with an HTTP GET request.
    destination_file : file-like
        The destination file where the data will be written. Must be
        a file-like object opened for binary write.

    """

    # TODO: Pending move of logging from util to separate module.
    logger = logging.getLogger()

    try:
        logger.info('Downloading %s', url)

        if data is not None:
            logger.info('Query parameters supplied, will use POST method.')
            data = urlencode(data).encode('utf-8')

        response = None

        if access_token is not None:
            auth = EarthdataAuth(config.oauth_uid, config.edl_password, access_token)
            with EarthdataSession() as session:
                session.auth = auth
                response = session.get(url)
                # TODO: Fallback authn
        elif config.fallback_authn_enabled:
            msg = ('No user access token in request. Fallback authentication enabled.')
            logger.warning(msg)
            # TODO: Fallback authn
            # response = _download_with_fallback_authn(config, url, data, logger)
        else:
            msg = f"Unable to download: Missing user access token & fallback not enabled for {url}"
            logging.error(msg)
            raise Exception(msg)

        logger.info(f'Completed {url}')

        return response

    except HTTPError as http_error:
        code = http_error.getcode()
        logger.error(f'Download failed with status code: {code}')
        body = http_error.read().decode()
        logger.error('Failed to download URL: {body}')

        if code in (401, 403):
            _handle_possible_eula_error(http_error, body)

        raise
