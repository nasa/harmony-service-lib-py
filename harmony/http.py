"""
Utility functions to download data from backend data sources so it can be operated on
locally.

When downloading from an EDL-token aware data source, this module uses EDL shared /
federated token authentication. It includes an optional fallback authentication that
uses an EDL user to download data when the feature is enabled.

This module relies on the harmony.util.config and its environment variables to be
set for correct operation. See that module and the project README for details.
"""

from functools import lru_cache
import json
from urllib.parse import urlencode, urlparse

import requests

from harmony.earthdata import EarthdataAuth, EarthdataSession
from harmony.exceptions import ForbiddenException
from harmony.logging import build_logger

# Timeout in seconds.  Per requests docs, this is not a time limit on
# the entire response download; rather, an exception is raised if the
# server has not issued a response for timeout seconds (more
# precisely, if no bytes have been received on the underlying socket
# for timeout seconds).  See:
# https://2.python-requests.org/en/master/user/quickstart/#timeouts
TIMEOUT = 60


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


def localhost_url(url, local_hostname):
    """Return a version of the url optimized for local development.

    If the url includes the string `localhost`, it will be replaced by
    the `local_hostname`.

    Parameters
    ----------
    url : str
        The url to check
    Returns
    -------
    str : The url, possibly converted to use a different local hostname
    """
    return url.replace('localhost', local_hostname)


def _is_eula_error(body: str) -> bool:
    """
    Tries to determine if the exception is due to a EULA that the user needs to
    approve, and if so, returns a response with the url where they can do so.

    Parameters
    ----------
    body: The body JSON string that may contain the EULA details.

    Returns
    -------
    A boolean indicating if the body contains a EULA error
    """
    try:
        json_object = json.loads(body)
        return "error_description" in json_object and "resolution_url" in json_object
    except Exception:
        return False


def _eula_error_message(body: str) -> str:
    """
    Constructs a user-friendly error indicating the required EULA
    acceptance and the URL where the user can do so.

    Parameters
    ----------
    body: The body JSON string that may contain the EULA details.

    Returns
    -------
    The string with the EULA message
    """
    json_object = json.loads(body)
    return (f"Request could not be completed because you need to agree to the EULA "
            f"at {json_object['resolution_url']}")


@lru_cache(maxsize=128)
def _valid(oauth_host: str, oauth_client_id: str, access_token: str) -> bool:
    """
    Validates the user access token with Earthdata Login.

    Parameters
    ----------
    oauth_host: The Earthdata Login hostname
    oauth_client_id: The EDL application's client id
    access_token: The user's access token to validate

    Returns
    -------
    Boolean indicating a valid or invalid user access token
    """
    url = f'{oauth_host}/oauth/tokens/user?token={access_token}&client_id={oauth_client_id}'
    response = requests.post(url, timeout=TIMEOUT)

    if response.ok:
        return True

    raise Exception(response.json())


@lru_cache(maxsize=128)
def _earthdata_session():
    """Constructs an EarthdataSession for use to download one or more files."""
    return EarthdataSession()


def _download(config, url: str, access_token: str, data):
    """Implements the download functionality.

    Using the EarthdataSession and EarthdataAuth extensions to the
    `requests` module, this function will download the given url and
    perform any necessary Earthdata Login OAuth handshakes.

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

    Returns
    -------
    requests.Response with the download result

    """
    auth = EarthdataAuth(config.oauth_uid, config.oauth_password, access_token)
    with _earthdata_session() as session:
        session.auth = auth
        if data is None:
            return session.get(url, timeout=TIMEOUT)
        else:
            # Including this header since the stdlib does by default,
            # but we've switched to `requests` which does not.
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            return session.post(url, headers=headers, data=data, timeout=TIMEOUT)


def _download_with_fallback_authn(config, url: str, data):
    """Downloads the given url using Basic authentication as a fallback
    mechanism should the normal EDL Oauth handshake fail.

    This function requires the `edl_username` and `edl_password`
    attributes in the config object to be populated with valid
    credentials.

    Parameters
    ----------
    config : harmony.util.Config
        The configuration for the current runtime environment.
    url : str
        The url for the resource to download
    data : dict or Tuple[str, str]
        Optional parameter for additional data to send to the server
        when making an HTTP POST request. These data will be URL
        encoded to a query string containing a series of `key=value`
        pairs, separated by ampersands. If None (the default), the
        request will be sent with an HTTP GET request.

    Returns
    -------
    requests.Response with the download result

    """
    auth = requests.auth.HTTPBasicAuth(config.edl_username, config.edl_password)
    if data is None:
        return requests.get(url, timeout=TIMEOUT, auth=auth)
    else:
        return requests.post(url, data=data, timeout=TIMEOUT, auth=auth)


def download(config, url: str, access_token: str, data, destination_file):
    """Downloads the given url using the provided EDL user access token
    and writes it to the provided file-like object.

    Exception cases:
    1. No user access token
    2. Invalid user access token
    3. Unable to authenticate the user with Earthdata Login
       a. User credentials (could happen even after token validation
       b. Application credentials
    4. Error response when downloading
    5. Data requires EULA acceptance by user
    6. If fallback authentication enabled, the application credentials are
       invalid, or do not have permission to download the data.

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

    Returns
    -------
    requests.Response with the download result

    Side-effects
    ------------
    Will write to provided destination_file
    """

    response = None
    logger = build_logger(config)
    logger.info('Downloading %s', url)

    if data is not None:
        logger.info('Query parameters supplied, will use POST method.')
        data = urlencode(data).encode('utf-8')

    if access_token is not None and _valid(config.oauth_host, config.oauth_client_id, access_token):
        response = _download(config, url, access_token, data)
        if response.ok:
            destination_file.write(response.content)
            logger.info(f'Completed {url}')
            return response

    if config.fallback_authn_enabled:
        msg = ('No valid user access token in request or EDL OAuth authentication failed.'
               'Fallback authentication enabled: retrying with Basic auth.')
        logger.warning(msg)
        response = _download_with_fallback_authn(config, url, data)
        if response.ok:
            destination_file.write(response.content)
            logger.info(f'Completed {url}')
            return response

    if _is_eula_error(response.content):
        msg = _eula_error_message(response.content)
        logger.info(f'{msg} due to: {response.content}')
        raise ForbiddenException(msg)

    if response.status_code in (401, 403):
        msg = f'Forbidden: Unable to download {url}'
        logger.info(f'{msg} due to: {response.content}')
        raise ForbiddenException(msg)

    if response.status_code == 500:
        logger.info(f'Unable to download (500) due to: {response.content}')
        raise Exception('Unable to download.')

    logger.info(f'Unable to download (unknown error) due to: {response.content}')
    raise Exception('Unable to download: unknown error.')
