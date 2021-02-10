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
    json_object = json.loads(body)
    return (f"Request could not be completed because you need to agree to the EULA "
            f"at {json_object['resolution_url']}")


@lru_cache(maxsize=128)
def _valid(oauth_host: str, oauth_client_id: str, access_token: str) -> bool:
    url = f'{oauth_host}/oauth/tokens/user?token={access_token}&client_id={oauth_client_id}'
    response = requests.post(url, timeout=TIMEOUT)

    if response.ok:
        return True

    raise Exception(response.json())


@lru_cache(maxsize=128)
def _earthdata_session():
    return EarthdataSession()


def _download(config, url: str, access_token: str, data):
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
    auth = requests.auth.HTTPBasicAuth(config.edl_username, config.edl_password)
    if data is None:
        return requests.get(url, timeout=TIMEOUT, auth=auth)
    else:
        return requests.post(url, data=data, timeout=TIMEOUT, auth=auth)


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
        logger.info(msg)
        raise ForbiddenException(msg)

    if response.status_code in (401, 403):
        msg = f'Forbidden: Unable to download {url}'
        logger.info(msg)
        raise ForbiddenException(msg)

    if response.status_code == 500:
        raise Exception('Unable to download.')

    raise Exception('Unable to download: unknown error.')
