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
from urllib.parse import urlparse
import datetime
import sys
import os
import re

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


def _download(config, url: str, access_token: str, data, user_agent=None, **kwargs_download_agent):
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
    user_agent : str
        The user agent that is requesting the download.
        E.g. harmony/0.0.0 (harmony-sit) harmony-service-lib/4.0 (gdal-subsetter)
    kwargs_download_agent: dict
        kwargs to be passed to the download agent
        E.g. stream=True

    Returns
    -------
    requests.Response with the download result

    """
    headers = {}
    if user_agent is not None:
        headers['user-agent'] = user_agent
    auth = EarthdataAuth(config.oauth_uid, config.oauth_password, access_token)
    with _earthdata_session() as session:
        session.auth = auth
        if data is None:
            return session.get(url, headers=headers, timeout=TIMEOUT, **kwargs_download_agent)
        else:
            # Including this header since the stdlib does by default,
            # but we've switched to `requests` which does not.
            headers['Content-Type'] = 'application/x-www-form-urlencoded'
            return session.post(url, headers=headers, data=data, timeout=TIMEOUT)


def _download_with_fallback_authn(config, url: str, data, user_agent=None, **kwargs_download_agent):
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
    user_agent : str
        The user agent that is requesting the download.
        E.g. harmony/0.0.0 (harmony-sit) harmony-service-lib/4.0 (gdal-subsetter)
    kwargs_download_agent: dict
        kwargs to be passed to the download agent
        E.g. stream=True

    Returns
    -------
    requests.Response with the download result

    """
    headers = {}
    if user_agent is not None:
        headers['user-agent'] = user_agent
    auth = requests.auth.HTTPBasicAuth(config.edl_username, config.edl_password)
    if data is None:
        return requests.get(url, headers=headers, timeout=TIMEOUT, auth=auth, **kwargs_download_agent)
    else:
        return requests.post(url, headers=headers, data=data, timeout=TIMEOUT, auth=auth)


def _log_download_performance(logger, url, duration_ms, file_size):
    """Logs a message tracking performance information related to a file download.

    Parameters
    ----------
    logger : logging.Logger
        The logger to use.
    url : str
        The url for the resource to download
    duration_ms: int
        The number of milliseconds the download took
    file_size: int
        The size of the downloaded file
    """
    host = 'Unknown'
    url_path = ''
    try:
        match = re.search('.*://([^/]+)(.*)', url)
        if match:
            host = match.group(1)
            url_path = match.group(2)
    except Exception:
        logger.exception(f'Unable to extract host name from {url}')
    extra_fields = {
        'durationMs': duration_ms,
        'host': host,
        "path": url_path,
        "size": file_size
    }
    logger.info('timing.download.end', extra=extra_fields)


def download(config, url: str, access_token: str, data, destination_file,
             user_agent=None, stream=True, buffer_size=1024*1024*16):
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
    user_agent : str
        The user agent that is requesting the download.
        E.g. harmony/0.0.0 (harmony-sit) harmony-service-lib/4.0 (gdal-subsetter)

    Returns
    -------
    requests.Response with the download result

    Side-effects
    ------------
    Will write to provided destination_file
    NOTE: streaming request is used to download the file,
          and the chunksize is defaulted to 16MB based on the experiment with a large file of 1.8Gb
          for optimized speed and memory consumption.
          If you are experiencing some performance decay for high-throughput small-sized granules,
          you may want to set stream=False.
    """

    response = None
    logger = build_logger(config)
    start_time = datetime.datetime.now()
    logger.info(f'timing.download.start {url}')

    if (not stream) and buffer_size:
        logger.warn(
            f"In download paramters, buffer_size={buffer_size} will be ignored since stream is set to be {stream}."
        )
    elif stream and not isinstance(buffer_size, int):
        raise Exception(f"In download parameters: buffer_size must be integer when stream={stream}.")

    if access_token is not None and _valid(config.oauth_host, config.oauth_client_id, access_token):
        response = _download(config, url, access_token, data, user_agent, stream=stream)

    if response is None or not response.ok:
        if config.fallback_authn_enabled:
            msg = ('No valid user access token in request or EDL OAuth authentication failed.'
                   'Fallback authentication enabled: retrying with Basic auth.')
            logger.warning(msg)
            response = _download_with_fallback_authn(config, url, data, user_agent, stream=stream)

    if response.ok:
        if not stream:
            destination_file.write(response.content)
            file_size = sys.getsizeof(response.content)
        else:
            for chunk in response.iter_content(chunk_size=buffer_size):
                destination_file.write(chunk)
            file_size = os.path.getsize(destination_file.name)
        time_diff = datetime.datetime.now() - start_time
        duration_ms = int(round(time_diff.total_seconds() * 1000))
        duration_logger = build_logger(config)
        _log_download_performance(duration_logger, url, duration_ms, file_size)

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
