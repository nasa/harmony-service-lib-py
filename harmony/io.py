"""
Utility functions to download data from backend data sources so it can be operated on
locally.

When downloading from an EDL-token aware data source, this module uses EDL shared /
federated token authentication. It includes an optional fallback authentication that
uses an EDL user to download data when the feature is enabled.

This module relies on the harmony.util.config and its environment variables to be
set for correct operation. See that module and the project README for details.
"""

from base64 import b64encode
from functools import lru_cache
import hashlib
from http.cookiejar import CookieJar
import json
import logging
from pathlib import Path, PurePath
from urllib.error import HTTPError
from urllib.request import (build_opener, Request,
                            HTTPBasicAuthHandler, HTTPCookieProcessor,
                            HTTPPasswordMgrWithDefaultRealm,
                            HTTPRedirectHandler)
from urllib import parse


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
    return url is not None and url.lower().startswith('http')


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
        .replace('//localhost', local_hostname) \
        .replace('file://', '')


class NullHTTPRedirectHandler(HTTPRedirectHandler):
    """
    Returns a handler that does not follow any redirects.

    Parameters
    ----------

    Returns
    -------
    """
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


@lru_cache(maxsize=None)
def _create_opener(follow_redirect=True):
    """Creates a urllib.request.OpenerDirector suitable for use with TEA.

    This opener will handle cookies, which is necessary for the JWT
    cookie that TEA sends on a redirect. If this cookie is not sent to
    TEA when redirected, TEA will fail to deliver the data.

    Returns
    -------
    opener : urllib.request.OpenerDirector
        An OpenerDirector that can be used to to open a URL using the
        EDL credentials, if they are available. The OpenerDirector is
        memoized so that subsequent calls use the same opener and
        cookie(s).
    """
    handlers = []
    if not follow_redirect:
        handlers.append(NullHTTPRedirectHandler())
    handlers.append(HTTPCookieProcessor(CookieJar()))

    return build_opener(*handlers)


@lru_cache(maxsize=None)
def _create_basic_auth_opener(config, logger):
    """Creates an OpenerDirector that will use HTTP(S) cookies and basic auth to open a URL
    using Earthdata Login (EDL) auth credentials.

    Will use Earthdata Login creds only if the following environment
    variables are set and will print a warning if they are not:

    OAUTH_UID: The username to be passed to Earthdata Login when challenged
    OAUTH_PASSWORD: The password to be passed to Earthdata Login when challenged

    Returns
    -------
    opener : urllib.request.OpenerDirector
        An OpenerDirector that can be used to to open a URL using the
        EDL credentials, if they are available. The OpenerDirector is
        memoized so that subsequent calls use the same opener and
        cookie(s).
    """
    auth_handler = HTTPBasicAuthHandler(HTTPPasswordMgrWithDefaultRealm())
    endpoints = ['https://sit.urs.earthdata.nasa.gov',
                 'https://uat.urs.earthdata.nasa.gov',
                 'https://urs.earthdata.nasa.gov']
    auth_handler.add_password(None, endpoints, config.edl_username, config.edl_password)

    cookie_processor = HTTPCookieProcessor(CookieJar())

    return build_opener(auth_handler, cookie_processor)


def _auth_header(config, access_token=None, include_basic_auth=False):
    """Returns a tuple representing an HTTP Authorization header.

    Conforms to RFC 6750: The OAuth 2.0 Authorization Framework: Bearer Token Usage
    See: https://tools.ietf.org/html/rfc6750

    Parameters
    ----------
    access_token : string (optional)
        The Earthdata Login token for the user making the request. Default: None.
    include_basic_auth : bool (optional)
        Include a Basic auth header using OAUTH_UID & OAUTH_PASSWORD. Default: False.

    Returns
    -------
    authorization token : tuple
        A tuple with the Authorization header name and Bearer token value.
    """
    values = []

    if access_token is not None:
        values.append(f'Bearer {access_token}')

    if include_basic_auth:
        creds = b64encode(f"{config.oauth_uid}:{config.oauth_password}".encode('utf-8'))
        creds = creds.decode('utf-8')
        values.append(f"Basic {creds}")

    return ('Authorization', ', '.join(values))


def _request_with_bearer_token_auth_header(config, url, access_token, encoded_data):
    """Returns a Request for a given URL with a non-redirecting
    Authorization header containing the Bearer token.

    The Bearer token should not be included in a redirect; doing so
    can result in a 400 error if the redirect URL from TEA (or other
    backend) is a pre-signed S3 URL, as it will be if TEA receives an
    in-region request (the requester is in the same region as the TEA
    app).

    Parameters
    ----------
    url : string
        The URL to fetch
    access_token :
        The Earthdata Login token of the caller to use for downloads
    data : dict or Tuple[str, str]
        Optional parameter for additional data to send to the server
        when making a HTTP POST request.
    """
    request = Request(url, data=encoded_data)
    auth_header = _auth_header(config, access_token=access_token)
    request.add_unredirected_header(*auth_header)

    return request


def _request_shared_token(config, user_access_token):
    """
    Gets a shared token from Earthdata Login.

            # Example reply body:
            # {
            #   "access_token": "abcd1234abcd1234abcd1234",
            #   "token_type": "Bearer",
            #   "expires_in": 36000,
            #   "refresh_token": "9876zyxw9876zyxw9876zyxw",
            #   "endpoint": "/api/users/morpheus"
            # }
    """
    def _initiate_oauth(access_token):
        url = (f"{config.oauth_host}/oauth/authorize"
               "?response_type=code"
               f"&client_id={config.oauth_client_id}"
               f"&redirect_uri={config.oauth_redirect_uri}")

        request = Request(url=url, method='GET')
        header = _auth_header(config, access_token=user_access_token, include_basic_auth=True)
        request.add_unredirected_header(*header)
        opener = _create_opener(follow_redirect=False)

        try:
            opener.open(request)
            raise Exception("Unable to get user authorization code: Did not receive expected redirect.")
        except HTTPError as http_error:
            code = None
            if 'Location' in http_error.headers or 'location' in http_error.headers:
                location = http_error.headers.get('Location') or http_error.headers.get('location')
                qs = parse.urlsplit(location).query
                items = parse.parse_qsl(qs)
                code = dict(items).get('code', None)
            if code is None:
                raise Exception("Unable to acquire authorization code from user access token")
            else:
                return code

    def _request_oauth_token(code):
        url = (f"{config.oauth_host}/oauth/token"
               "?grant_type=authorization_code"
               f"&code={code}"
               f"&redirect_uri={config.oauth_redirect_uri}")

        request = Request(url=url, method='POST')
        header = _auth_header(config, include_basic_auth=True)
        request.add_unredirected_header(*header)
        opener = _create_opener(follow_redirect=False)

        try:
            response = opener.open(request)

            body = response.read().decode()
            token_data = json.loads(body)
            return token_data.get('access_token', None)
        except HTTPError:
            raise Exception("Unable to acquire authorization code from user access token")

    # Step A: Request authorization code
    code = _initiate_oauth(user_access_token)

    # Step B: Retrieve token using authorization code acquired in step A
    token = _request_oauth_token(code)

    return token


def _download_from_http_with_bearer_token(config, url, access_token, encoded_data, logger):
    """
    Description.

    Parameters
    ----------

    Returns
    -------
    """
    try:
        request = _request_with_bearer_token_auth_header(config, url, access_token, encoded_data)
        opener = _create_opener()
        return opener.open(request)
    except HTTPError as http_error:
        if config.fallback_authn_enabled:
            msg = (f'Failed to download using access token due to {str(http_error)}.'
                   'Fallback authentication enabled.')
            logger.exception(msg, exc_info=http_error)
            return _download_with_fallback_authn(config, url, encoded_data, logger)
        else:
            logger.info("Download failed and fallback authentication not enabled.")
        raise


def _download_with_fallback_authn(config, url, encoded_data, logger):
    """Fallback: Use basic auth with the application uid and password.

    This should only happen in cases where the backend server does
    not yet support the EDL Bearer token authentication.
    """
    request = Request(url, data=encoded_data)
    opener = _create_basic_auth_opener(config, logger)
    return opener.open(request)


def _handle_possible_eula_error(http_error, body, forbidden_exception_klass):
    try:
        # Try to determine if this is a EULA error
        json_object = json.loads(body)
        eula_error = "error_description" in json_object and "resolution_url" in json_object
        if eula_error:
            body = (f"Request could not be completed because you need to agree to the EULA "
                    f"at {json_object['resolution_url']}")
    finally:
        raise forbidden_exception_klass(body) from http_error


@lru_cache(maxsize=None)
def shared_token_for_user(config, access_token):
    """
    Description.

    Parameters
    ----------

    Returns
    -------
    """
    return _request_shared_token(config, access_token)


def download_from_http(config, url, destination_path, access_token, logger, data,
                       harmony_exception_klass, forbidden_exception_klass):
    """
    Description.

    Parameters
    ----------

    Returns
    -------
    """
    try:
        logger.info('Downloading %s', url)

        if data is not None:
            logger.info('Query parameters supplied, will use POST method.')
            data = parse.urlencode(data).encode('utf-8')

        response = None
        if access_token is not None:
            shared_token = shared_token_for_user(config, access_token)
            response = _download_from_http_with_bearer_token(config, url, shared_token, data, logger)
        elif config.fallback_authn_enabled:
            msg = ('No user access token in request. Fallback authentication enabled.')
            logger.warning(msg)
            response = _download_with_fallback_authn(config, url, data, logger)
        else:
            msg = f"Unable to download: Missing user access token & fallback not enabled for {url}"
            logging.error(msg)
            raise harmony_exception_klass(msg, 'Error')

        with open(destination_path, 'wb') as local_file:
            local_file.write(response.read())

        logger.info('Completed %s', url)

        return destination_path

    except HTTPError as http_error:
        code = http_error.getcode()
        logger.error('Download failed with status code: ' + str(code))
        body = http_error.read().decode()
        logger.error('Failed to download URL:' + body)

        if code in (401, 403):
            _handle_possible_eula_error(http_error, body, forbidden_exception_klass)

        raise
