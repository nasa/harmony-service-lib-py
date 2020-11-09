"""
Utility functions to download data from backend data sources so it can be operated on
locally.

Required when using HTTPS, allowing Earthdata Login auth.  Prints a warning if not supplied:
    EDL_CLIENT_ID:    The EDL application client id used to acquire an EDL shared access token
    EDL_USERNAME:     The EDL application username used to acquire an EDL shared access token
    EDL_PASSWORD:     The EDL application password used to acquire an EDL shared access token
    EDL_REDIRECT_URI: A valid redirect URI for the EDL application (NOTE: the redirect URI is
                      not followed or used; it does need to be in the app's redirect URI list)
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
    return url is not None and url.lower().startswith('http')


def filename(directory_path: str, url: str) -> Path:
    """Constructs a filename from the url using the specified directory as its path."""
    return Path(
        directory_path,
        hashlib.sha256(url.encode('utf-8')).hexdigest()
    ).with_suffix(PurePath(url).suffix)


def optimized_url(url, local_hostname):
    """Return a version of the url optimized for local development."""
    return url \
        .replace('//localhost', local_hostname) \
        .replace('file://', '')

    if not url.startswith('http') and not url.startswith('s3'):
        return url

    return url


class NullHTTPRedirectHandler(HTTPRedirectHandler):
    """Returns a handler that does not follow any redirects."""
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

    EDL_USERNAME: The username to be passed to Earthdata Login when challenged
    EDL_PASSWORD: The password to be passed to Earthdata Login when challenged

    Returns
    -------
    opener : urllib.request.OpenerDirector
        An OpenerDirector that can be used to to open a URL using the
        EDL credentials, if they are available. The OpenerDirector is
        memoized so that subsequent calls use the same opener and
        cookie(s).
    """
    try:
        auth_handler = HTTPBasicAuthHandler(HTTPPasswordMgrWithDefaultRealm())
        edl_endpoints = ['https://sit.urs.earthdata.nasa.gov',
                         'https://uat.urs.earthdata.nasa.gov',
                         'https://urs.earthdata.nasa.gov']
        auth_handler.add_password(None, edl_endpoints, config.edl_username, config.edl_password)

        cookie_processor = HTTPCookieProcessor(CookieJar())

        return build_opener(auth_handler, cookie_processor)

    except KeyError:
        logger.warning('Earthdata Login environment variables EDL_USERNAME and EDL_PASSWORD must '
                       'be set up for authenticated downloads. Requests will be unauthenticated.')
        return build_opener()


def _auth_header(config, access_token=None, include_basic_auth=False):
    """Returns a tuple representing an HTTP Authorization header.

    Conforms to RFC 6750: The OAuth 2.0 Authorization Framework: Bearer Token Usage
    See: https://tools.ietf.org/html/rfc6750

    Parameters
    ----------
    access_token : string (optional)
        The Earthdata Login token for the user making the request. Default: None.
    include_basic_auth : bool (optional)
        Include a Basic auth header using EDL_USERNAME & EDL_PASSWORD. Default: False.

    Returns
    -------
    authorization token : tuple
        A tuple with the Authorization header name and Bearer token value.
    """
    values = []

    if access_token is not None:
        values.append(f'Bearer {access_token}')

    if include_basic_auth:
        edl_creds = b64encode(f"{config.edl_username}:{config.edl_password}".encode('utf-8'))
        edl_creds = edl_creds.decode('utf-8')
        values.append(f"Basic {edl_creds}")

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
    def _edl_request(url, method, access_token=None, get_code=True):
        request = Request(url=url, method=method)
        header = _auth_header(config, access_token=user_access_token, include_basic_auth=True)
        request.add_unredirected_header(*header)
        opener = _create_opener(follow_redirect=False)

        try:
            response = opener.open(request)

            if get_code:
                return None

            body = response.read().decode()
            token_data = json.loads(body)
            # {
            #   "access_token": "abcd1234abcd1234abcd1234",
            #   "token_type": "Bearer",
            #   "expires_in": 36000,
            #   "refresh_token": "9876zyxw9876zyxw9876zyxw",
            #   "endpoint": "/api/users/morpheus"
            # }
            return token_data.get('access_token', None)
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

    # A: Request authorization code
    url = (f"{config.urs_url}/oauth/authorize"
           "?response_type=code"
           f"&client_id={config.edl_client_id}"
           f"&redirect_uri={config.edl_redirect_uri}")
    code = _edl_request(url, 'GET', access_token=user_access_token, get_code=True)

    # B: Retrieve token using authorization code
    url = (f"{config.urs_url}/oauth/token"
           "?grant_type=authorization_code"
           f"&code={code}"
           f"&redirect_uri={config.edl_redirect_uri}")
    token = _edl_request(url, 'POST', get_code=False)

    return token


def _download_from_http_with_bearer_token(config, url, access_token, encoded_data, logger):
    try:
        request = _request_with_bearer_token_auth_header(config, url, access_token, encoded_data)
        opener = _create_opener()
        return opener.open(request)
    except HTTPError as http_error:
        if config.fallback_authn_enabled:
            msg = (f'Failed to download using access token due to {str(http_error)}. '
                   'Trying with EDL_USERNAME and EDL_PASSWORD.')
            logger.exception(msg, exc_info=http_error)
            return _download_from_http_with_basic_auth(config, url, encoded_data, logger)
        raise


def _download_from_http_with_basic_auth(config, url, encoded_data, logger):
    """Fallback: Use basic auth with the application username and password.

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
    return _request_shared_token(config, access_token)


def download_from_http(config, url, destination_path, access_token, logger, data,
                       harmony_exception_klass, forbidden_exception_klass):
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
            response = _download_from_http_with_basic_auth(config, url, data, logger)
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
