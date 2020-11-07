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
from collections import namedtuple
from functools import lru_cache
import hashlib
from http.cookiejar import CookieJar
import json
from os import path
from urllib.error import HTTPError
from urllib.request import (build_opener, Request,
                            HTTPBasicAuthHandler, HTTPCookieProcessor,
                            HTTPPasswordMgrWithDefaultRealm,
                            HTTPRedirectHandler)
from urllib import parse

import harmony.util
import harmony.aws


LOGGER = harmony.util.build_logger()

# TODO: PARAMETERIZE ALL THE THINGS
# urs_url = "https://uat.urs.earthdata.nasa.gov"
# edl_client_id = "kQY_EiF2oM_SZbbkP0Y8Mw"
# edl_redirect_uri = "http%3A//localhost:3000/oauth2/redirect"
# FALLBACK_AUTH_ENABLED = True


Config = namedtuple('Config', 'urs_url client_id username password redirect_uri fallback_authn_enabled')


@lru_cache(maxsize=None)
def _config():
    return Config(harmony.util.get_env('URS_URL') or "xyzzy",
                  harmony.util.get_env('EDL_CLIENT_ID') or "xyzzy",
                  harmony.util.get_env('EDL_USERNAME') or "xyzzy",
                  harmony.util.get_env('EDL_PASSWORD') or "xyzzy",
                  parse.quote_plus(harmony.util.get_env('EDL_REDIRECT_URI')) or "xyzzy",
                  (str.lower(harmony.util.get_env('FALLBACK_AUTHN_ENABLED')) or "xyzzy") == 'true')


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
def _create_basic_auth_opener(logger):
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
        auth_handler.add_password(None, edl_endpoints, _config().username, _config().password)

        cookie_processor = HTTPCookieProcessor(CookieJar())

        return build_opener(auth_handler, cookie_processor)

    except KeyError:
        logger.warning('Earthdata Login environment variables EDL_USERNAME and EDL_PASSWORD must '
                       'be set up for authenticated downloads. Requests will be unauthenticated.')
        return build_opener()


def _auth_header(access_token=None, include_basic_auth=False):
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
        edl_creds = b64encode(f"{_config().username}:{_config().password}".encode('utf-8'))
        edl_creds = edl_creds.decode('utf-8')
        values.append(f"Basic {edl_creds}")

    return ('Authorization', ', '.join(values))


def _request_with_bearer_token_auth_header(url, access_token, encoded_data):
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
    auth_header = _auth_header(access_token=access_token)
    request.add_unredirected_header(*auth_header)

    return request


def _request_shared_token(user_access_token):
    # A: Request authorization code
    auth_code_url = (f"{_config().urs_url}/oauth/authorize"
                     "?response_type=code"
                     f"&client_id={_config().client_id}"
                     f"&redirect_uri={_config().redirect_uri}")
    auth_code_request = Request(url=auth_code_url)
    auth_code_header = _auth_header(access_token=user_access_token, include_basic_auth=True)
    auth_code_request.add_unredirected_header(*auth_code_header)
    opener = _create_opener(follow_redirect=False)

    response = None
    code = None
    try:
        response = opener.open(auth_code_request)
    except HTTPError as http_error:
        if 'Location' in http_error.hdrs:
            qs = parse.urlsplit(http_error.hdrs.get('Location')).query
            items = parse.parse_qsl(qs)
            code = dict(items).get('code', None)

    if code is None:
        raise Exception("Unable to acquire authorization code from user access token")

    # B: Retrieve token using authorization code
    token_url = (f"{_config().urs_url}/oauth/token"
                 "?grant_type=authorization_code"
                 f"&code={code}"
                 f"&redirect_uri={_config().redirect_uri}")
    token_request = Request(url=token_url, method='POST')
    token_header = _auth_header(include_basic_auth=True)
    token_request.add_unredirected_header(*token_header)
    opener = _create_opener(follow_redirect=False)
    response = opener.open(token_request)
    body = response.read().decode()
    token_data = json.loads(body)
    # {
    #   "access_token": "***REMOVED***",
    #   "token_type": "Bearer",
    #   "expires_in": 604800,
    #   "refresh_token": "***REMOVED***",
    #   "endpoint": "/api/users/kbeam"
    # }
    return token_data.get('access_token', None)


def _download_from_http_with_bearer_token(url, access_token, encoded_data, logger):
    try:
        request = _request_with_bearer_token_auth_header(url, access_token, encoded_data)
        opener = _create_opener()
        return opener.open(request)
    except HTTPError as http_error:
        msg = (f'Failed to download using access token due to {str(http_error)}. '
               'Trying with EDL_USERNAME and EDL_PASSWORD.')
        logger.exception(msg, exc_info=http_error)
        if _config().fallback_authn_enabled:
            return _download_from_http_with_basic_auth(url, encoded_data, logger)


def _download_from_http_with_basic_auth(url, encoded_data, logger):
    """Fallback: Use basic auth with the application username and password.

    This should only happen in cases where the backend server does
    not yet support the EDL Bearer token authentication.
    """
    request = Request(url, data=encoded_data)
    opener = _create_basic_auth_opener(logger)
    return opener.open(request)


def _handle_possible_eula_error(http_error, body):
    try:
        # Try to determine if this is a EULA error
        json_object = json.loads(body)
        eula_error = "error_description" in json_object and "resolution_url" in json_object
        if eula_error:
            body = (f"Request could not be completed because you need to agree to the EULA "
                    f"at {json_object['resolution_url']}")
    finally:
        raise harmony.util.ForbiddenException(body) from http_error


@lru_cache(maxsize=None)
def shared_tokens():
    return {}


def _download_from_http(url, access_token, destination, data=None, logger=LOGGER):
    try:
        logger.info('Downloading %s', url)

        response = None

        if data is not None:
            logger.info('Query parameters supplied, will use POST method.')
            data = parse.urlencode(data).encode('utf-8')

        if access_token is not None:
            shared_token = None

            if access_token in shared_tokens():
                shared_token = shared_token().get(access_token)
            else:
                shared_token = _request_shared_token(access_token)
                shared_tokens()[access_token] = shared_token

            response = _download_from_http_with_bearer_token(url, shared_token, data, logger)
        elif _config().fallback_authn_enabled:
            response = _download_from_http_with_basic_auth(url, data, logger)
        else:
            logger.error(f"Unable to download: Missing user access token & fallback not enabled for {url}")

        with open(destination, 'wb') as local_file:
            local_file.write(response.read())

        logger.info('Completed %s', url)

        return destination
    except HTTPError as http_error:
        code = http_error.getcode()
        logger.error('Download failed with status code: ' + str(code))
        body = http_error.read().decode()
        logger.error('Failed to download URL:' + body)

        if code in (401, 403):
            _handle_possible_eula_error(http_error, body)
        else:
            raise


def download(url, destination_dir, logger=LOGGER, access_token=None, data=None):
    """
    Downloads the given URL to the given destination directory, using the basename of the URL
    as the filename in the destination directory.  Supports http://, https:// and s3:// schemes.
    When using the s3:// scheme, will run against us-west-2 unless the "AWS_DEFAULT_REGION"
    environment variable is set.

    When using http:// or https:// schemes, the access_token will be used for authentication
    if it is provided. If authentication with the access_token fails, or if the
    access_token is not provided, the following environment variables will be used to
    authenticate when downloading the data:

        EDL_CLIENT_ID:    The EDL application client id used to acquire an EDL shared access token
        EDL_USERNAME:     The EDL application username used to acquire an EDL shared access token
        EDL_PASSWORD:     The EDL application password used to acquire an EDL shared access token
        EDL_REDIRECT_URI: A valid redirect URI for the EDL application (NOTE: the redirect URI is
                          not followed or used; it does need to be in the app's redirect URI list)

    If these are not provided, unauthenticated requests will be made to download the data,
    with a warning message in the logs.

    Parameters
    ----------
    url : string
        The URL to fetch
    destination_dir : string
        The directory in which to place the downloaded file
    logger : Logger
        A logger to which the function will write, if provided
    access_token :
        The Earthdata Login token of the caller to use for downloads
    data : dict or Tuple[str, str]
        Optional parameter for additional data to
        send to the server when making a HTTP POST request through
        urllib.get.urlopen. These data will be URL encoded to a query string
        containing a series of `key=value` pairs, separated by ampersands. If
        None (the default), urllib.get.urlopen will use the  GET
        method.

    Returns
    -------
    destination : string
      The filename, including directory, of the downloaded file
    """
    basename = hashlib.sha256(url.encode('utf-8')).hexdigest()
    ext = path.basename(url).split('?')[0].split('.')[-1]
    filename = basename + '.' + ext
    destination = path.join(destination_dir, filename)
    # Don't overwrite, as this can be called many times for a granule
    if path.exists(destination):
        return destination

    url = harmony.aws.optimized_url(url)

    if url.startswith('s3'):
        return harmony.aws.download_from_s3(url, destination)

    return _download_from_http(url, access_token, destination, data, logger)
