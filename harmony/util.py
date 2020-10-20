"""
=======
util.py
=======

Utility methods, consisting of functions for moving remote data (HTTPS and S3) to be operated
on locally and for staging data results for external access (S3 pre-signed URL).

This module relies (overly?) heavily on environment variables to know which endpoints to use
and how to authenticate to them as follows:

Required when reading from or staging to S3:
    AWS_DEFAULT_REGION: The AWS region in which the S3 client is operating (default: "us-west-2")

Required when staging to S3 and not using the Harmony-provided stagingLocation prefix:
    STAGING_BUCKET: The bucket where staged files should be placed
    STAGING_PATH: The base path under which staged files should be placed

Recommended when using HTTPS, allowing Earthdata Login auth.  Prints a warning if not supplied:
    EDL_USERNAME: The username to be passed to Earthdata Login when challenged
    EDL_PASSWORD: The password to be passed to Earthdata Login when challenged

Optional when reading from or staging to S3:
    USE_LOCALSTACK: 'true' if the S3 client should connect to a LocalStack instance instead of
                    Amazon S3 (for testing)
"""

from base64 import b64decode
from datetime import datetime
from functools import lru_cache
import hashlib
from http.cookiejar import CookieJar
import http.client
import json
import logging
from pathlib import Path
import re
from urllib.error import HTTPError
from urllib.parse import urlencode
from os import environ, path
import sys
from urllib.request import (build_opener, Request,
                            HTTPBasicAuthHandler, HTTPPasswordMgrWithDefaultRealm,
                            HTTPCookieProcessor, HTTPSHandler)

import boto3
from pythonjsonlogger import jsonlogger
from nacl.secret import SecretBox


class TEAHTTPCookieProcessor(HTTPCookieProcessor):
    """
    TEA-specific cookie processor that only adds cookies if the
    redirect location is not an AWS S3 URL.
    """
    # TODO: Add _why_ we're doing this above

    # TODO: Is there a general pattern we should use for detecting s3?
    s3_location = re.compile(r's3\..*\.amazonaws.com')

    def http_request(self, request):
        # TODO: Simplify?
        url = request.get_full_url()
        match = TEAHTTPCookieProcessor.s3_location.search(url)
        if match is None:
            request = super().http_request(request)
        else:
            request.remove_header("Authorization")

        return request

    https_request = http_request


class HarmonyException(Exception):
    """Base class for Harmony exceptions.

    Attributes
    ----------
    message : string
        Explanation of the error
    category : string
        Classification of the type of harmony error
    """

    def __init__(self, message, category):
        self.message = message
        self.category = category


class CanceledException(HarmonyException):
    """Class for throwing an exception indicating a Harmony request has been canceled"""

    def __init__(self, message=None):
        super().__init__(message, 'Canceled')


class ForbiddenException(HarmonyException):
    """Class for throwing an exception indicating download failed due to not being able to access the data"""

    def __init__(self, message=None):
        super().__init__(message, 'Forbidden')


def get_env(name):
    """
    Returns the environment variable with the given name, or None if none exists.  Removes quotes
    around values if they exist

    Parameters
    ----------
    name : string
        The name of the value to retrieve

    Returns
    -------
    value : string
        The environment value or None if none exists
    """
    value = environ.get(name)
    if value is None:
        return value
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1].replace('\\"', '"')
    return value


def _use_localstack():
    """True when when running locally; influences how URLs are structured
    and how S3 is accessed.
    """
    return get_env('USE_LOCALSTACK') == 'true'


def _backend_host():
    return get_env('BACKEND_HOST') or 'localhost'


def _localstack_host():
    return get_env('LOCALSTACK_HOST') or _backend_host()


def _region():
    return get_env('AWS_DEFAULT_REGION') or 'us-west-2'


def _aws_parameters(use_localstack, localstack_host, region):
    if use_localstack:
        return {
            'endpoint_url': f'http://{localstack_host}:4566',
            'use_ssl': False,
            'aws_access_key_id': 'ACCESS_KEY',
            'aws_secret_access_key': 'SECRET_KEY',
            'region_name': region
        }
    else:
        return {
            'region_name': region
        }


REGION = get_env('AWS_DEFAULT_REGION') or 'us-west-2'


class HarmonyJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(HarmonyJsonFormatter, self).add_fields(
            log_record, record, message_dict)
        if not log_record.get('timestamp'):
            now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            log_record['timestamp'] = now
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname
        if not log_record.get('application'):
            log_record['application'] = get_env('APP_NAME') or sys.argv[0]


def build_logger():
    """
    Builds a logger with appropriate defaults for Harmony
    Parameters
    ----------
    name : string
        The name of the logger

    Returns
    -------
    logger : Logging
        A logger for service output
    """
    logger = logging.getLogger()
    syslog = logging.StreamHandler()
    text_formatter = get_env('TEXT_LOGGER') == 'true'
    if text_formatter:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(name)s.%(funcName)s:%(lineno)d] [%(user)s] %(message)s")
    else:
        formatter = HarmonyJsonFormatter()
    syslog.setFormatter(formatter)
    logger.addHandler(syslog)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


default_logger = build_logger()


def setup_stdout_log_formatting():
    """
    Updates sys.stdout and sys.stderr to pass messages through the Harmony log formatter.
    """
    # See https://stackoverflow.com/questions/11124093/redirect-python-print-output-to-logger/11124247
    class StreamToLogger(object):
        def __init__(self, logger, log_level=logging.INFO):
            self.logger = logger
            self.log_level = log_level
            self.linebuf = ''

        def write(self, buf):
            temp_linebuf = self.linebuf + buf
            self.linebuf = ''
            for line in temp_linebuf.splitlines(True):
                if line[-1] == '\n':
                    self.logger.log(self.log_level, line.rstrip())
                else:
                    self.linebuf += line

        def flush(self):
            if self.linebuf != '':
                self.logger.log(self.log_level, self.linebuf.rstrip())
            self.linebuf = ''
    sys.stdout = StreamToLogger(default_logger, logging.INFO)
    sys.stderr = StreamToLogger(default_logger, logging.ERROR)


def _get_aws_client(service):
    """
    Returns a boto3 client for accessing the provided service.  Accesses the service in us-west-2
    unless "AWS_DEFAULT_REGION" is set.  If the environment variable "USE_LOCALSTACK" is set to "true",
    it will return a client that will access a LocalStack instance instead of AWS.

    Parameters
    ----------
    service : string
        The AWS service name for which to construct a client, e.g. "s3" or "sqs"

    Returns
    -------
    s3_client : boto3.*.Client
        A client appropriate for accessing the provided service
    """
    service_params = _aws_parameters(_use_localstack(), _localstack_host(), _region())
    return boto3.client(service, **service_params)


@lru_cache
def _create_opener():
    # TODO: docstring
    cookie_processor = TEAHTTPCookieProcessor(CookieJar())
    https_handler = HTTPSHandler()

    return build_opener(cookie_processor, https_handler)


@lru_cache
def _create_basic_auth_opener(logger=default_logger):
    """
    Creates an OpenerDirector that will use HTTP(S) cookies and basic auth to open a URL
    using Earthdata Login (EDL) auth credentials.  Will use Earthdata Login creds only if
    the following environment variables are set and will print a warning if they are not:

    EDL_USERNAME: The username to be passed to Earthdata Login when challenged
    EDL_PASSWORD: The password to be passed to Earthdata Login when challenged

    Returns
    -------
    opener : urllib.request.OpenerDirector
        An OpenerDirector that can be used to to open a URL using the EDL credentials, if
        they are available.
    """
    try:
        auth_handler = HTTPBasicAuthHandler(HTTPPasswordMgrWithDefaultRealm())
        edl_endpoints = ['https://sit.urs.earthdata.nasa.gov',
                         'https://uat.urs.earthdata.nasa.gov',
                         'https://urs.earthdata.nasa.gov']
        auth_handler.add_password(None, edl_endpoints,
                                  get_env('EDL_USERNAME'), get_env('EDL_PASSWORD'))

        cookie_processor = TEAHTTPCookieProcessor(CookieJar())

        return build_opener(auth_handler, cookie_processor)

    except KeyError:
        logger.warning('Earthdata Login environment variables EDL_USERNAME and EDL_PASSWORD must '
                       'be set up for authenticated downloads. Requests will be unauthenticated.')
        return build_opener()


def _bearer_token_auth_header(access_token):
    """Returns a dictionary representing an HTTP Authorization header.

    Conforms to RFC 6750: The OAuth 2.0 Authorization Framework: Bearer Token Usage
    See: https://tools.ietf.org/html/rfc6750

    Parameters
    ----------
    access_token : string
        The Earthdata Login token for the user making the request.

    Returns
    -------
    authorization token : dict
        A dictionary with the Authorization header key and Bearer token.
    """
    return {
        'Authorization': f'Bearer {access_token}'
    }


def download(url, destination_dir, logger=default_logger, access_token=None, data=None):
    """
    Downloads the given URL to the given destination directory, using the basename of the URL
    as the filename in the destination directory.  Supports http://, https:// and s3:// schemes.
    When using the s3:// scheme, will run against us-west-2 unless the "AWS_DEFAULT_REGION"
    environment variable is set.

    When using http:// or https:// schemes, the access_token will be used for authentication
    if it is provided. If authentication with the access_token fails, or if the access_token
    is not provided, the following environment variables will be used to authenticate
    when downloading the data:

      EDL_USERNAME: The username to be passed to Earthdata Login when challenged
      EDL_PASSWORD: The password to be passed to Earthdata Login when challenged

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

    def download_from_s3(url, destination):
        bucket = url.split('/')[2]
        key = '/'.join(url.split('/')[3:])
        _get_aws_client('s3').download_file(bucket, key, destination)
        return destination

    def download_from_http(url, destination, data=None):
        response = None

        try:
            logger.info('Downloading %s', url)

            response = None
            if data is not None:
                logger.info('Query parameters supplied, will use POST method.')
                encoded_data = urlencode(data).encode('utf-8')
            else:
                encoded_data = None

            if access_token is not None:
                headers = _bearer_token_auth_header(access_token)
                try:
                    opener = _create_opener()
                    request = Request(url, headers=headers, data=encoded_data)
                    response = opener.open(request)
                except Exception as e:
                    msg = ('Failed to download using access token due to ' + str(e) +
                           ' Trying with EDL_USERNAME and EDL_PASSWORD', str(e))
                    logger.exception(msg, exc_info=e)

            if access_token is None or response is None or response.status != 200:
                opener = _create_basic_auth_opener(logger)
                response = opener.open(url, data=encoded_data)

            with open(destination, 'wb') as local_file:
                local_file.write(response.read())

            logger.info('Completed %s', url)

            return destination
        except HTTPError as http_error:
            code = http_error.getcode()
            logger.error('Download failed with status code: ' + str(code))
            body = http_error.read().decode()

            logger.error('Failed to download URL:' + body)
            if (code == 401 or code == 403):
                try:
                    # Try to determine if this is a EULA error
                    json_object = json.loads(body)
                    eula_error = "error_description" in json_object and "resolution_url" in json_object
                    if eula_error:
                        body = 'Request could not be completed because you need to agree to the EULA at ' + \
                            json_object['resolution_url']
                except Exception:
                    pass
                raise ForbiddenException(body) from http_error
            else:
                raise

    basename = hashlib.sha256(url.encode('utf-8')).hexdigest()
    ext = path.basename(url).split('?')[0].split('.')[-1]

    filename = basename + '.' + ext
    destination = path.join(destination_dir, filename)
    # Don't overwrite, as this can be called many times for a granule
    if path.exists(destination):
        return destination

    url = url.replace('//localhost', _localstack_host())

    # Allow faster local testing by referencing files directly
    url = url.replace('file://', '')
    if not url.startswith('http') and not url.startswith('s3'):
        return url

    if url.startswith('s3'):
        return download_from_s3(url, destination)

    return download_from_http(url, destination, data)


def stage(local_filename, remote_filename, mime, logger=default_logger, location=None):
    """
    Stages the given local filename, including directory path, to an S3 location with the given
    filename and mime-type

    Requires the following environment variables:
        AWS_DEFAULT_REGION: The AWS region in which the S3 client is operating

    Parameters
    ----------
    local_filename : string
        A path and filename to the local file that should be staged
    remote_filename : string
        The basename to give to the remote file
    mime : string
        The mime type to apply to the staged file for use when it is served, e.g. "application/x-netcdf4"
    location : string
        The S3 prefix URL under which to place the output file.  If not provided, STAGING_BUCKET and
        STAGING_PATH must be set in the environment
    logger : logging
        The logger to use

    Returns
    -------
    url : string
        An s3:// URL to the staged file
    """

    if location is None:
        staging_bucket = get_env('STAGING_BUCKET')
        staging_path = get_env('STAGING_PATH')

        if staging_path:
            key = '%s/%s' % (staging_path, remote_filename)
        else:
            key = remote_filename
    else:
        _, _, staging_bucket, staging_path = location.split('/', 3)
        key = staging_path + remote_filename

    if get_env('ENV') in ['dev', 'test'] and not _use_localstack():
        logger.warn("ENV=" + get_env('ENV') +
                    " and not using localstack, so we will not stage " + local_filename + " to " + key)
        return "http://example.com/" + key

    s3 = _get_aws_client('s3')
    s3.upload_file(local_filename, staging_bucket, key,
                   ExtraArgs={'ContentType': mime})

    return 's3://%s/%s' % (staging_bucket, key)


def receive_messages(queue_url, visibility_timeout_s=600, logger=default_logger):
    """
    Generates successive messages from reading the queue.  The caller
    is responsible for deleting or returning each message to the queue

    Parameters
    ----------
    queue_url : string
        The URL of the queue to receive messages on
    visibility_timeout_s : int
        The number of seconds to wait for a received message to be deleted
        before it is returned to the queue

    Yields
    ------
    receiptHandle, body : string, string
        A tuple of the receipt handle, used to delete or update messages,
        and the contents of the message
    """
    sqs = _get_aws_client('sqs')
    logger.info('Listening on %s' % (queue_url,))
    while True:
        receive_params = dict(
            QueueUrl=queue_url,
            VisibilityTimeout=visibility_timeout_s,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1
        )
        touch_health_check_file()
        response = sqs.receive_message(**receive_params)
        messages = response.get('Messages') or []
        if len(messages) == 1:
            yield (messages[0]['ReceiptHandle'], messages[0]['Body'])
        else:
            logger.info('No messages received.  Retrying.')


def delete_message(queue_url, receipt_handle):
    """
    Deletes the message with the given receipt handle from the provided queue URL,
    indicating successful processing

    Parameters
    ----------
    queue_url : string
        The queue from which the message originated
    receipt_handle : string
        The receipt handle of the message, as yielded by `receive_messages`
    """
    sqs = _get_aws_client('sqs')
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def change_message_visibility(queue_url, receipt_handle, visibility_timeout_s):
    """
    Updates the message visibility timeout of the message with the given receipt handle

    Parameters
    ----------
    queue_url : string
        The queue from which the message originated
    receipt_handle : string
        The receipt handle of the message, as yielded by `receive_messages`
    visibility_timeout_s : int
        The number of additional seconds to wait for a received message to be deleted
        before it is returned to the queue
    """
    sqs = _get_aws_client('sqs')
    sqs.change_message_visibility(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=visibility_timeout_s)


def touch_health_check_file():
    """
    Updates the mtime of the health check file
    """
    healthCheckPath = environ.get('HEALTH_CHECK_PATH', '/tmp/health.txt')
    # touch the health.txt file to update its timestamp
    Path(healthCheckPath).touch()


def create_decrypter(key=b'_THIS_IS_MY_32_CHARS_SECRET_KEY_'):
    box = SecretBox(key)

    def decrypter(encrypted_msg_str):
        parts = encrypted_msg_str.split(':')
        nonce = b64decode(parts[0])
        ciphertext = b64decode(parts[1])

        return box.decrypt(ciphertext, nonce).decode('utf-8')

    return decrypter


def nop_decrypter(cyphertext):
    return cyphertext
