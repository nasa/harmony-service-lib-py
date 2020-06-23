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
    USE_LOCALSTACK: 'true' if the S3 client should connect to a LocalStack instance instead of Amazon S3 (for testing)
"""

import sys
import boto3
import hashlib
import logging
from datetime import datetime
from pythonjsonlogger import jsonlogger
from http.cookiejar import CookieJar
from pathlib import Path
from urllib import request
from os import environ, path

class CanceledException(Exception):
    """Class for throwing an exception indicating a Harmony request has been canceled"""
    pass

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
        super(HarmonyJsonFormatter, self).add_fields(log_record, record, message_dict)
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
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s.%(funcName)s:%(lineno)d] [%(user)s] %(message)s")
    else:
        formatter = HarmonyJsonFormatter()
    syslog.setFormatter(formatter)
    logger.addHandler(syslog)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger

default_logger=build_logger()

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

def _setup_networking(logger=default_logger):
    """
    Sets up HTTP(S) cookies and basic auth so that HTTP calls using urllib.request will
    use Earthdata Login (EDL) auth as appropriate.  Will allow Earthdata login auth only if
    the following environment variables are set and will print a warning if they are not:

    EDL_USERNAME: The username to be passed to Earthdata Login when challenged
    EDL_PASSWORD: The password to be passed to Earthdata Login when challenged

    Returns
    -------
    None
    """
    try:
        manager = request.HTTPPasswordMgrWithDefaultRealm()
        edl_endpoints = ['https://sit.urs.earthdata.nasa.gov', 'https://uat.urs.earthdata.nasa.gov', 'https://urs.earthdata.nasa.gov']
        for endpoint in edl_endpoints:
            manager.add_password(None, endpoint, get_env('EDL_USERNAME'), get_env('EDL_PASSWORD'))
        auth = request.HTTPBasicAuthHandler(manager)

        jar = CookieJar()
        processor = request.HTTPCookieProcessor(jar)
        opener = request.build_opener(auth, processor)
        request.install_opener(opener)
    except KeyError:
        logger.warn('Earthdata Login environment variables EDL_USERNAME and EDL_PASSWORD must be set up for authenticated downloads.  Requests will be unauthenticated.')

def download(url, destination_dir, logger=default_logger):
    """
    Downloads the given URL to the given destination directory, using the basename of the URL
    as the filename in the destination directory.  Supports http://, https:// and s3:// schemes.
    When using the s3:// scheme, will run against us-west-2 unless the "AWS_DEFAULT_REGION"
    environment variable is set. When using http:// or https:// schemes, expects the following
    environment variables or will print a warning:

    EDL_USERNAME: The username to be passed to Earthdata Login when challenged
    EDL_PASSWORD: The password to be passed to Earthdata Login when challenged

    Note: The EDL environment variables are likely to be replaced by a token passed by the message
        in the future

    Parameters
    ----------
    url : string
        The URL to fetch
    destination_dir : string
        The directory in which to place the downloaded file

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

    def download_from_http(url, destination):
        _setup_networking()
        # Open the url
        f = request.urlopen(url)
        logger.info('Downloading %s', url)

        with open(destination, 'wb') as local_file:
            local_file.write(f.read())

        logger.info('Completed %s', url)
        return destination

    basename = hashlib.sha256(url.encode('utf-8')).hexdigest()
    ext = path.basename(url).split('?')[0].split('.')[-1]

    filename = basename + '.' + ext
    destination = path.join(destination_dir, filename)

    url = url.replace('//localhost', _localstack_host())

    # Allow faster local testing by referencing files directly
    url = url.replace('file://', '')
    if not url.startswith('http') and not url.startswith('s3'):
        return url

    # Don't overwrite, as this can be called many times for a granule
    if path.exists(destination):
        return destination

    if url.startswith('s3'):
        return download_from_s3(url, destination)

    return download_from_http(url, destination)


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
        logger.warn("ENV=" + get_env('ENV') + " and not using localstack, so we will not stage " + local_filename + " to " + key)
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
        response = sqs.receive_message(**receive_params)
        messages = response.get('Messages') or []

        if 'HEALTH_CHECK_PATH' in environ:
            # touch the health.txt file to update its timestamp
            Path(environ.get('HEALTH_CHECK_PATH')).touch()

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
