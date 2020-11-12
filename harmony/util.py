"""
Utility functions for logging, staging data results for external
access (S3 pre-signed URL), decrypting data using a shared secret, and
operating on message queues.

This module relies heavily on environment variables to know
which endpoints to use and how to authenticate to them as follows:

Required when receiving an encrypted user access token in the message (Always!):
    SHARED_SECRET_KEY:  The 32-byte shared encryption / decryption key to decrypt
                        the access token in the Harmony operation.

Required when reading from or staging to S3:
    AWS_DEFAULT_REGION: The AWS region in which the S3 client is operating (default: "us-west-2")

Required when staging to S3 and not using the Harmony-provided stagingLocation prefix:
    STAGING_BUCKET: The bucket where staged files should be placed
    STAGING_PATH: The base path under which staged files should be placed

Required when using HTTPS, allowing Earthdata Login auth:
    OAUTH_HOST:     The Earthdata Login (EDL) environment to connect to
    OAUTH_CLIENT_ID:    The EDL application client id used to acquire an EDL shared access token
    OAUTH_UID:          The EDL application UID used to acquire an EDL shared access token
    OAUTH_PASSWORD:     The EDL application password used to acquire an EDL shared access token
    OAUTH_REDIRECT_URI: A valid redirect URI for the EDL application (NOTE: the redirect URI is
                        not followed or used; it does need to be in the app's redirect URI list)

Optional, if support is needed for downloading data from an endpoint that is not
EDL-share-token aware:

    FALLBACK_AUTHN_ENABLED: Whether to try downloading with the EDL_* credentials.
    EDL_USERNAME:           An valid EDL user entity username.
    EDL_PASSWORD:           The password belonging to EDL_USERNAME.

Optional when reading from or staging to S3:
    USE_LOCALSTACK:  'true' if the S3 client should connect to a LocalStack instance instead of
                     Amazon S3 (for testing)
    LOCALSTACK_HOST: The hostname of the Localstack to connect to if `USE_LOCALSTACK`.
    BACKEND_HOST:    The hostname of the Harmony backend. Deprecated / unused by this package.

Optional:
    APP_NAME:          A name for the service that will appear in log entries.
    ENV:               The application environment. One of: dev, test. Used for local development.
    TEXT_LOGGER:       Whether to log in plaintext or JSON. Default: True (plaintext).
    HEALTH_CHECK_PATH: The filesystem path that should be `touch`ed to indicate the service is
                       alive.
"""

from base64 import b64decode
from collections import namedtuple
from datetime import datetime
from functools import lru_cache
import logging
from pathlib import Path
from os import environ
import sys
from urllib import parse

from pythonjsonlogger import jsonlogger
from nacl.secret import SecretBox

from harmony import aws
from harmony import io


DEFAULT_SHARED_SECRET_KEY = '_THIS_IS_MY_32_CHARS_SECRET_KEY_'


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


Config = namedtuple(
    'Config', [
        'app_name',
        'oauth_host',
        'oauth_client_id',
        'oauth_uid',
        'oauth_password',
        'oauth_redirect_uri',
        'fallback_authn_enabled',
        'edl_username',
        'edl_password',
        'use_localstack',
        'backend_host',
        'localstack_host',
        'aws_default_region',
        'staging_path',
        'staging_bucket',
        'env',
        'text_logger',
        'health_check_path',
        'shared_secret_key',
    ])


def _validated_config(config):
    """Validates that the given Config has values for all required
    variables and returns it if so. Raises an Exception if invalid.
    """
    required = [
        'shared_secret_key',
        'oauth_client_id',
        'oauth_uid',
        'oauth_password',
        'oauth_redirect_uri',
        'staging_path',
        'staging_bucket'
    ]

    unset = [var.upper() for var in required if getattr(config, var) is None]

    # Conditionally required
    if config.fallback_authn_enabled and getattr(config, 'edl_username') is None:
        unset.append("EDL_USERNAME")
    if config.fallback_authn_enabled and getattr(config, 'edl_password') is None:
        unset.append("EDL_PASSWORD")

    if len(unset) > 0:
        msg = f"Required environment variables are not set: {', '.join(unset)}"
        raise Exception(msg)

    # Warnings
    if config.shared_secret_key == DEFAULT_SHARED_SECRET_KEY:
        logging.warning("The shared_secret_key has its default (unsecure) value.")

    logging.info(config)

    return config


@lru_cache(maxsize=None)
def config(validate=True):
    """
    Returns the Config object with all parameters set to values that were set in the
    process' environment (as environment variables), or to their default values if not
    set.

    Parameters
    ----------
    validate : bool
        Whether to validate the config before returning it. Useful to disable when
        running unit tests.

    Returns
    -------
    harmony.util.Config
        The configuration values for this runtime environment.
    """
    def str_envvar(name: str, default: str) -> str:
        value = environ.get(name, default)
        return value.strip('\"') if value is not None else None

    def bool_envvar(name: str, default: bool) -> bool:
        value = environ.get(name)
        return str.lower(value) == 'true' if value is not None else default

    oauth_redirect_uri = str_envvar('OAUTH_REDIRECT_URI', None)
    if oauth_redirect_uri is not None:
        oauth_redirect_uri = parse.quote(oauth_redirect_uri)
    backend_host = str_envvar('BACKEND_HOST', 'localhost')
    localstack_host = str_envvar('LOCALSTACK_HOST', backend_host)

    config = Config(
        app_name=str_envvar('APP_NAME', sys.argv[0]),
        oauth_host=str_envvar('OAUTH_HOST', 'https://uat.urs.earthdata.nasa.gov'),
        oauth_client_id=str_envvar('OAUTH_CLIENT_ID', None),
        oauth_uid=str_envvar('OAUTH_UID', None),
        oauth_password=str_envvar('OAUTH_PASSWORD', None),
        oauth_redirect_uri=oauth_redirect_uri,
        fallback_authn_enabled=bool_envvar('FALLBACK_AUTHN_ENABLED', False),
        edl_username=str_envvar('EDL_USERNAME', None),
        edl_password=str_envvar('EDL_PASSWORD', None),
        use_localstack=bool_envvar('USE_LOCALSTACK', False),
        backend_host=backend_host,
        localstack_host=localstack_host,
        aws_default_region=str_envvar('AWS_DEFAULT_REGION', 'us-west-2'),
        staging_path=str_envvar('STAGING_PATH', None),
        staging_bucket=str_envvar('STAGING_BUCKET', None),
        env=str_envvar('ENV', 'dev'),
        text_logger=bool_envvar('TEXT_LOGGER', False),
        health_check_path=str_envvar('HEALTH_CHECK_PATH', '/tmp/health.txt'),
        shared_secret_key=str_envvar('SHARED_SECRET_KEY', DEFAULT_SHARED_SECRET_KEY)
    )

    if validate:
        return _validated_config(config)
    else:
        return config


class HarmonyJsonFormatter(jsonlogger.JsonFormatter):
    """A JSON log entry formatter."""
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
            log_record['application'] = self.app_name


@lru_cache(maxsize=None)
def build_logger(config, name=None):
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
    if config.text_logger:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s.%(funcName)s:%(lineno)d] %(message)s")
    else:
        formatter = HarmonyJsonFormatter()
        formatter.app_name = config.app_name
    syslog.setFormatter(formatter)
    logger.addHandler(syslog)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def setup_stdout_log_formatting(config):
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
    sys.stdout = StreamToLogger(build_logger(config), logging.INFO)
    sys.stderr = StreamToLogger(build_logger(config), logging.ERROR)


def download(url, destination_dir, logger=None, access_token=None, data=None, cfg=None):
    """
    Downloads the given URL to the given destination directory, using the basename of the URL
    as the filename in the destination directory.  Supports http://, https:// and s3:// schemes.
    When using the s3:// scheme, will run against us-west-2 unless the "AWS_DEFAULT_REGION"
    environment variable is set.

    When using http:// or https:// schemes, the access_token will be used for authentication.

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
    if cfg is None:
        cfg = config()
    if logger is None:
        logger = build_logger(cfg)

    destination_path = io.filename(destination_dir, url)
    if destination_path.exists():
        return str(destination_path)
    destination_path = str(destination_path)

    source = io.optimized_url(url, cfg.localstack_host)

    if aws.is_s3(source):
        return aws.download_from_s3(cfg, source, destination_path)

    if io.is_http(source):
        return io.download_from_http(cfg, source, destination_path, access_token,
                                     logger, data, HarmonyException, ForbiddenException)

    return source


def stage(local_filename, remote_filename, mime, logger=None, location=None, cfg=None):
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
    # The implementation of this function has been moved to the
    # harmony.aws module.
    if cfg is None:
        cfg = config()
    if logger is None:
        logger = build_logger(cfg)

    return aws.stage(cfg, local_filename, remote_filename, mime, logger, location)


def receive_messages(queue_url, visibility_timeout_s=600, logger=None, cfg=None):
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
    # The implementation of this function has been moved to the
    # harmony.aws module.
    if cfg is None:
        cfg = config()
    if logger is None:
        logger = build_logger(cfg)

    touch_health_check_file(cfg.health_check_path)
    return aws.receive_messages(cfg, queue_url, visibility_timeout_s, logger)


def delete_message(queue_url, receipt_handle, cfg=None):
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
    # The implementation of this function has been moved to the
    # harmony.aws module.
    if cfg is None:
        cfg = config()
    return aws.delete_message(cfg, queue_url, receipt_handle)


def change_message_visibility(queue_url, receipt_handle, visibility_timeout_s, cfg=None):
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
    # The implementation of this function has been moved to the
    # harmony.aws module.
    if cfg is None:
        cfg = config()
    return aws.change_message_visibility(cfg, queue_url, receipt_handle, visibility_timeout_s)


def touch_health_check_file(health_check_path):
    """
    Updates the mtime of the health check file.
    """
    Path(health_check_path).touch()


def create_decrypter(key=b'_THIS_IS_MY_32_CHARS_SECRET_KEY_'):
    """Creates a function that will decrypt cyphertext using a shared secret
    (symmetric) 32-byte key.

    The returned decrypter function has type signature: str -> str.
    """
    box = SecretBox(key)

    def decrypter(encrypted_msg_str):
        """Decrypt encrypted text using the shared secret (symmetric) key
        in the function's closure."""

        parts = encrypted_msg_str.split(':')
        nonce = b64decode(parts[0])
        ciphertext = b64decode(parts[1])

        return box.decrypt(ciphertext, nonce).decode('utf-8')

    return decrypter


def nop_decrypter(ciphertext):
    """An identity decrypter function. A NOP: it returns the ciphertext
    as-is. Its other responsibility is to have nothing to do with
    crypto-currency transactions in exactly the same way that it has
    nothing to do with quantum computing.
    """
    return ciphertext
