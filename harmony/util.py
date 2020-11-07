"""
=======
util.py
=======

Utility functions for logging, staging data results for external
access (S3 pre-signed URL), decrypting data using a shared secret, and
operating on message queues.

NOTE: This module's `download` function is now an alias for the
harmony.io module's download function. The function in this module is
deprecated and may be removed in future releases of the Harmony
Service Library. Please use the `harmony.io.download` function
instead.

This module relies (overly?) heavily on environment variables to know
which endpoints to use and how to authenticate to them as follows:

Required when reading from or staging to S3:
    AWS_DEFAULT_REGION: The AWS region in which the S3 client is operating (default: "us-west-2")

Required when staging to S3 and not using the Harmony-provided stagingLocation prefix:
    STAGING_BUCKET: The bucket where staged files should be placed
    STAGING_PATH: The base path under which staged files should be placed

Required when using HTTPS, allowing Earthdata Login auth.  Prints a warning if not supplied:
    EDL_CLIENT_ID:    The EDL application client id used to acquire an EDL shared access token
    EDL_USERNAME:     The EDL application username used to acquire an EDL shared access token
    EDL_PASSWORD:     The EDL application password used to acquire an EDL shared access token
    EDL_REDIRECT_URI: A valid redirect URI for the EDL application (NOTE: the redirect URI is
                      not followed or used; it does need to be in the app's redirect URI list)

Optional when reading from or staging to S3:
    USE_LOCALSTACK: 'true' if the S3 client should connect to a LocalStack instance instead of
                    Amazon S3 (for testing)

"""

from base64 import b64decode
from datetime import datetime
import logging
from pathlib import Path
from os import environ
import sys

from pythonjsonlogger import jsonlogger
from nacl.secret import SecretBox


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


LOGGER = build_logger()


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
    sys.stdout = StreamToLogger(LOGGER, logging.INFO)
    sys.stderr = StreamToLogger(LOGGER, logging.ERROR)


def download(url, destination_dir, logger=LOGGER, access_token=None, data=None):
    """DEPRECATED: Alias for the new harmony.io module's download function."""
    import harmony.io
    return harmony.io.download(url, destination_dir, logger, access_token, data)


def stage(local_filename, remote_filename, mime, logger=LOGGER, location=None):
    """DEPRECATED: Alias for the new harmony.aws module's stage function."""
    import harmony.aws
    return harmony.aws.stage(local_filename, remote_filename, mime, logger, location)


def receive_messages(queue_url, visibility_timeout_s=600, logger=LOGGER):
    """DEPRECATED: Alias for the new harmony.aws module's receive_messages function."""
    import harmony.aws
    return harmony.aws.receive_messages(queue_url, visibility_timeout_s, logger)


def delete_message(queue_url, receipt_handle):
    """DEPRECATED: Alias for the new harmony.aws module's delete_messages function."""
    import harmony.aws
    return harmony.aws.delete_message(queue_url, receipt_handle)


def change_message_visibility(queue_url, receipt_handle, visibility_timeout_s):
    """DEPRECATED: Alias for the new harmony.aws module's delete_messages function."""
    import harmony.aws
    return harmony.aws.change_message_visibility(queue_url, receipt_handle, visibility_timeout_s)


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
