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

import boto3
import hashlib
import logging

from http.cookiejar import CookieJar
from urllib import request
from os import environ, path

_s3 = None

def _use_localstack():
    """True when when running locally; influences how URLs are structured
    and how S3 is accessed.
    """
    return environ.get('USE_LOCALSTACK') == 'true'


def _s3_parameters():
    region = environ.get('AWS_DEFAULT_REGION') or 'us-west-2'
    if _use_localstack():
        backend_host = environ.get('BACKEND_HOST') or 'localhost'
        return {
            'endpoint_url': f'http://{backend_host}:4572',
            'use_ssl': False,
            'aws_access_key_id': 'ACCESS_KEY',
            'aws_secret_access_key': 'SECRET_KEY',
            'region_name': region
        } 
    else:
        return {
            'region_name': region
        }

def _get_s3_client():
    """
    Returns a client for accessing S3.  Accesses S3 in us-west-2 unless "AWS_DEFAULT_REGION"
    is set.  If the environment variable "USE_LOCALSTACK" is set to "true", it will return
    a client that will access a LocalStack S3 instance instead of AWS.

    Returns
    -------
    s3_client : boto3.S3.Client
        A client appropriate for accessing S3
    """
    if _s3 != None:
        return _s3
    s3_parameters = _s3_parameters()
    return boto3.client('s3', **s3_parameters);


def _setup_networking(logger=logging):
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
            manager.add_password(None, endpoint, environ['EDL_USERNAME'], environ['EDL_PASSWORD'])
        auth = request.HTTPBasicAuthHandler(manager)

        jar = CookieJar()
        processor = request.HTTPCookieProcessor(jar)
        opener = request.build_opener(auth, processor)
        request.install_opener(opener)
    except KeyError:
        logger.warn('Earthdata Login environment variables EDL_USERNAME and EDL_PASSWORD must be set up for authenticated downloads.  Requests will be unauthenticated.')

def download(url, destination_dir, logger=logging):
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
        _get_s3_client().download_file(bucket, key, destination)
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

    url = url.replace('//localhost', '//host.docker.internal')

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


def stage(local_filename, remote_filename, mime, logger=logging, location=None):
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
        staging_bucket = environ.get('STAGING_BUCKET')
        staging_path = environ.get('STAGING_PATH')

        if staging_path:
            key = '%s/%s' % (staging_path, remote_filename)
        else:
            key = remote_filename
    else:
        _, _, staging_bucket, staging_path = location.split('/', 3)
        key = staging_path + remote_filename

    if environ.get('ENV') in ['dev', 'test'] and not _use_localstack():
        logger.warn("ENV=" + environ['ENV'] + " and not using localstack, so we will not stage " + local_filename + " to " + key)
        return "http://example.com/" + key

    s3 = _get_s3_client()
    s3.upload_file(local_filename, staging_bucket, key,
                ExtraArgs={'ContentType': mime})

    return 's3://%s/%s' % (staging_bucket, key)
