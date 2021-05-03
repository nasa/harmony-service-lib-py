"""
This module includes various AWS-specific functions to stage data in S3 and deal with
messages in SQS queues.

This module relies on the harmony.util.config and its environment variables to be
set for correct operation. See that module and the project README for details.
"""
import boto3
from botocore.config import Config


def is_s3(url: str) -> bool:
    """Predicate to determine if a url is an S3 endpoint."""
    return url is not None and url.lower().startswith('s3')


def _aws_parameters(use_localstack, localstack_host, region):
    """Constructs a configuration dict that can be used to create an aws client.

    Parameters
    ----------
    use_localstack : bool
        Whether to use the localstack in this environment.
    localstack_host : str
        The hostname of the localstack services (if use_localstack enabled).
    region : str
        The AWS region to connect to.
    Returns
    -------

    """
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


def _get_aws_client(config, service, user_agent=None):
    """
    Returns a boto3 client for accessing the provided service.  Accesses the service in us-west-2
    unless "AWS_DEFAULT_REGION" is set.  If the environment variable "USE_LOCALSTACK" is set to "true",
    it will return a client that will access a LocalStack instance instead of AWS.

    Parameters
    ----------
    config : harmony.util.Config
        The configuration for the current runtime environment.
    service : string
        The AWS service name for which to construct a client, e.g. "s3" or "sqs"
    user_agent : string
        The user agent that is requesting the aws service.
        E.g. harmony/0.0.0 (harmony-sit) harmony-service-lib/4.0 (gdal-subsetter)

    Returns
    -------
    s3_client : boto3.*.Client
        A client appropriate for accessing the provided service
    """
    boto_cfg = Config(user_agent_extra=user_agent)
    service_params = _aws_parameters(config.use_localstack, config.localstack_host, config.aws_default_region)

    return boto3.client(service_name=service, config=boto_cfg, **service_params)


def download(config, url, destination_file, user_agent=None):
    """Download an S3 object to the specified destination directory.

    Parameters
    ----------
    config : harmony.util.Config
        The configuration for the current runtime environment.
    destination_file : file-like
        The destination file where the object will be written. Must be
        a file-like object opened for binary write.
    user_agent : string
        The user agent that is requesting the download.
        E.g. harmony/0.0.0 (harmony-sit) harmony-service-lib/4.0 (gdal-subsetter)
    """
    bucket = url.split('/')[2]
    key = '/'.join(url.split('/')[3:])
    aws_client = _get_aws_client(config, 's3', user_agent)
    aws_client.download_fileobj(bucket, key, destination_file)


def stage(config, local_filename, remote_filename, mime, logger, location=None):
    """
    Stages the given local filename, including directory path, to an S3 location with the given
    filename and mime-type

    Requires the following environment variables:
        AWS_DEFAULT_REGION: The AWS region in which the S3 client is operating

    Parameters
    ----------
    config : harmony.util.Config
        The configuration for the current runtime environment.
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
    key = None
    staging_bucket = config.staging_bucket

    if location is None:
        if config.staging_path:
            key = '%s/%s' % (config.staging_path, remote_filename)
        else:
            key = remote_filename
    else:
        _, _, staging_bucket, staging_path = location.split('/', 3)
        key = staging_path + remote_filename

    if config.env in ['dev', 'test'] and not config.use_localstack:
        logger.warning(f"ENV={config.env}"
                       f" and not using localstack, so we will not stage {local_filename} to {key}")
        return "http://example.com/" + key

    s3 = _get_aws_client(config, 's3')
    s3.upload_file(local_filename, staging_bucket, key, ExtraArgs={'ContentType': mime})

    return 's3://%s/%s' % (staging_bucket, key)


def receive_messages(config, queue_url, visibility_timeout_s, logger):
    """
    Generates successive messages from reading the queue.  The caller
    is responsible for deleting or returning each message to the queue

    Parameters
    ----------
    config : harmony.util.Config
        The configuration for the current runtime environment.
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
    if visibility_timeout_s is None:
        visibility_timeout_s = 600

    sqs = _get_aws_client(config, 'sqs')
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
        if len(messages) == 1:
            yield (messages[0]['ReceiptHandle'], messages[0]['Body'])
        else:
            logger.info('No messages received.  Retrying.')


def delete_message(config, queue_url, receipt_handle):
    """
    Deletes the message with the given receipt handle from the provided queue URL,
    indicating successful processing

    Parameters
    ----------
    config : harmony.util.Config
        The configuration for the current runtime environment.
    queue_url : string
        The queue from which the message originated
    receipt_handle : string
        The receipt handle of the message, as yielded by `receive_messages`
    """
    sqs = _get_aws_client(config, 'sqs')
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def change_message_visibility(config, queue_url, receipt_handle, visibility_timeout_s):
    """
    Updates the message visibility timeout of the message with the given receipt handle

    Parameters
    ----------
    config : harmony.util.Config
        The configuration for the current runtime environment.
    queue_url : string
        The queue from which the message originated
    receipt_handle : string
        The receipt handle of the message, as yielded by `receive_messages`
    visibility_timeout_s : int
        The number of additional seconds to wait for a received message to be deleted
        before it is returned to the queue
    """
    sqs = _get_aws_client(config, 'sqs')
    sqs.change_message_visibility(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=visibility_timeout_s)
