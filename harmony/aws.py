import boto3

import harmony.util


LOGGER = harmony.util.build_logger()


def _use_localstack():
    """True when when running locally; influences how URLs are structured
    and how S3 is accessed.
    """
    return harmony.util.get_env('USE_LOCALSTACK') == 'true'


def _backend_host():
    return harmony.util.get_env('BACKEND_HOST') or 'localhost'


def _localstack_host():
    return harmony.util.get_env('LOCALSTACK_HOST') or _backend_host()


def _region():
    return harmony.util.get_env('AWS_DEFAULT_REGION') or 'us-west-2'


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


def get_aws_client(service):
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


def download_from_s3(url, destination):
    bucket = url.split('/')[2]
    key = '/'.join(url.split('/')[3:])
    get_aws_client('s3').download_file(bucket, key, destination)

    return destination


def optimized_url(url):
    """Return a version of the url optimized for local development."""

    url = url.replace('//localhost', _localstack_host())

    # Allow faster local testing by referencing files directly
    url = url.replace('file://', '')
    if not url.startswith('http') and not url.startswith('s3'):
        return url

    return url


def stage(local_filename, remote_filename, mime, logger=LOGGER, location=None):
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
        staging_bucket = harmony.util.get_env('STAGING_BUCKET')
        staging_path = harmony.util.get_env('STAGING_PATH')

        if staging_path:
            key = '%s/%s' % (staging_path, remote_filename)
        else:
            key = remote_filename
    else:
        _, _, staging_bucket, staging_path = location.split('/', 3)
        key = staging_path + remote_filename

    if harmony.util.get_env('ENV') in ['dev', 'test'] and not harmony.aws.use_localstack():
        logger.warn("ENV=" + harmony.util.get_env('ENV') +
                    " and not using localstack, so we will not stage " + local_filename + " to " + key)
        return "http://example.com/" + key

    s3 = harmony.aws.get_aws_client('s3')
    s3.upload_file(local_filename, staging_bucket, key,
                   ExtraArgs={'ContentType': mime})

    return 's3://%s/%s' % (staging_bucket, key)


def receive_messages(queue_url, visibility_timeout_s=600, logger=LOGGER):
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
    sqs = harmony.aws.get_aws_client('sqs')
    logger.info('Listening on %s' % (queue_url,))
    while True:
        receive_params = dict(
            QueueUrl=queue_url,
            VisibilityTimeout=visibility_timeout_s,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1
        )
        harmony.util.touch_health_check_file()
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
    sqs = harmony.aws.get_aws_client('sqs')
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
    sqs = harmony.aws.get_aws_client('sqs')
    sqs.change_message_visibility(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=visibility_timeout_s)
