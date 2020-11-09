import boto3


def is_s3(url: str) -> bool:
    return url is not None and url.lower().startswith('s3')


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


def _get_aws_client(config, service):
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
    service_params = _aws_parameters(config.use_localstack, config.localstack_host, config.aws_default_region)
    return boto3.client(service, **service_params)


def download_from_s3(config, url, destination_path):
    bucket = url.split('/')[2]
    key = '/'.join(url.split('/')[3:])
    _get_aws_client(config, 's3').download_file(bucket, key, destination_path)

    return destination_path


def stage(config, local_filename, remote_filename, mime, logger, location=None):
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
    sqs = _get_aws_client(config, 'sqs')
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def change_message_visibility(config, queue_url, receipt_handle, visibility_timeout_s):
    sqs = _get_aws_client(config, 'sqs')
    sqs.change_message_visibility(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=visibility_timeout_s)
