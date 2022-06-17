from urllib.parse import urlparse
import boto3
from pystac import STAC_IO
from harmony import util
from harmony import aws

"""
Read and write to s3 when STAC links start with s3://.
https://pystac.readthedocs.io/en/0.5/concepts.html#using-stac-io
"""

config = util.config()
service_params = aws.aws_parameters(config.use_localstack, config.localstack_host, config.aws_default_region)


def read(uri):
    parsed = urlparse(uri)
    if parsed.scheme == 's3':
        bucket = parsed.netloc
        key = parsed.path[1:]
        s3 = boto3.resource('s3', **service_params)
        obj = s3.Object(bucket, key)
        return obj.get()['Body'].read().decode('utf-8')
    else:
        return STAC_IO.default_read_text_method(uri)


def write(uri, txt):
    parsed = urlparse(uri)
    if parsed.scheme == 's3':
        bucket = parsed.netloc
        key = parsed.path[1:]
        s3 = boto3.resource("s3", **service_params)
        s3.Object(bucket, key).put(Body=txt)
    else:
        STAC_IO.default_write_text_method(uri, txt)
