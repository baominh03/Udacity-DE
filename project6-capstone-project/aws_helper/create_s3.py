import pandas as pd
import boto3
import configparser
from botocore.exceptions import ClientError
from time import time

config_file_path = 'dwh_p.cfg'

config = configparser.ConfigParser()
config.read_file(open(config_file_path))


# read data from dwh.cfg file
KEY = config.get("AWS", "key")
SECRET = config.get("AWS", "secret")

S3_BUCKET_OUTPUT = config.get("S3", "S3_BUCKET_OUTPUT")
S3_REGION = config.get("S3", "S3_REGION")


def create_s3_bucket(s3_bucket_name, s3_region):
    """
    Create S3 bucket 
    OUTPUTS:
    * s3
    """
    try:
        t0 = time()
        s3 = boto3.client('s3',
                          region_name=s3_region,
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET)
        location = {'LocationConstraint': s3_region}
        s3.create_bucket(Bucket=s3_bucket_name,
                         CreateBucketConfiguration=location)

        print('CREATED S3 BUCKET [' + s3_bucket_name + '] in: {0:.2f} sec(s)\n'.format(time()-t0))
    except ClientError as e:
        print.error('S3 creation failed: '+ e)
        exit()
    return s3


if __name__ == "__main__":
    create_s3_bucket(S3_BUCKET_OUTPUT, S3_REGION)
