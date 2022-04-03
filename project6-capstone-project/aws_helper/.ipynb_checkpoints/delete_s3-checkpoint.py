import boto3
import configparser
from time import time
from time import sleep

config_file_path = 'dwh_p.cfg'

config = configparser.ConfigParser()
config.read_file(open(config_file_path))


# read data from dwh.cfg file
KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")

S3_BUCKET_OUTPUT = config.get("S3", "S3_BUCKET_OUTPUT")
S3_REGION = config.get("S3", "S3_REGION")


def delete_s3_bucket(s3_bucket_name, s3_region):
    """
    To delete S3 bucket
    """

    t0 = time()
    s3_resource = boto3.resource('s3',
                                 region_name=s3_region,
                                 aws_access_key_id=KEY,
                                 aws_secret_access_key=SECRET)

    s3_client = boto3.client('s3',
                             region_name=s3_region,
                             aws_access_key_id=KEY,
                             aws_secret_access_key=SECRET)

    bucket = s3_resource.Bucket(s3_bucket_name)
    bucket.objects.all().delete()
    print('=== Removed files in bucket: [' + s3_bucket_name +
          '] in: {0:.2f} sec(s)\n'.format(time()-t0))

    s3_client.delete_bucket(Bucket=s3_bucket_name)
    print('=== Deleted Bucket [' + s3_bucket_name + '] in: {0:.2f} sec(s)\n'.format(time()-t0))


if __name__ == "__main__":
    delete_s3_bucket(S3_BUCKET_OUTPUT, S3_REGION)
