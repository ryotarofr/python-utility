import boto3
from botocore.exceptions import ClientError

REGION = "ap-northeast-1"
CLIENT = boto3.client('s3', region_name=REGION)


def get_object_from_s3(object_name, bucket_name):
    try:
        response = CLIENT.get_object(Bucket=bucket_name, Key=object_name)
        return True, response
    except ClientError as e:
        return False, e


def upload_to_s3(buffer, bucket_name, key_name):
    try:
        response = CLIENT.put_object(
            Body=buffer, Bucket=bucket_name, Key=key_name
        )
        return True, response
    except ClientError as e:
        return False, e
