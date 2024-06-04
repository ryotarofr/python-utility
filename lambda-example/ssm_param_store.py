import boto3
import traceback

REGION = 'ap-northeast-1'


def get_paramater(key, WithDecryption=False):
    try:
        ssm = boto3.client('ssm', region_name=REGION)
        response = ssm.get_parameter(Name=key, WithDecryption=WithDecryption)
        return response['Parameter']['Value']
    except Exception as e:
        traceback.print_exc()
