import boto3
import os   
from dotenv import load_dotenv
load_dotenv()

client = boto3.client('secretsmanager', region_name='eu-north-1', aws_access_key_id=os.getenv("AWS_ACCESS_KEY"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
response = client.get_secret_value(SecretId='arn:aws:secretsmanager:eu-north-1:361966322300:secret:Redshift/pricescope/admin-RFateP')
secret_string = response['SecretString']

print(f"Secret string (first 50 characters): {secret_string}")

