import boto3, os
from dotenv import load_dotenv
load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

files_to_upload = [('glue_jobs/bronze_to_silver.py', 'scripts/glue_jobs/bronze_to_silver.py')]

for local_file, s3_key in files_to_upload:
    s3.upload_file(local_file, os.getenv("S3_BUCKET_NAME"), s3_key)
    print(f"Uploaded {local_file} -> s3://{os.getenv('S3_BUCKET_NAME')}/{s3_key}")


#put_object and upload_file are two different methods provided by the boto3 library to upload files to Amazon S3. Here are the key differences between them: put_object:
# - put_object is a method that allows you to upload an object (file) to S3
# - It is typically used for uploading small files or data that can be represented as a string or bytes
# - It requires you to provide the content of the file as a parameter (Body) when calling the method

#upload_file is a method that allows you to upload a local file to S3
#It is typically used for uploading larger files or when you have a local file path

#upload file and put_object are both idempotent, meaning that if you call them multiple times with the same parameters, the result will be the same (the file will be uploaded to S3). However, they have different use cases and performance characteristics. For larger files, upload_file is generally preferred as it handles multipart uploads and retries automatically.
