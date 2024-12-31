import os
import boto3

def copy_files_between_buckets(source_client, source_bucket, destination_client, destination_bucket):
    """
    Copies all files from a source S3 bucket to a destination S3 bucket.

    Args:
        source_client (boto3.client): S3 client for the source bucket.
        source_bucket (str): Name of the source bucket.
        destination_client (boto3.client): S3 client for the destination bucket.
        destination_bucket (str): Name of the destination bucket.

    Returns:
        None
    """
    try:
        source_response = source_client.list_objects_v2(Bucket=source_bucket)
        for object_metadata in source_response.get('Contents', []):
            object_key = object_metadata['Key']
            print(f"Copying {object_key} from {source_bucket} to {destination_bucket}...")

            # Download the object to memory
            obj = source_client.get_object(Bucket=source_bucket, Key=object_key)
            data = obj['Body'].read()

            # Upload the object to the destination bucket
            destination_client.put_object(Bucket=destination_bucket, Key=object_key, Body=data)
            print(f"{object_key} copied successfully to {destination_bucket}.")
    except Exception as e:
        print(f"Error copying files from {source_bucket} to {destination_bucket}: {str(e)}")

def lambda_handler(event, context):
    """
    Lambda function to copy files from two source S3 buckets to a single destination bucket.

    Returns:
        dict: A status message indicating the operation result.
    """
    destination_bucket = 'hsludwlbucket2'

    # First account credentials
    source_client1 = boto3.client(
        "s3",
        aws_access_key_id=os.environ['aws_access_key_id'],
        aws_secret_access_key=os.environ['aws_secret_access_key'],
        aws_session_token=os.environ['aws_session_token']
    )
    source_bucket1 = 'hsludwlbucket'

    # Second account credentials
    source_client2 = boto3.client(
        "s3",
        aws_access_key_id=os.environ['aws_access_key_id2'],
        aws_secret_access_key=os.environ['aws_secret_access_key2'],
        aws_session_token=os.environ['aws_session_token2']
    )
    source_bucket2 = 'thisismytestbucket111'

    # Destination S3 client
    destination_client = boto3.client("s3")

    # Copy files from the first source bucket
    copy_files_between_buckets(source_client1, source_bucket1, destination_client, destination_bucket)

    # Copy files from the second source bucket
    copy_files_between_buckets(source_client2, source_bucket2, destination_client, destination_bucket)

    return {"status": "All files copied successfully"}
