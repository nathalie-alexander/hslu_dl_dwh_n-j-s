import boto3
import utils


def lambda_handler(event, context):
    destination_bucket = 'hsludwlbucket2'

    # first account
    aws_access_key_id = "ASIAUCWHK7V3YQYFFQCA"
    aws_secret_access_key = ""
    aws_session_token = ""
    source_bucket = 'hsludwlbucket'

    # second account
    aws_access_key_id2 = "ASIARYDIXPCPUJK7HF7T"
    aws_secret_access_key2 = ""
    aws_session_token2 = ""
    source_bucket2 = 'thisismytestbucket111'

    s3_client_source = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key,
                                    aws_session_token=aws_session_token)

    s3_client_source2 = boto3.client("s3", aws_access_key_id=aws_access_key_id2,
                                     aws_secret_access_key=aws_secret_access_key2,
                                     aws_session_token=aws_session_token2)

    utils.dump_s3_bucket_to_local(s3_client_source2, source_bucket2, "../data/weather_dump/")

    # # S3 client for the destination bucket
    # s3_client_destination = boto3.client("s3")
    #
    # ## --- first source bucket ---
    # response = s3_client_source.list_objects_v2(Bucket=source_bucket)
    #
    # # Copy each file from the source bucket to the destination bucket
    # for content in response.get('Contents', []):
    #     key = content['Key']
    #     print(f"Copying {key} from {source_bucket} to {destination_bucket}...")
    #
    #     # Download the object to memory
    #     obj = s3_client_source.get_object(Bucket=source_bucket, Key=key)
    #     data = obj['Body'].read()
    #
    #     # Upload the object to the destination bucket
    #     s3_client_destination.put_object(Bucket=destination_bucket, Key=key, Body=data)
    #     print(f"{key} copied successfully to {destination_bucket}.")
    #
    # ## --- second source bucket ---
    # response2 = s3_client_source2.list_objects_v2(Bucket=source_bucket2)
    #
    # # Copy each file from the source bucket to the destination bucket
    # for content in response2.get('Contents', []):
    #     key = content['Key']
    #     print(f"Copying {key} from {source_bucket2} to {destination_bucket}...")
    #
    #     # Download the object to memory
    #     obj = s3_client_source2.get_object(Bucket=source_bucket2, Key=key)
    #     data = obj['Body'].read()
    #
    #     # Upload the object to the destination bucket
    #     s3_client_destination.put_object(Bucket=destination_bucket, Key=key, Body=data)
    #     print(f"{key} copied successfully to {destination_bucket}.")
    #
    # return {"status": "All files copied successfully"}


if __name__ == '__main__':
    lambda_handler(None, None)