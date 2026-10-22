import os
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


class S3UploadError(Exception):
    """Custom exception for S3 upload errors."""
    pass


def get_s3_client():
    """
    Create and return an S3 client using environment variables or AWS credentials profile.

    Returns:
        boto3 S3 client

    Raises:
        S3UploadError: If credentials are not available
    """
    try:
        # Boto3 will automatically check:
        # 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        # 2. Shared credential file (~/.aws/credentials)
        # 3. AWS config file (~/.aws/config)
        # 4. IAM role (if running on EC2)
        client = boto3.client('s3')
        # Verify credentials work by listing buckets
        client.list_buckets()
        return client
    except NoCredentialsError:
        raise S3UploadError(
            "AWS credentials not found. Please configure credentials using: "
            "1. Environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, "
            "2. AWS CLI with 'aws configure', or "
            "3. ~/.aws/credentials file"
        )
    except (BotoCoreError, ClientError) as e:
        raise S3UploadError(f"Failed to create S3 client: {e}")


def upload_file_to_s3(
    file_path: str,
    bucket_name: str,
    object_key: str,
    s3_client: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Upload a single file to S3.

    Args:
        file_path: Local path to the file
        bucket_name: S3 bucket name
        object_key: S3 object key (path within bucket)
        s3_client: Optional S3 client (will create if not provided)

    Returns:
        Dictionary with upload status and metadata

    Raises:
        S3UploadError: If upload fails
    """
    path = Path(file_path)

    if not path.exists():
        raise S3UploadError(f"File not found: {file_path}")

    if not path.is_file():
        raise S3UploadError(f"Path is not a file: {file_path}")

    try:
        if s3_client is None:
            s3_client = get_s3_client()

        logger.info(f"Uploading {file_path} to s3://{bucket_name}/{object_key}")

        s3_client.upload_file(
            file_path,
            bucket_name,
            object_key
        )

        # Get object metadata to verify upload
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)

        logger.info(f"Upload successful. Size: {response['ContentLength']} bytes")

        return {
            'status': 'success',
            'bucket': bucket_name,
            'key': object_key,
            'size': response['ContentLength'],
            'etag': response['ETag']
        }

    except (BotoCoreError, ClientError) as e:
        logger.error(f"S3 upload failed: {e}")
        raise S3UploadError(f"Failed to upload {file_path} to S3: {e}")


def upload_directory_to_s3(
    local_dir: str,
    bucket_name: str,
    s3_prefix: str = '',
    s3_client: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Upload all files from a directory to S3.

    Args:
        local_dir: Local directory path
        bucket_name: S3 bucket name
        s3_prefix: Prefix for S3 object keys
        s3_client: Optional S3 client

    Returns:
        Dictionary with upload summary

    Raises:
        S3UploadError: If upload fails
    """
    local_path = Path(local_dir)

    if not local_path.exists():
        raise S3UploadError(f"Directory not found: {local_dir}")

    if not local_path.is_dir():
        raise S3UploadError(f"Path is not a directory: {local_dir}")

    try:
        if s3_client is None:
            s3_client = get_s3_client()

        results = {
            'total_files': 0,
            'successful': 0,
            'failed': 0,
            'errors': []
        }

        # Get all files in directory
        files = [f for f in local_path.rglob('*') if f.is_file()]

        logger.info(f"Found {len(files)} files to upload from {local_dir}")

        for file_path in files:
            # Calculate relative path for S3 key
            relative_path = file_path.relative_to(local_path)
            object_key = f"{s3_prefix}/{relative_path}" if s3_prefix else str(relative_path)

            try:
                upload_file_to_s3(
                    str(file_path),
                    bucket_name,
                    object_key,
                    s3_client
                )
                results['successful'] += 1
            except S3UploadError as e:
                results['failed'] += 1
                results['errors'].append({
                    'file': str(file_path),
                    'error': str(e)
                })
                logger.error(f"Failed to upload {file_path}: {e}")

        results['total_files'] = len(files)

        logger.info(f"Upload summary: {results['successful']}/{results['total_files']} successful")

        return results

    except S3UploadError:
        raise
    except Exception as e:
        logger.error(f"Unexpected error during directory upload: {e}")
        raise S3UploadError(f"Directory upload failed: {e}")


def verify_s3_upload(
    bucket_name: str,
    object_key: str,
    s3_client: Optional[Any] = None
) -> bool:
    """
    Verify that a file exists in S3.

    Args:
        bucket_name: S3 bucket name
        object_key: S3 object key
        s3_client: Optional S3 client

    Returns:
        True if object exists, False otherwise
    """
    try:
        if s3_client is None:
            s3_client = get_s3_client()

        s3_client.head_object(Bucket=bucket_name, Key=object_key)
        logger.info(f"Verification successful: s3://{bucket_name}/{object_key}")
        return True

    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.warning(f"Object not found: s3://{bucket_name}/{object_key}")
            return False
        else:
            logger.error(f"Verification failed: {e}")
            return False
    except Exception as e:
        logger.error(f"Unexpected verification error: {e}")
        return False


def upload_pipeline_artifacts(
    raw_file: str,
    clean_file: str,
    rejected_file: str,
    bucket_name: str,
    s3_prefix: str = 'etl-sales-pipeline',
    s3_client: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Upload all pipeline artifacts to S3.

    Args:
        raw_file: Path to raw data file
        clean_file: Path to clean data file
        rejected_file: Path to rejected data file
        bucket_name: S3 bucket name
        s3_prefix: Prefix for S3 keys
        s3_client: Optional S3 client

    Returns:
        Dictionary with upload results for all artifacts
    """
    logger.info("Starting S3 upload of pipeline artifacts")

    results = {
        'raw': None,
        'clean': None,
        'rejected': None,
        'overall_status': 'pending'
    }

    try:
        if s3_client is None:
            s3_client = get_s3_client()

        # Upload raw data
        try:
            results['raw'] = upload_file_to_s3(
                raw_file,
                bucket_name,
                f"{s3_prefix}/raw/{Path(raw_file).name}",
                s3_client
            )
        except S3UploadError as e:
            logger.error(f"Failed to upload raw data: {e}")
            results['raw'] = {'status': 'failed', 'error': str(e)}

        # Upload clean data
        try:
            results['clean'] = upload_file_to_s3(
                clean_file,
                bucket_name,
                f"{s3_prefix}/processed/{Path(clean_file).name}",
                s3_client
            )
        except S3UploadError as e:
            logger.error(f"Failed to upload clean data: {e}")
            results['clean'] = {'status': 'failed', 'error': str(e)}

        # Upload rejected data
        try:
            results['rejected'] = upload_file_to_s3(
                rejected_file,
                bucket_name,
                f"{s3_prefix}/rejected/{Path(rejected_file).name}",
                s3_client
            )
        except S3UploadError as e:
            logger.error(f"Failed to upload rejected data: {e}")
            results['rejected'] = {'status': 'failed', 'error': str(e)}

        # Determine overall status
        all_success = all(
            r.get('status') == 'success' for r in [results['raw'], results['clean'], results['rejected']]
            if r is not None
        )
        results['overall_status'] = 'success' if all_success else 'partial_failure'

        return results

    except S3UploadError as e:
        logger.error(f"S3 upload failed: {e}")
        results['overall_status'] = 'failed'
        results['error'] = str(e)
        return results


def get_required_permissions() -> list:
    """
    Return the minimum required AWS IAM permissions for S3 operations.

    Returns:
        List of required IAM permissions
    """
    return [
        's3:PutObject',
        's3:GetObject',
        's3:ListBucket',
        's3:HeadObject'
    ]


def get_iam_policy_template(bucket_name: str) -> str:
    """
    Return an IAM policy template for the required S3 permissions.

    Args:
        bucket_name: Name of the S3 bucket

    Returns:
        JSON-formatted IAM policy
    """
    return f"""{{
    "Version": "2012-10-17",
    "Statement": [
        {{
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:HeadObject"
            ],
            "Resource": "arn:aws:s3:::{bucket_name}/*"
        }},
        {{
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": "arn:aws:s3:::{bucket_name}"
        }}
    ]
}}"""
