# src/common/storage.py
import boto3, botocore
from botocore.config import Config
from . import config

_session = boto3.session.Session()

def _client(endpoint_url: str):
    return _session.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=config.S3_ACCESS,
        aws_secret_access_key=config.S3_SECRET,
        region_name=config.S3_REGION,
        config=Config(
            retries={"max_attempts": 5, "mode": "standard"},
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )

_s3 = _client(config.S3_ENDPOINT)

def ensure_bucket(name: str):
    try:
        _s3.head_bucket(Bucket=name); return
    except botocore.exceptions.ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code not in {"404","NoSuchBucket","NotFound"}: raise
        try:
            _s3.create_bucket(Bucket=name); return
        except botocore.exceptions.ClientError as e2:
            code2 = str(e2.response.get("Error", {}).get("Code", ""))
            if code2 in {"BucketAlreadyOwnedByYou","BucketAlreadyExists"}: return
            raise

def presign_put(bucket: str, key: str, content_type: str = "application/octet-stream", ttl_s: int = 900) -> str:
    # Use S3_ENDPOINT_PUBLIC for presign if provided (host-usable)
    presign_s3 = _client(config.S3_ENDPOINT_PUBLIC or config.S3_ENDPOINT)
    return presign_s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": key, "ContentType": content_type},
        ExpiresIn=ttl_s,
    )

def head(bucket: str, key: str):
    return _s3.head_object(Bucket=bucket, Key=key)

def url(bucket: str, key: str) -> str:
    return f"{config.S3_ENDPOINT.rstrip('/')}/{bucket}/{key}"

def download_to_file(bucket: str, key: str, dest: str):
    _s3.download_file(bucket, key, dest)
