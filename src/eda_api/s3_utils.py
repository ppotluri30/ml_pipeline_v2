import os, fsspec
from urllib.parse import urlparse

def storage_options(uri: str):
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        opts = {
            "key": os.getenv("S3_ACCESS_KEY"),
            "secret": os.getenv("S3_SECRET_KEY"),
            "client_kwargs": {
                "endpoint_url": os.getenv("S3_ENDPOINT"),
                "region_name": os.getenv("S3_REGION", "us-east-1"),
            },
            "config_kwargs": {"s3": {"addressing_style": "path"}},
        }
        # allow self-signed if https on localhost is used
        if opts["client_kwargs"]["endpoint_url"] and opts["client_kwargs"]["endpoint_url"].startswith("https://"):
            opts["client_kwargs"]["verify"] = False
        return opts
    return {}

def fs_for(uri: str):
    return fsspec.filesystem("s3", **storage_options(uri)) if urlparse(uri).scheme == "s3" else fsspec.filesystem("file")

def open_uri(uri: str, mode="rb"):
    return fs_for(uri).open(uri, mode)
