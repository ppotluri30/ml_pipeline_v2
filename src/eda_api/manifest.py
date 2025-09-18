import json
from .s3_utils import open_uri

def load_manifest(manifest_uri: str) -> dict:
    with open_uri(manifest_uri, "rb") as f:
        return json.load(f)
