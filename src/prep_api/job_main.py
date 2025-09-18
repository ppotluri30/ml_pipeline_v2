# src/prep_api/job_main.py
import os, json, sys
from src.prep_api.prep_dataset import process_input

def main():
    # Where your raw files live (local or MinIO/S3)
    input_uri   = os.getenv("INPUT_URI",       "s3://ml/dataset/")  # or "dataset/"
    domain      = os.getenv("DOMAIN",          "traffic")
    output_base = os.getenv("OUTPUT_BASE_URI", "s3://ml")

    manifests = process_input(input_uri, domain=domain, output_base=output_base)
    print(json.dumps({"count": len(manifests), "manifests": manifests}, indent=2))

if __name__ == "__main__":
    sys.exit(main() or 0)
