# .env.ps1  — edit as needed
$env:KUBE_NAMESPACE = "dev"

# MinIO (from your cluster secret)
$env:S3_ENDPOINT    = "http://127.0.0.1:9000"   # port-forward target (HTTP)
$env:S3_REGION      = "us-east-1"
$env:S3_ACCESS_KEY  = "minioadmin"
$env:S3_SECRET_KEY  = "minioadmin"

# Storage layout
$env:S3_BUCKET      = "mlstore"                # valid bucket (≥3 chars)
$env:INPUT_PREFIX   = "dataset/"               # where CSVs live in the bucket
$env:DOMAIN         = "traffic"

# Local source of truth for uploads
$env:LOCAL_DATASET_DIR = "dataset"             # local folder to mirror to MinIO
