import os

# Service metadata
SERVICE_NAME = os.getenv("SERVICE_NAME", "service")
NAMESPACE    = os.getenv("NAMESPACE", "dev")

# Redis (status)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Kafka (transport)
QUEUE_BACKEND   = os.getenv("QUEUE_BACKEND", "redis")  # "kafka" or "redis"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
KAFKA_GROUP     = os.getenv("KAFKA_GROUP", f"{SERVICE_NAME}-{NAMESPACE}")

# Replication prefixes (let you duplicate pipeline by env/region)
TOPIC_PREFIX  = os.getenv("TOPIC_PREFIX", "")   # e.g., "us-"
BUCKET_PREFIX = os.getenv("BUCKET_PREFIX", "")  # e.g., "us-"
def tp(name: str) -> str: return f"{TOPIC_PREFIX}{name}"
def bp(name: str) -> str: return f"{BUCKET_PREFIX}{name}"

# Topics (prep/train must match)
TOPIC_PRE   = tp(os.getenv("TOPIC_PRE", "preprocessed-data"))
TOPIC_TRAIN = tp(os.getenv("TOPIC_TRAIN", "train-requests"))
TOPIC_MODEL = tp(os.getenv("TOPIC_MODEL", "model-events"))

# MinIO / S3
S3_ENDPOINT         = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ENDPOINT_PUBLIC  = os.getenv("S3_ENDPOINT_PUBLIC", None)  # e.g., "http://localhost:9000"
S3_REGION           = os.getenv("S3_REGION", "us-east-1")
S3_ACCESS           = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET           = os.getenv("S3_SECRET_KEY", "minioadmin")
BUCKET_RAW          = bp(os.getenv("BUCKET_RAW", "raw"))
BUCKET_PRE          = bp(os.getenv("BUCKET_PRE", "pre"))
BUCKET_MODELS       = bp(os.getenv("BUCKET_MODELS", "models"))

# Training knobs
TRAIN_MAX_RUNTIME_S = int(os.getenv("TRAIN_MAX_RUNTIME_S", "600"))
TRAIN_USE_GPU       = os.getenv("TRAIN_USE_GPU", "false").lower() == "true"

# CORS
ALLOW_ORIGINS = os.getenv("ALLOW_ORIGINS", "*")

# Build/version
BUILD_SHA = os.getenv("BUILD_SHA", "dev")
