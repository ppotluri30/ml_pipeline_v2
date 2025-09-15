# src/prep_api/app.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from uuid import uuid4
from prometheus_client import Counter, Histogram, generate_latest
from fastapi.responses import PlainTextResponse
from datetime import datetime
import time

from src.common import config, storage
from src.common.queue import enqueue, get_status, set_status

app = FastAPI(title=f"PREP API ({config.NAMESPACE})", version=config.BUILD_SHA)
app.add_middleware(CORSMiddleware, allow_origins=[config.ALLOW_ORIGINS], allow_methods=["*"], allow_headers=["*"])

REQS = Counter("http_requests_total", "Total requests", ["path"])
LAT  = Histogram("http_request_latency_seconds", "Latency", ["path"])

class UploadInit(BaseModel):
    dataset: str = Field(..., description="Logical dataset name")
    content_type: str = "application/octet-stream"

class JobStatus(BaseModel):
    job_id: str
    status: str
    result_uri: str | None = None
    error: str | None = None

@app.on_event("startup")
def _startup():
    storage.ensure_bucket(config.BUCKET_RAW)
    storage.ensure_bucket(config.BUCKET_PRE)
    storage.ensure_bucket(config.BUCKET_MODELS)

@app.get("/health")
def health(): return {"ok": True, "service": "prep", "ns": config.NAMESPACE}

@app.get("/live")
def live(): return {"ok": True}

@app.get("/metrics", response_class=PlainTextResponse)
def metrics(): return generate_latest()

# ---------- Original endpoints ----------
@app.post("/upload/init")
def upload_init(req: UploadInit):
    REQS.labels("/upload/init").inc()
    today = datetime.utcnow().strftime("%Y/%m/%d")
    obj_id = str(uuid4())
    key = f"{req.dataset}/{today}/{obj_id}.parquet"
    put_url = storage.presign_put(config.BUCKET_RAW, key, content_type=req.content_type)
    return {"object_key": key, "bucket": config.BUCKET_RAW, "put_url": put_url}

class SubmitJob(BaseModel):
    dataset: str
    object_key: str
    params: dict = {}

@app.post("/jobs", response_model=JobStatus, status_code=202)
def submit_job(req: SubmitJob):
    with LAT.labels("/jobs").time():
        REQS.labels("/jobs").inc()
        job_id = f"{req.dataset}:{req.object_key}"
        msg = {
            "job_id": job_id,
            "type": "preprocess",
            "payload": {
                "dataset": req.dataset,
                "bucket": config.BUCKET_RAW,
                "object_key": req.object_key,
                "params": req.params,
                "out_bucket": config.BUCKET_PRE
            }
        }
        set_status(job_id, "queued")
        enqueue(config.TOPIC_PRE, msg)
        return JobStatus(job_id=job_id, status="queued")

@app.get("/jobs/{job_id:path}", response_model=JobStatus)
def job_status(job_id: str):
    REQS.labels("/jobs/status").inc()
    st = get_status(job_id)
    if not st: raise HTTPException(404, "unknown job")
    return JobStatus(job_id=job_id, **st)

@app.get("/job", response_model=JobStatus)
def job_status_q(job_id: str):
    st = get_status(job_id)
    if not st: raise HTTPException(404, "unknown job")
    return JobStatus(job_id=job_id, **st)

@app.get("/jobs/{job_id:path}/wait", response_model=JobStatus)
def job_wait(job_id: str, timeout: int = 60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = get_status(job_id)
        if st and st["status"] in {"succeeded","failed"}:
            return JobStatus(job_id=job_id, **st)
        time.sleep(1)
    st = get_status(job_id) or {"status": "unknown"}
    return JobStatus(job_id=job_id, **st)

# ---------- Streamlined ingest flow ----------
class IngestInit(BaseModel):
    dataset: str
    content_type: str = "application/octet-stream"

class IngestInitResponse(BaseModel):
    job_id: str
    object_key: str
    put_url: str
    poll_url: str
    upload_host_header: str = "minio:9000"

@app.post("/ingest/init", response_model=IngestInitResponse)
def ingest_init(req: IngestInit):
    today = datetime.utcnow().strftime("%Y/%m/%d")
    obj_id = str(uuid4())
    object_key = f"{req.dataset}/{today}/{obj_id}.parquet"
    put_url = storage.presign_put(config.BUCKET_RAW, object_key, content_type=req.content_type)
    job_id = f"{req.dataset}:{object_key}"
    set_status(job_id, "pending-upload")
    poll_url = f"/job?job_id={job_id}"
    return IngestInitResponse(
        job_id=job_id,
        object_key=object_key,
        put_url=put_url,
        poll_url=poll_url,
        upload_host_header="minio:9000"
    )

class IngestComplete(BaseModel):
    job_id: str
    object_key: str
    params: dict = {}

@app.post("/ingest/complete", response_model=JobStatus)
def ingest_complete(req: IngestComplete):
    # verify uploaded object exists
    try:
        storage.head(config.BUCKET_RAW, req.object_key)
    except Exception:
        raise HTTPException(400, "Object not found in raw bucket; did you upload?")
    payload = {
        "dataset": req.job_id.split(":",1)[0],
        "bucket": config.BUCKET_RAW,
        "object_key": req.object_key,
        "params": req.params,
        "out_bucket": config.BUCKET_PRE
    }
    set_status(req.job_id, "queued")
    enqueue(config.TOPIC_PRE, {"job_id": req.job_id, "type": "preprocess", "payload": payload})
    return JobStatus(job_id=req.job_id, status="queued")
