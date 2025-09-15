# src/train_svc/app.py
import os, time, threading, tempfile
from fastapi import FastAPI
from prometheus_client import Counter, Histogram, generate_latest
from fastapi.responses import PlainTextResponse
from src.common import config, storage
from src.common.queue import dequeue_blocking, set_status, heartbeat

app = FastAPI(title=f"TRAIN SVC ({config.NAMESPACE})", version=config.BUILD_SHA)
DONE = Counter("train_jobs_completed_total", "Completed training jobs")
FAIL = Counter("train_jobs_failed_total", "Failed training jobs")
DUR  = Histogram("train_job_duration_seconds", "Training duration")
BATCH = os.getenv("TRAIN_BATCH_SIZE", "32")

@app.get("/health")
def health(): return {"ok": True, "service": "train", "ns": config.NAMESPACE}

@app.get("/live")
def live(): return {"ok": True}

@app.get("/metrics", response_class=PlainTextResponse)
def metrics(): return generate_latest()

def _preprocess_and_train(payload: dict) -> str:
    raw_bucket = payload["bucket"]; raw_key = payload["object_key"]
    out_bucket = payload["out_bucket"]
    with tempfile.TemporaryDirectory() as td:
        raw_path = os.path.join(td, "raw.parquet")
        storage.download_to_file(raw_bucket, raw_key, raw_path)
        # (stub) preprocess → produce features
        features_key = raw_key.replace("/"+raw_key.split("/")[-1], "/features-"+raw_key.split("/")[-1])
        time.sleep(1.0)

        size = payload.get("params", {}).get("model_size", "small")
        t_map = {"small": 2, "medium": 5, "large": 12}
        time.sleep(t_map.get(size, 2))

        model_key = f"{payload['dataset']}/v{int(time.time())}/model.bin"
        model_uri = storage.url(config.BUCKET_MODELS, model_key)
        return model_uri

def worker_loop():
    name = f"trainer-{config.NAMESPACE}"
    while True:
        msg = dequeue_blocking(config.TOPIC_PRE, timeout=5)
        heartbeat(name)
        if not msg:
            continue
        job_id = msg.get("job_id", "unknown")
        payload = msg.get("payload", {})
        print(f"[train] received job {job_id} for object {payload.get('object_key')}")
        start = time.time()
        try:
            set_status(job_id, "running")
            model_uri = _preprocess_and_train(payload)
            set_status(job_id, "succeeded", result_uri=model_uri)
            DONE.inc()
        except Exception as e:
            set_status(job_id, "failed", error=str(e))
            FAIL.inc()
        finally:
            DUR.observe(time.time() - start)

t = threading.Thread(target=worker_loop, daemon=True)
t.start()
