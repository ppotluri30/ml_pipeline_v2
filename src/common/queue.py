# src/common/queue.py
import os
import json, time
from typing import Optional, Dict, Any
from . import config

# ----------------------------
# STATUS STORE (always Redis)
# ----------------------------
import redis
_r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=0, decode_responses=True)

def _ns(key: str) -> str: return f"{config.NAMESPACE}:{key}"
STATUS_KEY  = _ns("jobs:status")
RESULT_KEY  = _ns("jobs:result")
ERROR_KEY   = _ns("jobs:error")
HEARTBEAT_P = _ns("workers")

def set_status(job_id: str, status: str, result_uri: str | None = None, error: str | None = None):
    _r.hset(STATUS_KEY, job_id, status)
    if result_uri is not None: _r.hset(RESULT_KEY, job_id, result_uri)
    if error is not None:      _r.hset(ERROR_KEY, job_id, error)

def get_status(job_id: str) -> Optional[Dict[str, Any]]:
    if not _r.hexists(STATUS_KEY, job_id): return None
    return {
        "status":     _r.hget(STATUS_KEY, job_id),
        "result_uri": _r.hget(RESULT_KEY, job_id),
        "error":      _r.hget(ERROR_KEY, job_id),
    }

def heartbeat(worker_name: str):
    _r.set(f"{HEARTBEAT_P}:{worker_name}", int(time.time()))

# ----------------------------
# QUEUE TRANSPORT (switchable)
# ----------------------------
if config.QUEUE_BACKEND == "redis":
    def _qkey(q: str) -> str: return _ns(f"queue:{q}")

    def enqueue(topic: str, msg: Dict[str, Any]) -> None:
        _r.lpush(_qkey(topic), json.dumps(msg))
        if "job_id" in msg:
            _r.hset(STATUS_KEY, msg["job_id"], "queued")

    def dequeue_blocking(topic: str, timeout: int = 0) -> Optional[Dict[str, Any]]:
        item = _r.brpop(_qkey(topic), timeout=timeout)
        if not item: return None
        _, raw = item
        return json.loads(raw)

elif config.QUEUE_BACKEND == "kafka":
    from confluent_kafka import Producer, Consumer, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
    _producer = None
    _consumers: dict[str, Consumer] = {}

    # --- SASL/PLAINTEXT envs (read once) ---
    KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL")  # e.g., SASL_PLAINTEXT
    KAFKA_SASL_MECHANISM    = os.getenv("KAFKA_SASL_MECHANISM")     # e.g., SCRAM-SHA-256
    KAFKA_SASL_USERNAME     = os.getenv("KAFKA_SASL_USERNAME")      # e.g., user1
    KAFKA_SASL_PASSWORD     = os.getenv("KAFKA_SASL_PASSWORD")      # e.g., from secret

    def _base_kafka_conf():
        conf = {"bootstrap.servers": config.KAFKA_BOOTSTRAP}
        if KAFKA_SECURITY_PROTOCOL:
            conf["security.protocol"] = KAFKA_SECURITY_PROTOCOL
        if KAFKA_SASL_MECHANISM:
            conf["sasl.mechanism"] = KAFKA_SASL_MECHANISM
        if KAFKA_SASL_USERNAME:
            conf["sasl.username"] = KAFKA_SASL_USERNAME
        if KAFKA_SASL_PASSWORD:
            conf["sasl.password"] = KAFKA_SASL_PASSWORD
        return conf

    def _get_admin():
        # <-- USE SASL CONF HERE
        return AdminClient(_base_kafka_conf())

    def ensure_topic(topic: str, num_partitions: int = 1, replication: int = 1):
        admin = _get_admin()
        fs = admin.create_topics([NewTopic(topic, num_partitions=num_partitions, replication_factor=replication)],
                                 request_timeout=15)
        for t, f in fs.items():
            try:
                f.result()
            except Exception as e:
                msg = str(e)
                if 'TopicAlreadyExists' in msg or 'already exists' in msg:
                    continue
                print(f"[kafka-admin] create topic {t} warning: {e}")

    def _get_producer():
        # <-- USE SASL CONF HERE
        global _producer
        if _producer is None:
            _producer = Producer(_base_kafka_conf())
        return _producer

    def _get_consumer(topic: str):
        if topic not in _consumers:
            try:
                ensure_topic(topic)
            except Exception:
                pass
            conf = _base_kafka_conf()  # <-- USE SASL CONF HERE
            conf.update({
                "group.id": config.KAFKA_GROUP,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            })
            c = Consumer(conf)
            c.subscribe([topic])
            _consumers[topic] = c
        return _consumers[topic]

    def enqueue(topic: str, msg: Dict[str, Any]) -> None:
        try:
            ensure_topic(topic)
        except Exception:
            pass
        p = _get_producer()
        payload = json.dumps(msg).encode("utf-8")
        p.produce(topic, value=payload)
        p.poll(0)

    def dequeue_blocking(topic: str, timeout: int = 0) -> Optional[Dict[str, Any]]:
        c = _get_consumer(topic)
        t = float(timeout) if timeout and timeout > 0 else 1.0
        m = c.poll(timeout=t)
        if m is None:
            return None
        if m.error():
            raise KafkaException(m.error())
        try:
            return json.loads(m.value().decode("utf-8"))
        except Exception as e:
            print(f"[kafka] decode error: {e}")
            return None

else:
    raise RuntimeError(f"Unsupported QUEUE_BACKEND: {config.QUEUE_BACKEND}")
