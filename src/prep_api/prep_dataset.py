#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PREP ingest for 'dataset/' root:
- Accepts local paths or s3:// URIs.
- Pairs <name>.csv with <name>_test.csv (case-insensitively).
- Handles mild typos (ElBorn vs elborn, PobleSec vs PoblesSec) via fuzzy matching.
- Writes train/val parquet + manifest.json per dataset.
- Stays robust/efficient: single-pass write to MinIO, idempotent dataset IDs, no chatty back-and-forth.

Usage examples:
  python -m src.prep_api.prep_dataset --input dataset/ --domain traffic --output-base s3://ml
  python -m src.prep_api.prep_dataset --input s3://ml/dataset/ --domain traffic
"""

import os, json, math, hashlib, re
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import List, Dict, Tuple
import difflib

import numpy as np
import pandas as pd
import fsspec
from src.prep_api.data_utils import apply_legacy_pipeline

# ------------------------
# CONFIG (env overrides)
# ------------------------
DEFAULT_DOMAIN = os.getenv("DOMAIN", "traffic")
DEFAULT_OUTPUT_BASE = os.getenv("OUTPUT_BASE_URI", "s3://ml")

S3_ENDPOINT = os.getenv("S3_ENDPOINT")  # e.g., http://minio.dev.svc.cluster.local:9000
S3_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET = os.getenv("S3_SECRET_KEY")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

REQUIRED_COLUMNS = [
    "time","down","up","rnti_count",
    "mcs_down","mcs_down_var","mcs_up","mcs_up_var",
    "rb_down","rb_down_var","rb_up","rb_up_var",
]

DTYPES = {
    "down": "float64",
    "up": "float64",
    "rnti_count": "Int64",
    "mcs_down": "float64",
    "mcs_down_var": "float64",
    "mcs_up": "float64",
    "mcs_up_var": "float64",
    "rb_down": "float64",
    "rb_down_var": "float64",
    "rb_up": "float64",
    "rb_up_var": "float64",
}

# ------------------------
# S3/MinIO helpers
# ------------------------
def storage_options(uri: str) -> Dict:
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        opts = {
            "key": S3_KEY,
            "secret": S3_SECRET,
            "client_kwargs": {"region_name": S3_REGION},
            "config_kwargs": {"s3": {"addressing_style": "path"}},  # <-- add this
        }
        if S3_ENDPOINT:
            opts["client_kwargs"]["endpoint_url"] = S3_ENDPOINT
        return opts
    return {}



def _fs_for(uri: str):
    parsed = urlparse(uri)
    if parsed.scheme in ("s3",):
        return fsspec.filesystem("s3", **storage_options(uri))
    return fsspec.filesystem("file")

def _is_dir(uri: str) -> bool:
    fs = _fs_for(uri)
    try:
        return fs.isdir(uri)
    except Exception:
        # local path with empty scheme
        return os.path.isdir(uri)

def list_csv_parquet(root: str) -> List[str]:
    fs = _fs_for(root)
    exts = (".csv", ".parquet", ".pq")
    files: List[str] = []

    parsed = urlparse(root)
    # S3/MinIO path discovery
    if parsed.scheme == "s3":
        base = root if root.endswith("/") else root + "/"
        # find() is most reliable across s3fs versions for recursive listing
        try:
            listing = fs.find(base, detail=False)
        except TypeError:
            listing = fs.find(base)  # older s3fs
        # keep only our extensions and ensure full s3:// prefix
        for p in listing:
            if p.lower().endswith(exts):
                if not p.startswith("s3://"):
                    p = "s3://" + p  # s3fs often returns "bucket/key"; make it a full URI
                files.append(p)
        return sorted(files)

    # Local/other FS
    # Recursive glob using ** (works for local)
    for pat in ("*.csv", "*.parquet", "*.pq"):
        files.extend(fs.glob(root.rstrip("/") + "/**/" + pat))

    # If root points to a single file, respect it
    if not files:
        if parsed.scheme == "" and os.path.isfile(root):
            return [root]
        if not _is_dir(root):
            return [root]
    return sorted(files)


# ------------------------
# Grouping & pairing
# ------------------------
_slug_re = re.compile(r"[^a-z0-9]+")

def base_slug(name: str) -> Tuple[str, bool]:
    """
    Produce a normalized base slug and whether it's a 'test' file.
    Examples:
      ElBorn.csv -> ('elborn', False)
      ElBorn_test.csv -> ('elborn', True)
      LesCorts_test.csv -> ('lescorts', True)
      full_dataset.csv -> ('full_dataset', False)
    """
    stem = os.path.splitext(os.path.basename(name))[0]
    is_test = False
    s = stem.lower()
    # normalize common test markers
    if s.endswith("_test") or s.endswith("-test") or s.endswith(" test"):
        is_test = True
        s = s.replace("_test","").replace("-test","").replace(" test","")
    # collapse separators
    s = _slug_re.sub("", s)  # keep only a-z0-9
    return s or "dataset", is_test

def fuzzy_canonicalize(slugs: List[str], threshold: float = 0.92) -> Dict[str, str]:
    """
    Map similar slugs to a canonical representative (handles minor typos like 'poblesec' vs 'poblessec').
    """
    canon: Dict[str, str] = {}
    reps: List[str] = []
    for s in slugs:
        if not reps:
            reps.append(s); canon[s] = s; continue
        match = difflib.get_close_matches(s, reps, n=1, cutoff=threshold)
        if match:
            canon[s] = match[0]
        else:
            reps.append(s); canon[s] = s
    # also map each rep to itself
    for r in reps:
        canon[r] = r
    return canon

def discover_groups(root_or_file: str) -> Dict[str, Dict[str, List[str]]]:
    files = list_csv_parquet(root_or_file)
    if not files:
        raise FileNotFoundError(f"No CSV/Parquet files under {root_or_file}")

    raw: List[Tuple[str, bool, str]] = []
    for path in files:
        s, is_test = base_slug(path)
        raw.append((s, is_test, path))

    # canonicalize slugs
    uniq_slugs = sorted({s for s, _, _ in raw})
    canon_map = fuzzy_canonicalize(uniq_slugs)

    groups: Dict[str, Dict[str, List[str]]] = {}
    for s, is_test, path in raw:
        c = canon_map.get(s, s)
        d = groups.setdefault(c, {"train_files": [], "val_files": []})
        if is_test:
            d["val_files"].append(path)
        else:
            d["train_files"].append(path)

    return groups

# ------------------------
# Core processing
# ------------------------
def compute_content_hash(file_uris: List[str]) -> str:
    h = hashlib.sha256()
    for u in sorted(file_uris):
        fs = _fs_for(u)
        info = fs.info(u)
        sig = f"{u}|{info.get('size')}|{info.get('mtime')}"
        h.update(sig.encode("utf-8"))
    return "sha256:" + h.hexdigest()

def read_concat(files: List[str]) -> pd.DataFrame:
    parts = []
    for u in files:
        so = storage_options(u)
        if u.lower().endswith((".parquet", ".pq")):
            df = pd.read_parquet(u, storage_options=so)
        else:
            df = pd.read_csv(
                u, storage_options=so, dtype=DTYPES,
                parse_dates=["time"]
)
        parts.append(df)
    df = pd.concat(parts, ignore_index=True)
    # columns check (allow extras)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    # ensure time is datetime (naive UTC)
    if not np.issubdtype(df["time"].dtype, np.datetime64):
        df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True).dt.tz_convert(None)
    # cast numerics
    for col, typ in DTYPES.items():
        if col in df.columns:
            if typ == "Int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(typ)
    df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    return df

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    eps = 1e-9
    rb_down = df["rb_down"].astype(float).fillna(0.0)
    rb_up = df["rb_up"].astype(float).fillna(0.0)
    mcs_down = df["mcs_down"].astype(float).fillna(0.0)
    mcs_up = df["mcs_up"].astype(float).fillna(0.0)
    mcs_down_var = df["mcs_down_var"].astype(float).clip(lower=0.0).fillna(0.0)
    mcs_up_var = df["mcs_up_var"].astype(float).clip(lower=0.0).fillna(0.0)

    out = df.copy()
    out["rb_total"] = rb_down + rb_up
    out["down_per_rb"] = out["down"].astype(float) / rb_down.replace(0, np.nan)
    out["up_per_rb"] = out["up"].astype(float) / rb_up.replace(0, np.nan)
    out["mcs_down_std"] = np.sqrt(mcs_down_var)
    out["mcs_up_std"] = np.sqrt(mcs_up_var)
    out["mcs_down_cv"] = out["mcs_down_std"] / (mcs_down + eps)
    out["mcs_up_cv"] = out["mcs_up_std"] / (mcs_up + eps)
    t = pd.to_datetime(out["time"], errors="coerce")
    out["hour_of_day"] = t.dt.hour
    out["day_of_week"] = t.dt.dayofweek
    return out

def time_split(df: pd.DataFrame, val_ratio: float = 0.2) -> Tuple[pd.DataFrame, pd.DataFrame]:
    n = len(df)
    cut = max(1, int(round(n * (1.0 - val_ratio))))
    return df.iloc[:cut].copy(), df.iloc[cut:].copy()

def profile_summary(df: pd.DataFrame) -> dict:
    # pandas compatibility: older versions don't accept datetime_is_numeric
    try:
        desc = df.describe(include="all", datetime_is_numeric=True).to_dict()
    except TypeError:
        desc = df.describe(include="all").to_dict()

    nulls = df.isna().sum().to_dict()
    return {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "null_counts": {k:int(v) for k,v in nulls.items()},
        "time_window": {
            "start": str(pd.to_datetime(df["time"]).min()),
            "end": str(pd.to_datetime(df["time"]).max()),
        },
        "describe": desc,
    }


def build_output_paths(base: str, domain: str, dataset_id: str) -> Dict[str, str]:
    root = base.rstrip("/")
    prefix = f"{root}/feature_store/{domain}/{dataset_id}"
    return {
        "train": f"{prefix}/train.parquet",
        "val": f"{prefix}/val.parquet",
        "stats": f"{root}/stats/{domain}/{dataset_id}/profile.json",
        "schema": f"{root}/schemas/{domain}/v1/schema.json",
        "manifest": f"{root}/manifests/{domain}/{dataset_id}/manifest.json",
    }

def write_parquet(df: pd.DataFrame, uri: str):
    fs = _fs_for(uri)
    parent = uri.rsplit("/", 1)[0]
    try:
        if not fs.exists(parent):
            fs.mkdirs(parent, exist_ok=True)
    except Exception:
        pass
    df.to_parquet(uri, index=False, compression="snappy", storage_options=storage_options(uri))

# in src/prep_api/prep_dataset.py



def _json_default(o):
    if isinstance(o, (pd.Timestamp, datetime)):
        return o.isoformat()
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    return str(o)

def write_json(obj: dict, uri: str):
    fs = _fs_for(uri)
    parent = uri.rsplit("/", 1)[0]
    try:
        if not fs.exists(parent):
            fs.mkdirs(parent, exist_ok=True)
    except Exception:
        pass
    with fs.open(uri, "wb") as f:
        f.write(json.dumps(obj, indent=2, sort_keys=True, default=_json_default).encode("utf-8"))


def default_schema() -> dict:
    return {
        "version": 1,
        "fields": [
            {"name":"time","type":"timestamp"},
            {"name":"down","type":"float"},
            {"name":"up","type":"float"},
            {"name":"rnti_count","type":"int"},
            {"name":"mcs_down","type":"float"},
            {"name":"mcs_down_var","type":"float"},
            {"name":"mcs_up","type":"float"},
            {"name":"mcs_up_var","type":"float"},
            {"name":"rb_down","type":"float"},
            {"name":"rb_down_var","type":"float"},
            {"name":"rb_up","type":"float"},
            {"name":"rb_up_var","type":"float"},
        ],
    }

def make_dataset_id(domain: str, slug: str, df: pd.DataFrame, content_hash: str) -> str:
    tmax = pd.to_datetime(df["time"]).max().strftime("%Y-%m-%dT%H%M")
    hash8 = content_hash.split(":")[1][:8]
    return f"{domain}-{slug}-{tmax}-{hash8}"

def process_group(slug: str, train_files: List[str], val_files: List[str],
                  domain: str, output_base: str) -> str:
    """
    If both train_files and val_files exist:
      - read train_files -> df_train
      - read val_files   -> df_val
    else:
      - read all files -> df_all -> time split
    Then add features, write parquet, stats, schema, manifest. Return manifest URI.
    """
    all_files = sorted(train_files + val_files)
    if not all_files:
        raise ValueError(f"No files for group '{slug}'")

    content_hash = compute_content_hash(all_files)

    if train_files and val_files:
        df_train_raw = read_concat(train_files)
        df_val_raw   = read_concat(val_files)
        df_train, scaler = apply_legacy_pipeline(df_train_raw, fit=True,  scaler_name="StandardScaler")
        df_val,   _      = apply_legacy_pipeline(df_val_raw,   fit=False, scaler=scaler)
        df_all = pd.concat([df_train, df_val], ignore_index=True).sort_values("time")

    else:
        df_all_raw = read_concat(all_files)
        df_all, scaler = apply_legacy_pipeline(df_all_raw, fit=True, scaler_name="StandardScaler")
        df_train, df_val = time_split(df_all, val_ratio=0.2)


    dataset_id = make_dataset_id(domain, slug, df_all, content_hash)
    paths = build_output_paths(output_base, domain, dataset_id)

    # writes
    write_parquet(df_train, paths["train"])
    write_parquet(df_val, paths["val"])
    write_json(profile_summary(df_all), paths["stats"])
    write_json(default_schema(), paths["schema"])

    manifest = {
        "dataset_id": dataset_id,
        "domain": domain,
        "slug": slug,
        "content_hash": content_hash,
        "time_window": {
            "start": str(pd.to_datetime(df_all["time"]).min()),
            "end": str(pd.to_datetime(df_all["time"]).max()),
        },
        "artifacts": {
            "features_train": paths["train"],
            "features_val": paths["val"],
            "stats": paths["stats"],
        },
        "schema": paths["schema"],
        "created_by": {
            "git_sha": os.getenv("GIT_SHA",""),
            "image": os.getenv("IMAGE_TAG","prep:dev"),
        },
        "created_at": datetime.now(timezone.utc).isoformat(),
        "columns": REQUIRED_COLUMNS,
        "features_added": [
            "rb_total","down_per_rb","up_per_rb",
            "mcs_down_std","mcs_up_std","mcs_down_cv","mcs_up_cv",
            "hour_of_day","day_of_week"
        ],
        "source_files": all_files,
    }
    write_json(manifest, paths["manifest"])
    return paths["manifest"]

def process_input(input_uri: str, domain: str = DEFAULT_DOMAIN, output_base: str = DEFAULT_OUTPUT_BASE) -> List[str]:
    """
    If input is a root folder (e.g., dataset/), process all groups.
    If input is a single file or folder, process it as one group via time split.
    Returns list of manifest URIs.
    """
    manifests: List[str] = []
    if _is_dir(input_uri) or input_uri.endswith("/"):
        groups = discover_groups(input_uri)
        for slug, files in groups.items():
            # noise guard: ignore empty groups
            if not files["train_files"] and not files["val_files"]:
                continue
            m = process_group(slug, files["train_files"], files["val_files"], domain, output_base)
            manifests.append(m)
    else:
        # single path → treat as one group named by its slug
        s, is_test = base_slug(input_uri)
        # if it's a single *_test file, treat as val-only (train empty)
        train_files = [] if is_test else [input_uri]
        val_files = [input_uri] if is_test else []
        m = process_group(s, train_files, val_files, domain, output_base)
        manifests.append(m)
    return manifests

# ------------------------
# CLI
# ------------------------
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="PREP: ingest dataset root/file(s) and emit manifest(s) for TRAIN.")
    ap.add_argument("--input", required=True, help="Root folder (e.g., dataset/) or single file; supports s3://")
    ap.add_argument("--domain", default=DEFAULT_DOMAIN, help="Domain (for output paths)")
    ap.add_argument("--output-base", default=DEFAULT_OUTPUT_BASE, help="Where to write artifacts (e.g., s3://ml)")
    args = ap.parse_args()
    out = process_input(args.input, domain=args.domain, output_base=args.output_base)
    print(json.dumps({"manifest_uris": out}, indent=2))
