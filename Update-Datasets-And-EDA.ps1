param(
  [string]$LocalDatasetDir = ".\dataset",
  [string]$Bucket = "mlstore",
  [string]$Domain = "traffic",
  [string]$S3Endpoint = "http://127.0.0.1:9000",  # change if you port-forward to another local port
  [string]$S3Key = $env:S3_ACCESS_KEY,
  [string]$S3Secret = $env:S3_SECRET_KEY,
  [int]$EDAPort = 8000,
  [switch]$StartEDA = $true
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
if (Test-Path ".\.env.ps1") { . .\.env.ps1 }

function Test-MinIO {
  try { (& "$env:SystemRoot\System32\curl.exe" -s -o NUL -w "%{http_code}" "$S3Endpoint/minio/health/ready") -eq "200" }
  catch { $false }
}
if ($S3Endpoint -match "127\.0\.0\.1:\d+" -and -not (Test-MinIO)) {
  $port = [int]($S3Endpoint -replace '.*:(\d+)$','$1')
  Write-Host "Starting port-forward to MinIO on $S3Endpoint..." -ForegroundColor Cyan
  Start-Process -WindowStyle Hidden -FilePath "kubectl" -ArgumentList "-n dev port-forward --address 127.0.0.1 svc/minio $port:9000"
  Start-Sleep -Seconds 2
  if (-not (Test-MinIO)) { throw "MinIO not reachable at $S3Endpoint" }
}

# Pick Python (PS5-safe)
$py = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }

# Deps
& $py -m pip install --quiet --upgrade pip
& $py -m pip install --quiet pandas pyarrow boto3 s3fs fsspec

# -- Step 1: upload local files -> s3://bucket/dataset/** (overwrite) with concurrency
$pyUpload = @'
import os, pathlib
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from boto3.s3.transfer import TransferConfig

EP=os.environ["S3_ENDPOINT"]; B=os.environ["S3_BUCKET"]; K=os.environ["S3_ACCESS_KEY"]; S=os.environ["S3_SECRET_KEY"]
root=pathlib.Path(os.environ.get("LOCAL_DATASET_DIR",".")).resolve()
if not root.exists(): raise SystemExit(f"Local folder missing: {root}")
files=[p for p in root.rglob("*") if p.is_file()]
s3=boto3.client("s3", endpoint_url=EP, aws_access_key_id=K, aws_secret_access_key=S, region_name="us-east-1")
cfg=TransferConfig(multipart_threshold=8*1024*1024, multipart_chunksize=8*1024*1024, max_concurrency=8, use_threads=True)

def put(p):
    key=f"dataset/{p.relative_to(root).as_posix()}"
    s3.upload_file(str(p), B, key, Config=cfg)
    print("uploaded:", key, flush=True)

print(f"starting_uploads total={len(files)}")
with ThreadPoolExecutor(max_workers=8) as ex:
    for _ in as_completed([ex.submit(put,p) for p in files]): pass
print("uploaded_files", len(files))
'@
$tmp1 = Join-Path $env:TEMP ("upload_ds_{0}.py" -f ([guid]::NewGuid().ToString("N")))
Set-Content $tmp1 $pyUpload -Encoding UTF8
$env:S3_ENDPOINT=$S3Endpoint; $env:S3_BUCKET=$Bucket; $env:S3_ACCESS_KEY=$S3Key; $env:S3_SECRET_KEY=$S3Secret
$env:LOCAL_DATASET_DIR = (Resolve-Path $LocalDatasetDir).Path
& $py $tmp1
Remove-Item $tmp1 -Force

# -- Step 2: create ONE manifest PER FILE with a STABLE ID (no timestamp); overwrite features/manifest
$pyMan = @'
import os, re, json, tempfile, pathlib
import pandas as pd, numpy as np
import pyarrow as pa, pyarrow.parquet as pq
import boto3, s3fs

EP=os.environ["S3_ENDPOINT"]; B=os.environ["S3_BUCKET"]; K=os.environ["S3_ACCESS_KEY"]; S=os.environ["S3_SECRET_KEY"]; D=os.environ.get("DOMAIN","traffic")
fs=s3fs.S3FileSystem(key=K, secret=S, client_kwargs={"endpoint_url":EP})
s3=boto3.client("s3", endpoint_url=EP, aws_access_key_id=K, aws_secret_access_key=S, region_name="us-east-1")

def clean(x): return re.sub(r"[^A-Za-z0-9_-]+","-",x)

# list all CSV/Parquet under dataset/
paths=fs.find(f"s3://{B}/dataset/")
files=[p for p in paths if not p.endswith("/") and p.split(".")[-1].lower() in ("csv","txt","parquet","pq")]
if not files:
    print("No CSV/Parquet under dataset/"); raise SystemExit(0)

for uri in sorted(files):
    # derive a STABLE dataset id from relative path (folder-file), so it's unique and never changes
    rel = uri.split("/dataset/",1)[-1]              # e.g. "sub/ElBorn.csv" or "ElBorn.csv"
    p   = pathlib.PurePosixPath(rel)
    stem = clean(p.stem)
    parent = clean(p.parent.name) if p.parent.name and p.parent.name != "" else ""
    ds_id = f"{parent}-{stem}" if parent and parent != "dataset" else stem

    ext = p.suffix.lower().lstrip(".")
    try:
        with fs.open(uri, "rb") as f:
            df = pd.read_csv(f, low_memory=False) if ext in ("csv","txt") else pd.read_parquet(f)
    except Exception as e:
        print("skip(read-error):", uri, "->", e); continue

    df = df.dropna(axis=1, how="all")
    if len(df)==0:
        print("skip(empty):", uri); continue

    n=len(df); cut=max(1,int(n*0.8))
    train, val = df.iloc[:cut], df.iloc[cut:]

    tmp=pathlib.Path(tempfile.mkdtemp())
    pq.write_table(pa.Table.from_pandas(train), tmp/"train.parquet")
    pq.write_table(pa.Table.from_pandas(val),   tmp/"val.parquet")

    # OVERWRITE features at a STABLE location
    s3.upload_file(str(tmp/"train.parquet"), B, f"pre/{ds_id}/train.parquet")
    s3.upload_file(str(tmp/"val.parquet"),   B, f"pre/{ds_id}/val.parquet")

    # OVERWRITE manifest at a STABLE location
    man={"dataset_id": ds_id,
         "source_key": uri,
         "artifacts":{
           "features_train": f"s3://{B}/pre/{ds_id}/train.parquet",
           "features_val":   f"s3://{B}/pre/{ds_id}/val.parquet"}}
    s3.put_object(Bucket=B, Key=f"manifests/{D}/{ds_id}/manifest.json",
                  Body=json.dumps(man).encode("utf-8"),
                  ContentType="application/json")
    print("manifest_written", f"s3://{B}/manifests/{D}/{ds_id}/manifest.json", "(overwritten)")
'@


# -- Step 3: purge old __root__/demo manifests (so catalog is clean)
$pyPurge = @'
import os, boto3
EP=os.environ["S3_ENDPOINT"]; B=os.environ["S3_BUCKET"]; K=os.environ["S3_ACCESS_KEY"]; S=os.environ["S3_SECRET_KEY"]
s3=boto3.client("s3", endpoint_url=EP, aws_access_key_id=K, aws_secret_access_key=S, region_name="us-east-1")
def purge(prefix):
    p=s3.get_paginator("list_objects_v2"); batch=[]
    for page in p.paginate(Bucket=B, Prefix=prefix):
        for o in page.get("Contents", []):
            batch.append({"Key": o["Key"]})
            if len(batch)==900: s3.delete_objects(Bucket=B, Delete={"Objects":batch}); batch=[]
    if batch: s3.delete_objects(Bucket=B, Delete={"Objects":batch})
purge("manifests/{DOMAIN}/__root__-".format(DOMAIN=os.environ.get("DOMAIN","traffic")))
purge("manifests/{DOMAIN}/demo-".format(DOMAIN=os.environ.get("DOMAIN","traffic")))
print("purged_noise")
'@
$tmp3 = Join-Path $env:TEMP ("purge_manifests_{0}.py" -f ([guid]::NewGuid().ToString("N")))
Set-Content $tmp3 $pyPurge -Encoding UTF8
& $py $tmp3
Remove-Item $tmp3 -Force

# -- Step 4: find newest REAL manifest and open EDA with manifest_uri=...
$pyLatest = @'
import os, boto3
EP=os.environ["S3_ENDPOINT"]; B=os.environ["S3_BUCKET"]; D=os.environ.get("DOMAIN","traffic")
s3=boto3.client("s3", endpoint_url=EP, region_name="us-east-1",
                aws_access_key_id=os.environ["S3_ACCESS_KEY"], aws_secret_access_key=os.environ["S3_SECRET_KEY"])
best=None; best_ts=None
p=s3.get_paginator("list_objects_v2")
for page in p.paginate(Bucket=B, Prefix=f"manifests/{D}/"):
    for o in page.get("Contents", []):
        k=o["Key"]
        if not k.endswith("/manifest.json"): continue
        if "/__root__-" in k or "/demo-" in k: continue
        ts=o.get("LastModified")
        if ts is None: continue
        if best_ts is None or ts>best_ts:
            best_ts=ts; best=k
if best:
    print("s3://{}/{}".format(B, best))
'@
$tmp4 = Join-Path $env:TEMP ("latest_manifest_{0}.py" -f ([guid]::NewGuid().ToString("N")))
Set-Content $tmp4 $pyLatest -Encoding UTF8
$latest = & $py $tmp4
Remove-Item $tmp4 -Force

if ($StartEDA) {
  $env:S3_ENDPOINT=$S3Endpoint; $env:S3_BUCKET=$Bucket; $env:S3_ACCESS_KEY=$S3Key; $env:S3_SECRET_KEY=$S3Secret; $env:DOMAIN=$Domain; $env:EDA_PORT="$EDAPort"
  Start-Process -FilePath "powershell" -ArgumentList "-NoProfile","-ExecutionPolicy","Bypass","-File",".\scripts\run-eda.ps1"
  Start-Sleep -Seconds 3
  if ($latest -and $latest.Trim().Length -gt 0) {
    Start-Process ("http://127.0.0.1:{0}/?manifest_uri={1}" -f $EDAPort, [uri]::EscapeDataString($latest.Trim()))
  } else {
    Start-Process ("http://127.0.0.1:{0}/?ds=latest" -f $EDAPort)
  }
}

Write-Host "`nDone. Uploaded local datasets, created per-file manifests, cleaned noise, and opened EDA." -ForegroundColor Green
