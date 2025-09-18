Param(
  [switch]$UploadIfMissing = $true
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Resolve-Path (Join-Path $here "..") | % Path
Set-Location $root

# 0) Load config
if (Test-Path ".\.env.ps1") { . .\.env.ps1 } else {
  throw "Missing .env.ps1 at repo root. Create it first."
}

# 1) Ensure kubectl port-forward to MinIO (IPv4)
function Test-MinIO {
  try {
    $resp = & "$env:SystemRoot\System32\curl.exe" -s -o NUL -w "%{http_code}" "$($env:S3_ENDPOINT)/minio/health/ready"
    return ($resp -eq "200")
  } catch { return $false }
}

if (-not (Test-MinIO)) {
  Write-Host "Starting kubectl port-forward to MinIO..."
  $ns = $env:KUBE_NAMESPACE
  Start-Process -WindowStyle Hidden -FilePath "kubectl" -ArgumentList "-n $ns port-forward --address 127.0.0.1 svc/minio 9000:9000"

  # Wait until health=200
  $deadline = (Get-Date).AddSeconds(20)
  do {
    Start-Sleep -Milliseconds 600
    $ok = Test-MinIO
  } until ($ok -or (Get-Date) -gt $deadline)
  if (-not $ok) { throw "MinIO port-forward failed (no health 200). Check 'kubectl -n $ns get svc minio'." }
}

# 2) Upload dataset to s3://bucket/prefix if missing
$bucket = $env:S3_BUCKET
$prefix = $env:INPUT_PREFIX.TrimStart("/")
$inputUri = "s3://$bucket/$prefix"
$baseUri  = "s3://$bucket"

$py = @"
import os, fsspec, glob, sys
endpoint=os.environ["S3_ENDPOINT"]; key=os.environ["S3_ACCESS_KEY"]; secret=os.environ["S3_SECRET_KEY"]
region=os.environ.get("S3_REGION","us-east-1"); bucket=os.environ["S3_BUCKET"]; prefix=os.environ["INPUT_PREFIX"].lstrip("/")
local=os.environ.get("LOCAL_DATASET_DIR","dataset")
fs=fsspec.filesystem("s3", key=key, secret=secret,
    client_kwargs={"endpoint_url":endpoint, "region_name":region},
    config_kwargs={"s3":{"addressing_style":"path"}})
need_upload = (not fs.exists(f"s3://{bucket}/{prefix}")) or (len(fs.ls(f"s3://{bucket}/{prefix}"))==0)
print(f"s3://{bucket}/{prefix} exists: ", not need_upload)
if need_upload:
    try: fs.mkdir(f"s3://{bucket}")
    except Exception: pass
    try: fs.mkdirs(f"s3://{bucket}/{prefix}")
    except Exception: pass
    files = glob.glob(os.path.join(local, "*"))
    if not files:
        print(f"No local files found in {local}; cannot upload."); sys.exit(2)
    for p in files:
        dst=f"s3://{bucket}/{prefix}{os.path.basename(p)}"
        with open(p,"rb") as fsrc, fs.open(dst,"wb") as fdst: fdst.write(fsrc.read())
        print("uploaded:", dst)
    print("upload_done")
else:
    print("upload_skip")
"@

if ($UploadIfMissing) {
  # Write to a temp .py and run it (PowerShell-safe)
  $tmpPy = Join-Path $env:TEMP ("upload_minio_{0}.py" -f ([guid]::NewGuid().ToString("n")))
  Set-Content -Path $tmpPy -Value $py -Encoding UTF8
  try {
    & python $tmpPy
    if ($LASTEXITCODE -ne 0) { throw "Python uploader exited with code $LASTEXITCODE" }
  }
  finally {
    Remove-Item $tmpPy -Force -ErrorAction SilentlyContinue
  }
}

# 3) Run PREP job (uses your existing module)
$env:INPUT_URI        = $inputUri
$env:OUTPUT_BASE_URI  = $baseUri

Write-Host "`nRunning PREP..."
& python -m src.prep_api.job_main
Write-Host "`nDone."
