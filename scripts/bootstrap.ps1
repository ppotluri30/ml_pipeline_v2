# scripts/bootstrap.ps1
$ErrorActionPreference = "Stop"
if (-not (Test-Path ".\.venv")) { python -m venv .venv }
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip

@'
pandas>=2.0
pyarrow>=14
fsspec>=2024.2.0
s3fs>=2024.2.0
numpy>=1.25
scikit-learn>=1.3
fastapi[standard]>=0.115
uvicorn[standard]>=0.29
seaborn>=0.13
statsmodels>=0.14
dcor>=0.6
scipy>=1.11
'@ | Set-Content -Path requirements.txt -Encoding UTF8

pip install -r requirements.txt

# optional: we don't need boto3; remove to avoid botocore conflicts
pip uninstall -y boto3 2>$null | Out-Null

Write-Host "Bootstrap complete."
