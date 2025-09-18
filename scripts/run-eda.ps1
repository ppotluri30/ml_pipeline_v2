# scripts/run-eda.ps1
# Load config
if (Test-Path ".\.env.ps1") { . .\.env.ps1 } else { throw "Missing .env.ps1" }

# Ensure MinIO port-forward (reuse earlier health check)
$resp = & "$env:SystemRoot\System32\curl.exe" -s -o NUL -w "%{http_code}" "$($env:S3_ENDPOINT)/minio/health/ready"
if ($resp -ne "200") {
  Start-Process -WindowStyle Hidden -FilePath "kubectl" -ArgumentList "-n $($env:KUBE_NAMESPACE) port-forward --address 127.0.0.1 svc/minio 9000:9000"
  Start-Sleep -Seconds 2
}

# Find a free local port
function Get-FreePort {
  $l = New-Object System.Net.Sockets.TcpListener ([System.Net.IPAddress]::Loopback,0)
  $l.Start()
  $p = $l.LocalEndpoint.Port
  $l.Stop()
  return $p
}

.\.venv\Scripts\Activate.ps1
$env:EDA_MAX_ROWS = "200000"

# prefer EDA_PORT from env, else pick a free one
$port = if ($env:EDA_PORT) { [int]$env:EDA_PORT } else { Get-FreePort }

Write-Host "Starting EDA on http://127.0.0.1:$port ..."
try {
  uvicorn src.eda_api.main:app --host 127.0.0.1 --port $port --reload
} catch {
  # If a policy blocks this port (WinError 10013), try another free port once
  Write-Warning "Port $port failed (`$($_.Exception.Message)`); trying another."
  $port = Get-FreePort
  Write-Host "Starting EDA on http://127.0.0.1:$port ..."
  uvicorn src.eda_api.main:app --host 127.0.0.1 --port $port --reload
}
