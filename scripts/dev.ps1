# =============================================================================
# STREAMX — local dev stack for Windows (PowerShell)
#
# Equivalent of `make dev` (see Makefile). Starts, in separate windows:
#   - 3 brokers  (:9092-9094, metrics :9102-9104)
#   - API        (:8080, SQLite at data\streamx.db)
#   - Web        (:3000)
#
# Usage (from the repo root):
#   powershell -ExecutionPolicy Bypass -File scripts\dev.ps1
#
# Stop: close each window, or press Ctrl-C inside a window.
# =============================================================================
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)   # always run from repo root

New-Item -ItemType Directory -Force .\data | Out-Null

$cluster = "broker-1=127.0.0.1:9092,broker-2=127.0.0.1:9093,broker-3=127.0.0.1:9094"

Write-Host "==> STREAMX dev cluster (Windows)"
Write-Host "    DB: sqlite (data\streamx.db)"

# --- 3 brokers -----------------------------------------------------------
for ($i = 1; $i -le 3; $i++) {
    $env:STREAMX_BROKER_ID       = "broker-$i"
    $env:STREAMX_BROKER_ADDR     = ":$($i + 9091)"
    $env:STREAMX_METRICS_ADDR    = ":$($i + 9101)"
    $env:STREAMX_CLUSTER         = $cluster
    $env:STREAMX_DATA_DIR        = ".\data\broker-$i"
    Start-Process powershell -ArgumentList "-NoExit","-Command","go run .\cmd\broker"
    Write-Host "==> broker-$i on :$($i + 9091) (metrics :$($i + 9101))"
}

# --- API ------------------------------------------------------------------
$env:STREAMX_API_ADDR          = ":8080"
$env:STREAMX_DB_URL            = "sqlite://./data/streamx.db"
$env:STREAMX_ADMIN_USER        = "admin"
$env:STREAMX_ADMIN_PASSWORD    = "admin123"
$env:STREAMX_OPERATOR_USER     = "operator"
$env:STREAMX_OPERATOR_PASSWORD = "operator123"
$env:STREAMX_REDIS_URL         = ""
$env:STREAMX_BROKER_CLUSTER    = $cluster
Start-Process powershell -ArgumentList "-NoExit","-Command","go run .\cmd\api"
Write-Host "==> api on :8080"

# --- Web ------------------------------------------------------------------
Start-Process powershell -ArgumentList "-NoExit","-Command","cd apps\web; npm run dev"
Write-Host "==> web on :3000"

Write-Host ""
Write-Host "==> all started. Dashboard: http://localhost:3000 (admin / admin123)"
Write-Host "    Stop: close each window (or Ctrl-C inside a window)."