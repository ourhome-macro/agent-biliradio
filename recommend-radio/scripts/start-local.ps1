[CmdletBinding()]
param(
    [switch]$Rebuild,
    [switch]$NoFrontend,
    [switch]$EnableFeed,
    [switch]$ValidateOnly
)

$ErrorActionPreference = 'Stop'
$appRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $appRoot '..')).Path
$backendRoot = Join-Path $appRoot 'backend'
$frontendRoot = Join-Path $appRoot 'frontend'
$runRoot = Join-Path $repoRoot '.amem'
$launcher = (Get-Command py -ErrorAction Stop).Source
$python = (& $launcher -3.12 -c 'import sys; print(sys.executable)' | Select-Object -First 1).Trim()
if (-not (Test-Path -LiteralPath $python)) { throw 'Python 3.12 executable was not found.' }
$redisConfigJson = & $python -c 'import json,os,sys; from dotenv import load_dotenv; load_dotenv(sys.argv[1],override=False); print(json.dumps({key:os.getenv(key,"") for key in ("JOB_REDIS_URL","FEED_REDIS_URL","REDIS_SERVER_PATH")}))' (Join-Path $appRoot '.env')
if ($LASTEXITCODE -ne 0) { throw 'Could not read native Redis configuration.' }
$redisConfig = $redisConfigJson | ConvertFrom-Json
foreach ($key in @('JOB_REDIS_URL','FEED_REDIS_URL','REDIS_SERVER_PATH')) {
    if ($redisConfig.$key) { [Environment]::SetEnvironmentVariable($key,$redisConfig.$key,'Process') }
}

& $python -c 'import a2wsgi, celery, starlette, uvicorn, redis'
if ($LASTEXITCODE -ne 0) { throw 'Install recommend-radio/backend/requirements.txt first.' }
$redisServerPath = $env:REDIS_SERVER_PATH
if (-not $redisServerPath) {
    $redisFolder = Get-ChildItem -LiteralPath $repoRoot -Directory -Filter 'Redis-*' |
        Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName 'redis-server.exe') } |
        Select-Object -First 1
    if ($redisFolder) { $redisServerPath = Join-Path $redisFolder.FullName 'redis-server.exe' }
}
if (-not $redisServerPath -or -not (Test-Path -LiteralPath $redisServerPath)) {
    throw 'Set REDIS_SERVER_PATH to the supplied Windows Redis server.'
}
& $redisServerPath --version
if ($LASTEXITCODE -ne 0) { throw 'Native Redis binary validation failed.' }
if ($ValidateOnly) {
    Write-Host 'Native Redis and host Python dependencies are valid; Docker is not required.'
    return
}
New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
$redisState = Join-Path $appRoot 'server-data/redis'
New-Item -ItemType Directory -Path $redisState -Force | Out-Null
$env:JOB_TRANSPORT = 'redis_stream'
$env:RABBITMQ_ENABLED = 'false'
if (-not $env:JOB_REDIS_URL) { $env:JOB_REDIS_URL = 'redis://127.0.0.1:6379/1' }
$redisOwned = $null
& $python -c 'import os,redis; redis.Redis.from_url(os.environ["JOB_REDIS_URL"],socket_connect_timeout=1).ping()' 2>$null
if ($LASTEXITCODE -ne 0) {
    $redisOrigin = [Uri]$env:JOB_REDIS_URL
    if ($redisOrigin.Host -notin @('127.0.0.1', 'localhost') -or $redisOrigin.UserInfo) {
        throw 'Start the configured Redis endpoint before launching Radio.'
    }
    $redisOwned = Start-Process -FilePath $redisServerPath -ArgumentList @(
        '--bind', '127.0.0.1', '--port', "$($redisOrigin.Port)",
        '--dir', ('"' + $redisState + '"'), '--appendonly', 'yes',
        '--appendfsync', 'everysec', '--maxmemory', '512mb',
        '--maxmemory-policy', 'noeviction', '--protected-mode', 'yes'
    ) -WorkingDirectory (Split-Path -Parent $redisServerPath) -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $runRoot 'radio-redis.out.log') `
        -RedirectStandardError (Join-Path $runRoot 'radio-redis.err.log')
    $redisReady = $false
    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        & $python -c 'import os,redis; redis.Redis.from_url(os.environ["JOB_REDIS_URL"],socket_connect_timeout=1).ping()' 2>$null
        if ($LASTEXITCODE -eq 0) { $redisReady = $true; break }
        Start-Sleep -Milliseconds 500
    }
    if (-not $redisReady) { throw 'Native Redis did not become ready.' }
}

if (-not $NoFrontend -and ($Rebuild -or -not (Test-Path (Join-Path $frontendRoot 'dist\index.html')))) {
    Push-Location $frontendRoot
    try {
        if (-not (Test-Path node_modules)) { & npm ci }
        if ($LASTEXITCODE -ne 0) { throw 'npm ci failed.' }
        & npm run build
        if ($LASTEXITCODE -ne 0) { throw 'Frontend build failed.' }
    } finally {
        Pop-Location
    }
}

New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $appRoot 'server-data') -Force | Out-Null
$env:APP_DATA_DIR = Join-Path $appRoot 'server-data'
if ($EnableFeed) {
    $env:FEED_ENABLED = 'true'
    if (-not $env:FEED_REDIS_URL) { $env:FEED_REDIS_URL = $env:JOB_REDIS_URL -replace '/[0-9]+$', '/0' }
    $env:MEDIA_WORKDIR = Join-Path $env:APP_DATA_DIR 'media-work'
}
$env:AMEM_DB_PATH = Join-Path $env:APP_DATA_DIR 'amem.sqlite3'
$env:AMEM_TRANSPORT = 'embedded'
$env:RABBITMQ_ENABLED = 'false'
$env:RADIO_OUTBOX_EMBEDDED = 'true'
$env:RABBITMQ_HOST = '127.0.0.1'
$env:SSE_GATEWAY_INTERNAL_URL = ''
$env:OTEL_SDK_DISABLED = 'true'
$env:FRONTEND_DIST = Join-Path $frontendRoot 'dist'
$embeddingUrl = $env:AMEM_EMBEDDING_BASE_URL
if (-not $embeddingUrl -and (Test-Path -LiteralPath (Join-Path $appRoot '.env'))) {
    $match = [regex]::Match(
        (Get-Content -LiteralPath (Join-Path $appRoot '.env') -Raw),
        '(?m)^AMEM_EMBEDDING_BASE_URL=(.+)$'
    )
    if ($match.Success) { $embeddingUrl = $match.Groups[1].Value.Trim(' ', '"', "'") }
}
if (-not $embeddingUrl) { $embeddingUrl = 'http://127.0.0.1:8001/v1' }
$env:AMEM_EMBEDDING_BASE_URL = $embeddingUrl -replace 'host\.docker\.internal', '127.0.0.1'
$parsedEmbedding = [Uri]$env:AMEM_EMBEDDING_BASE_URL
if ($parsedEmbedding.Host -in @('127.0.0.1', 'localhost')) {
    $health = "http://127.0.0.1:$($parsedEmbedding.Port)/health"
    try {
        Invoke-RestMethod -Uri $health -TimeoutSec 3 | Out-Null
    } catch {
        $service = Get-Service -Name 'RecommendRadioBgeM3' -ErrorAction SilentlyContinue
        if ($service) {
            if ($service.Status -ne 'Running') { Start-Service -Name 'RecommendRadioBgeM3' }
        } else {
            & $python -c 'import fastapi, uvicorn, sentence_transformers'
            if ($LASTEXITCODE -ne 0) {
                throw 'Start the configured embedding service or install its Python dependencies.'
            }
            Start-Process -FilePath $python -ArgumentList @(
                '-m', 'uvicorn', 'embedding_server:app',
                '--host', '127.0.0.1', '--port', "$($parsedEmbedding.Port)"
            ) -WorkingDirectory $backendRoot -WindowStyle Hidden
        }
        $ready = $false
        for ($attempt = 0; $attempt -lt 45; $attempt++) {
            try {
                Invoke-RestMethod -Uri $health -TimeoutSec 3 | Out-Null
                $ready = $true
                break
            } catch { Start-Sleep -Seconds 2 }
        }
        if (-not $ready) { throw 'Embedding endpoint did not become ready.' }
    }
}

Push-Location $backendRoot
try {
    & $python migrate_db.py
    if ($LASTEXITCODE -ne 0) { throw 'Database migration failed; application was not started.' }
    if ($EnableFeed) {
        & $python -m feed_runtime.cli provision
        if ($LASTEXITCODE -ne 0) { throw 'Configure the existing MinIO service and Feed credentials before enabling Feed.' }
    }
} finally {
    Pop-Location
}

$env:AUTO_DREAM_ENABLED = 'false'
$web = Start-Process -FilePath $python -ArgumentList @(
    '-m', 'uvicorn', 'web_asgi:app', '--host', '127.0.0.1', '--port', '3000'
) -WorkingDirectory $backendRoot -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput (Join-Path $runRoot 'radio-web.out.log') `
    -RedirectStandardError (Join-Path $runRoot 'radio-web.err.log')

$env:AUTO_DREAM_ENABLED = 'true'
$worker = Start-Process -FilePath $python -ArgumentList @(
    '-m', 'redis_stream_jobs', '--lanes', 'jobs,events', '--concurrency', '2'
) -WorkingDirectory $backendRoot -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput (Join-Path $runRoot 'radio-worker.out.log') `
    -RedirectStandardError (Join-Path $runRoot 'radio-worker.err.log')

$mediaWorker = $null
if ($EnableFeed) {
    $env:AUTO_DREAM_ENABLED = 'false'
    $env:AMEM_ENABLED = 'false'
    $mediaWorker = Start-Process -FilePath $python -ArgumentList @(
        '-m', 'redis_stream_jobs', '--lanes', 'media', '--concurrency', '2'
    ) -WorkingDirectory $backendRoot -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $runRoot 'radio-media.out.log') `
        -RedirectStandardError (Join-Path $runRoot 'radio-media.err.log')
}
@{ web = $web.Id; worker = $worker.Id; redis = $(if ($redisOwned) { $redisOwned.Id } else { 0 }); redisPort = ([Uri]$env:JOB_REDIS_URL).Port; mediaWorker = $(if ($mediaWorker) { $mediaWorker.Id } else { 0 }) } |
    ConvertTo-Json | Set-Content -LiteralPath (Join-Path $runRoot 'radio-host-pids.json') -Encoding UTF8

$deadline = (Get-Date).AddSeconds(90)
$healthy = $false
do {
    if ($web.HasExited -or $worker.HasExited -or ($mediaWorker -and $mediaWorker.HasExited)) { break }
    try {
        $response = Invoke-RestMethod -Uri 'http://127.0.0.1:3000/health/ready' -TimeoutSec 3
        $healthy = $response.data.status -eq 'ready'
    } catch { }
    if (-not $healthy) { Start-Sleep -Seconds 2 }
} while (-not $healthy -and (Get-Date) -lt $deadline)
if (-not $healthy) {
    foreach ($started in @($web, $worker, $mediaWorker)) {
        if (-not $started) { continue }
        if (-not $started.HasExited) {
            Stop-Process -Id $started.Id -ErrorAction SilentlyContinue
        }
    }
    Remove-Item -LiteralPath (Join-Path $runRoot 'radio-host-pids.json') -Force -ErrorAction SilentlyContinue
    throw "Radio host services did not become ready; inspect logs in $runRoot"
}
if ($EnableFeed) {
    Write-Host 'Radio ready: http://127.0.0.1:3000 (native Redis Stream)'
    Write-Host 'Feed enabled; media worker uses the configured existing MinIO service.'
} else {
    Write-Host 'Radio ready: http://127.0.0.1:3000 (native Redis Stream)'
}
