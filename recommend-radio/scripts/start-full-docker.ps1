[CmdletBinding()]
param(
    [switch]$Rebuild,
    [switch]$NoFrontend,
    [switch]$ValidateOnly
)

$ErrorActionPreference = 'Stop'

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$appRoot = Split-Path -Parent $scriptRoot
$backendRoot = Join-Path $appRoot 'backend'
$embeddingPort = 8001
$backendPort = 5000
$ssePort = 18080
$httpPort = 3000
$rabbitManagementPort = 15672
$startLocalEmbedding = $true

function Initialize-Configuration {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'Docker CLI was not found. Install Docker Desktop first.'
    }
    Push-Location $appRoot
    try {
        $configurationJson = & docker compose -f docker-compose.full.yml config --format json
        if ($LASTEXITCODE -ne 0) { throw 'Docker Compose configuration is invalid.' }
        $configuration = ($configurationJson -join "`n") | ConvertFrom-Json
        $script:backendPort = [int]$configuration.services.backend.ports[0].published
        $script:ssePort = [int]$configuration.services.'sse-gateway'.ports[0].published
        $script:httpPort = [int]$configuration.services.frontend.ports[0].published
        $managementMapping = $configuration.services.rabbitmq.ports | Where-Object { $_.target -eq 15672 }
        $script:rabbitManagementPort = [int]$managementMapping.published
        $embeddingUrl = [Uri]$configuration.services.amem.environment.AMEM_EMBEDDING_BASE_URL
        $script:startLocalEmbedding = $embeddingUrl.Host -in @('host.docker.internal', '127.0.0.1', 'localhost')
        if ($startLocalEmbedding) { $script:embeddingPort = $embeddingUrl.Port }
    } finally {
        Pop-Location
    }
}

function Test-HttpReady {
    param([string]$Url)
    try {
        $response = Invoke-RestMethod -Method Get -Uri $Url -TimeoutSec 3
        return $response
    } catch {
        return $null
    }
}

function Wait-HttpReady {
    param([string]$Url, [int]$TimeoutSeconds = 120)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $response = Test-HttpReady -Url $Url
        if ($null -ne $response) { return $response }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for $Url"
}

function Start-EmbeddingServer {
    if (-not $startLocalEmbedding) {
        Write-Host '[embedding] using configured external provider' -ForegroundColor Cyan
        return
    }
    $healthUrl = "http://127.0.0.1:$embeddingPort/health"
    $existing = Test-HttpReady -Url $healthUrl
    if ($existing -and $existing.status -eq 'ready') {
        Write-Host "[embedding] already ready: $($existing.model)" -ForegroundColor Green
        return
    }

    $embeddingService = Get-Service -Name 'RecommendRadioBgeM3' -ErrorAction SilentlyContinue
    if ($embeddingService) {
        Write-Host '[embedding] starting Windows service RecommendRadioBgeM3...' -ForegroundColor Cyan
        if ($embeddingService.Status -ne 'Running') { Start-Service -Name 'RecommendRadioBgeM3' }
        $ready = Wait-HttpReady -Url $healthUrl
        if ($ready.status -ne 'ready') { throw 'RecommendRadioBgeM3 did not become ready.' }
        Write-Host "[embedding] Windows service ready: $($ready.model)" -ForegroundColor Green
        return
    }

    $pythonLauncher = Join-Path $env:WINDIR 'py.exe'
    if (-not (Test-Path $pythonLauncher)) {
        throw 'Python launcher py.exe was not found. Install Python 3.12 first.'
    }
    & $pythonLauncher -3.12 -c "import uvicorn, fastapi, sentence_transformers"
    if ($LASTEXITCODE -ne 0) {
        throw 'Local embedding dependencies are missing: uvicorn, fastapi, sentence-transformers (Python 3.12).'
    }
    Write-Host '[embedding] starting local bge-m3 service...' -ForegroundColor Cyan
    Start-Process -FilePath $pythonLauncher `
        -ArgumentList @('-3.12', '-m', 'uvicorn', 'embedding_server:app', '--host', '127.0.0.1', '--port', "$embeddingPort") `
        -WorkingDirectory $backendRoot `
        -WindowStyle Hidden
    $ready = Wait-HttpReady -Url $healthUrl
    if ($ready.status -ne 'ready') { throw 'bge-m3 embedding service did not become ready.' }
    Write-Host "[embedding] ready: $($ready.model)" -ForegroundColor Green
}

function Test-DockerReady {
    $previousErrorPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'SilentlyContinue'
        & docker info *> $null
        $ready = $LASTEXITCODE -eq 0
    } catch {
        $ready = $false
    } finally {
        $ErrorActionPreference = $previousErrorPreference
    }
    return $ready
}

function Start-DockerEngine {
    if (Test-DockerReady) {
        Write-Host '[docker] engine already ready' -ForegroundColor Green
        return
    }
    $desktop = Join-Path $env:ProgramFiles 'Docker\Docker\Docker Desktop.exe'
    if (-not (Test-Path $desktop)) {
        throw 'Docker engine is unavailable and Docker Desktop was not found.'
    }
    Write-Host '[docker] starting Docker Desktop...' -ForegroundColor Cyan
    Start-Process -FilePath $desktop -WindowStyle Hidden
    $deadline = (Get-Date).AddSeconds(180)
    do {
        Start-Sleep -Seconds 3
        if (Test-DockerReady) {
            Write-Host '[docker] engine ready' -ForegroundColor Green
            return
        }
    } while ((Get-Date) -lt $deadline)
    throw 'Timed out waiting for Docker Desktop engine.'
}

function Start-ComposeStack {
    Push-Location $appRoot
    try {
        $services = @('otel-collector', 'rabbitmq', 'amem', 'outbox', 'task-worker', 'event-worker', 'backend', 'sse-gateway')
        if (-not $NoFrontend) { $services += 'frontend' }
        if ($Rebuild) {
            $buildServices = @('migrate') + @($services | Where-Object { $_ -notin @('rabbitmq','otel-collector') })
            & docker compose -f docker-compose.full.yml build @buildServices
            if ($LASTEXITCODE -ne 0) { throw "docker compose build failed with exit code $LASTEXITCODE" }
        }
        $arguments = @(
            'compose', '-f', 'docker-compose.full.yml', 'up', '-d', '--no-build',
            '--wait', '--wait-timeout', '240'
        )
        $arguments += $services
        Write-Host "[docker] starting: $($services -join ', ')" -ForegroundColor Cyan
        & docker @arguments
        if ($LASTEXITCODE -ne 0) { throw "docker compose failed with exit code $LASTEXITCODE" }
    } finally {
        Pop-Location
    }
}

Initialize-Configuration
if ($ValidateOnly) {
    Write-Host 'Startup arguments and Compose configuration are valid.' -ForegroundColor Green
    return
}
Start-DockerEngine
Start-EmbeddingServer
Start-ComposeStack

$backend = Wait-HttpReady -Url "http://127.0.0.1:$backendPort/health/ready" -TimeoutSeconds 90
if ($backend.data.status -ne 'ready') { throw 'Recommend Radio backend did not become ready.' }
$sseGateway = Wait-HttpReady -Url "http://127.0.0.1:$ssePort/health/ready" -TimeoutSeconds 60
if ($sseGateway.status -ne 'ready') { throw 'Recommend Radio SSE gateway did not become ready.' }

Write-Host ''
Write-Host 'Recommend Radio is ready.' -ForegroundColor Green
if (-not $NoFrontend) { Write-Host "  UI:       http://localhost:$httpPort" -ForegroundColor Green }
Write-Host "  Backend:  http://127.0.0.1:$backendPort" -ForegroundColor Green
Write-Host "  SSE:      http://127.0.0.1:$ssePort/health/ready" -ForegroundColor Green
if ($startLocalEmbedding) { Write-Host "  Embedding: http://127.0.0.1:$embeddingPort/health" -ForegroundColor Green }
Write-Host "  RabbitMQ: http://127.0.0.1:$rabbitManagementPort" -ForegroundColor Green
