param(
    [ValidateSet('prepare','infra','build','up','status','down','validate','start-native-storage','stop-native-storage')]
    [string]$Action = 'status',
    [string]$Project = 'radio-kafka',
    [string]$PrivateEnv = (Join-Path $PSScriptRoot '..\server-data\kafka-acceptance\private.env')
)
$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$workspaceRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot '..'))
$privatePath = [IO.Path]::GetFullPath($PrivateEnv)
$privateDirectory = Split-Path -Parent $privatePath
if ($Project -notmatch '^[a-z0-9][a-z0-9_-]*$') { throw 'Use a lowercase Docker project identifier.' }
if ($Action -in @('prepare','infra','build','up','start-native-storage')) {
    & (Join-Path $PSScriptRoot 'init-docker-private-env.ps1') -PrivateEnv $privatePath
}
if (-not (Test-Path -LiteralPath $privatePath)) { throw 'Run prepare first to create the isolated private environment.' }
$settings = @{}
foreach ($line in [IO.File]::ReadAllLines($privatePath)) {
    if ($line -match '^([^#=]+)=(.*)$') { $settings[$matches[1]] = $matches[2] }
}
foreach ($key in @('RADIO_DOCKER_DATABASE_URL','RADIO_DOCKER_MIGRATION_DATABASE_URL')) {
    $targetUri = [Uri]$settings[$key]
    if ($targetUri.Host -ne 'mysql' -or $targetUri.Port -ne 3306 -or $targetUri.AbsolutePath.Trim('/') -ne $settings.MYSQL_DATABASE) {
        throw "Docker DSN $key must target mysql:3306 and the configured MYSQL_DATABASE."
    }
}
function Invoke-Compose([string]$File, [string[]]$Command) {
    & docker compose --progress plain --project-name $Project --env-file $privatePath -f (Join-Path $projectRoot $File) @Command
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed ($LASTEXITCODE): $($Command[0])" }
}
function Storage-Process {
    $pidFile = Join-Path $privateDirectory 'minio.pid'
    $dataPath = [IO.Path]::GetFullPath((Join-Path $privateDirectory 'minio-data'))
    $binaryPath = Join-Path $workspaceRoot 'minio.exe'
    $dataArgument = '(^|\s)"?' + [regex]::Escape($dataPath) + '"?(\s|$)'
    if (Test-Path -LiteralPath $pidFile) {
        $number = 0
        if ([int]::TryParse(([IO.File]::ReadAllText($pidFile).Trim()), [ref]$number)) {
            $candidate = Get-CimInstance Win32_Process -Filter "ProcessId=$number" -ErrorAction SilentlyContinue
            if ($candidate -and $candidate.ExecutablePath -eq $binaryPath -and $candidate.CommandLine -match $dataArgument) { return $candidate }
        }
    }
    $owned = @(Get-CimInstance Win32_Process -Filter "Name='minio.exe'" -ErrorAction SilentlyContinue | Where-Object {
        $_.ExecutablePath -eq $binaryPath -and $_.CommandLine -match $dataArgument
    })
    if ($owned.Count -eq 1) { [IO.File]::WriteAllText($pidFile, [string]$owned[0].ProcessId); return $owned[0] }
    return $null
}
function Start-Storage {
    $owned = Storage-Process
    if ($owned) { Write-Output "Isolated MinIO already running (PID $($owned.ProcessId))."; return }
    $binary = Join-Path $workspaceRoot 'minio.exe'
    if (-not (Test-Path -LiteralPath $binary)) { throw 'Workspace minio.exe is required for the configured host media storage.' }
    $dataPath = [IO.Path]::GetFullPath((Join-Path $privateDirectory 'minio-data'))
    New-Item -ItemType Directory -Force -Path $dataPath | Out-Null
    $port = if ($settings.MINIO_DOCKER_PORT) { $settings.MINIO_DOCKER_PORT } else { '19000' }
    $console = if ($settings.MINIO_CONSOLE_PORT) { $settings.MINIO_CONSOLE_PORT } else { '19001' }
    $oldUser, $oldPassword = $env:MINIO_ROOT_USER, $env:MINIO_ROOT_PASSWORD
    try {
        $env:MINIO_ROOT_USER = $settings.MINIO_ACCESS_KEY
        $env:MINIO_ROOT_PASSWORD = $settings.MINIO_SECRET_KEY
        $process = Start-Process -FilePath $binary -ArgumentList @('server', ('"' + $dataPath + '"'), '--address', "127.0.0.1:$port", '--console-address', "127.0.0.1:$console") -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $privateDirectory 'minio.stdout.log') -RedirectStandardError (Join-Path $privateDirectory 'minio.stderr.log')
    } finally { $env:MINIO_ROOT_USER = $oldUser; $env:MINIO_ROOT_PASSWORD = $oldPassword }
    [IO.File]::WriteAllText((Join-Path $privateDirectory 'minio.pid'), [string]$process.Id)
    for ($attempt=0; $attempt -lt 30; $attempt++) {
        try { $reply = Invoke-WebRequest -Uri "http://127.0.0.1:$port/minio/health/live" -TimeoutSec 1; if ($reply.StatusCode -eq 200) { Write-Output 'Isolated host MinIO is healthy.'; return } } catch { }
        if ($process.HasExited) { throw 'MinIO startup failed; inspect its private log files.' }
        Start-Sleep -Milliseconds 250
    }
    throw 'MinIO did not become healthy within the startup budget.'
}
Push-Location $projectRoot
try {
    switch ($Action) {
        'prepare' { Invoke-Compose 'docker-compose.yml' @('config','--quiet'); Write-Output 'Docker configuration validated; original .env was not modified.' }
        'validate' { Invoke-Compose 'docker-compose.yml' @('config','--quiet') }
        'infra' {
            Invoke-Compose 'docker-compose.infra.yml' @('up','-d','--wait','--wait-timeout','180','mysql','redis-cache')
            & py -3.12 (Join-Path $PSScriptRoot 'bootstrap-docker-mysql.py') --private-env $privatePath
            if ($LASTEXITCODE -ne 0) { throw 'Isolated MySQL account bootstrap failed.' }
            Invoke-Compose 'docker-compose.infra.yml' @('up','-d','--wait','--wait-timeout','240','kafka','elasticsearch')
        }
        'build' { Invoke-Compose 'docker-compose.yml' @('build','backend','frontend') }
        'up' {
            Start-Storage
            New-Item -ItemType Directory -Force -Path (Join-Path $projectRoot 'server-data\docker-runtime\app') | Out-Null
            Invoke-Compose 'docker-compose.yml' @('build','backend','frontend')
            Invoke-Compose 'docker-compose.yml' @('up','-d','--no-build')
        }
        'status' { Invoke-Compose 'docker-compose.yml' @('ps'); $owned=Storage-Process; Write-Output $(if ($owned) { 'Isolated host MinIO: running' } else { 'Isolated host MinIO: not managed/running' }) }
        'down' { Invoke-Compose 'docker-compose.yml' @('down'); Write-Output 'Named volumes and host media data retained.' }
        'start-native-storage' { Start-Storage }
        'stop-native-storage' { $owned=Storage-Process; if ($owned) { Stop-Process -Id $owned.ProcessId; Write-Output 'Stopped only the verified isolated MinIO process.' } }
    }
} finally { Pop-Location }
