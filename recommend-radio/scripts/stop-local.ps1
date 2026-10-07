[CmdletBinding()]
param()

$runRoot = Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) '.amem'
$stateFile = Join-Path $runRoot 'radio-host-pids.json'
if (-not (Test-Path -LiteralPath $stateFile)) { return }
$state = Get-Content -LiteralPath $stateFile -Raw | ConvertFrom-Json
foreach ($name in @('web', 'worker', 'mediaWorker')) {
    $processId = [int]$state.$name
    if ($processId -le 0) { continue }
    $process = Get-CimInstance Win32_Process -Filter "ProcessId=$processId" -ErrorAction SilentlyContinue
    if ($process -and $process.CommandLine -match '(web_asgi:app|task_app.*worker|redis_stream_jobs)') {
        Stop-Process -Id $processId -ErrorAction SilentlyContinue
    }
}
if ([int]$state.redis -gt 0) {
    $process = Get-CimInstance Win32_Process -Filter "ProcessId=$([int]$state.redis)" -ErrorAction SilentlyContinue
    $ownedDirectory = Join-Path (Split-Path -Parent $PSScriptRoot) 'server-data/redis'
    if ($process -and $process.Name -eq 'redis-server.exe' -and $process.CommandLine.Contains($ownedDirectory)) {
        & py -3.12 -c 'import redis,sys; redis.Redis(host="127.0.0.1",port=int(sys.argv[1]),socket_timeout=5).shutdown(nosave=True)' ([int]$state.redisPort) 2>$null
        if ($LASTEXITCODE -ne 0) { Stop-Process -Id ([int]$state.redis) -ErrorAction SilentlyContinue }
    }
}
Remove-Item -LiteralPath $stateFile -Force
