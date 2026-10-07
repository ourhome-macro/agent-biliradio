[CmdletBinding()]
param()

$appRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $appRoot '..')).Path
$stateFile = Join-Path $repoRoot '.amem\radio-host-pids.json'

function Measure-DirectoryMiB([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return 0 }
    $sum = (Get-ChildItem -LiteralPath $Path -Recurse -File -ErrorAction SilentlyContinue |
        Measure-Object -Property Length -Sum).Sum
    return [math]::Round([double]$sum / 1MB, 2)
}

$processes = @()
if (Test-Path -LiteralPath $stateFile) {
    $pids = Get-Content -LiteralPath $stateFile -Raw | ConvertFrom-Json
    foreach ($role in @('web', 'worker')) {
        $processId = [int]$pids.$role
        $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
        if ($process) {
            $processes += [pscustomobject]@{
                Role = $role
                PID = $processId
                WorkingSetMiB = [math]::Round($process.WorkingSet64 / 1MB, 2)
                PrivateMiB = [math]::Round($process.PrivateMemorySize64 / 1MB, 2)
            }
        }
    }
}

$rabbitMemory = 'unavailable'
$ErrorActionPreference = 'Continue'
$dockerInfo = & docker info --format '{{.ServerVersion}}' 2>$null
if ($LASTEXITCODE -eq 0 -and $dockerInfo) {
    $rabbitMemory = (& docker stats --no-stream --format '{{.MemUsage}}' bilibili-radio-rabbitmq 2>$null)
}
$ErrorActionPreference = 'Stop'

[pscustomobject]@{
    SourceRuntimeMiB = Measure-DirectoryMiB (Join-Path $repoRoot 'src\agent_memory_runtime')
    BackendMiB = Measure-DirectoryMiB (Join-Path $appRoot 'backend')
    FrontendBuildMiB = Measure-DirectoryMiB (Join-Path $appRoot 'frontend\dist')
    RuntimeDataMiB = Measure-DirectoryMiB (Join-Path $appRoot 'server-data')
    Processes = $processes
    RabbitMQMemory = $rabbitMemory
} | ConvertTo-Json -Depth 5
