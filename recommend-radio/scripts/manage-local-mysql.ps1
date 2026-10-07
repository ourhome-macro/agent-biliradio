param(
    [ValidateSet('start', 'status', 'stop')]
    [string]$Action = 'status'
)

$ErrorActionPreference = 'Stop'
$mysqlRuntimeRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../server-data/mysql84'))
$mysqlServerExe = Join-Path $mysqlRuntimeRoot 'server/bin/mysqld.exe'
$mysqlConfigPath = Join-Path $mysqlRuntimeRoot 'my.ini'
$mysqlPidPath = Join-Path $mysqlRuntimeRoot 'process.pid'
if (-not (Test-Path -LiteralPath $mysqlConfigPath)) {
    throw 'Dedicated local MySQL is not provisioned in server-data/mysql84.'
}

if ($Action -eq 'start') {
    $existingProcess = $null
    if (Test-Path -LiteralPath $mysqlPidPath) {
        $existingId = [int](Get-Content -LiteralPath $mysqlPidPath -Raw).Trim()
        $existingProcess = Get-Process -Id $existingId -ErrorAction SilentlyContinue
    }
    if ($existingProcess -and $existingProcess.Path -eq $mysqlServerExe) {
        Write-Output "Dedicated MySQL already running (PID $($existingProcess.Id))."
    } else {
        $mysqlProcess = Start-Process -FilePath $mysqlServerExe `
            -ArgumentList @("`"--defaults-file=$mysqlConfigPath`"") `
            -WindowStyle Hidden -PassThru
        $mysqlProcess.Id | Set-Content -LiteralPath $mysqlPidPath
        Write-Output "Dedicated MySQL launched (PID $($mysqlProcess.Id)); use status to verify readiness."
    }
    return
}

# Credentials stay in the ACL-protected local file; never put them in process arguments.
$env:RADIO_LOCAL_MYSQL_DIRECTORY = $mysqlRuntimeRoot
$env:RADIO_LOCAL_MYSQL_ACTION = $Action
try {
    @'
import json, os
from pathlib import Path
import pymysql
from sqlalchemy.engine import make_url

root = Path(os.environ['RADIO_LOCAL_MYSQL_DIRECTORY'])
url = make_url(json.loads((root / 'credentials.json').read_text(encoding='utf-8'))['rootUrl'])
if url.host != '127.0.0.1' or url.port == 3306:
    raise SystemExit('Refusing to manage an instance outside the dedicated loopback deployment.')
try:
    conn = pymysql.connect(host=url.host, port=url.port, user=url.username,
                           password=url.password, connect_timeout=3)
except pymysql.Error:
    raise SystemExit('Dedicated MySQL is unavailable; inspect server-data/mysql84/mysql-error.log.') from None
with conn.cursor() as cursor:
    if os.environ['RADIO_LOCAL_MYSQL_ACTION'] == 'stop':
        cursor.execute('SHUTDOWN')
        print('Dedicated MySQL clean shutdown requested.')
    else:
        cursor.execute('SELECT VERSION()')
        print('Dedicated MySQL ready:', cursor.fetchone()[0], 'port', url.port)
conn.close()
'@ | py -3.12 -
    if ($LASTEXITCODE -ne 0) { throw 'Dedicated MySQL command failed.' }
} finally {
    Remove-Item Env:RADIO_LOCAL_MYSQL_DIRECTORY -ErrorAction SilentlyContinue
    Remove-Item Env:RADIO_LOCAL_MYSQL_ACTION -ErrorAction SilentlyContinue
}
