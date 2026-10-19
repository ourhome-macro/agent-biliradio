param([string]$PrivateEnv = (Join-Path $PSScriptRoot '..\server-data\kafka-acceptance\private.env'), [string]$Database = 'radio_acceptance')
$ErrorActionPreference = 'Stop'
if ($Database -notmatch '^[A-Za-z][A-Za-z0-9_]{0,62}$') { throw 'Use a bounded alphanumeric MySQL schema name.' }
$target = [IO.Path]::GetFullPath($PrivateEnv)
$directory = Split-Path -Parent $target
New-Item -ItemType Directory -Force -Path $directory | Out-Null
$identity = [Security.Principal.WindowsIdentity]::GetCurrent().User
$acl = [Security.AccessControl.DirectorySecurity]::new()
$acl.SetAccessRuleProtection($true, $false)
$acl.SetOwner($identity)
foreach ($principal in @($identity, [Security.Principal.SecurityIdentifier]::new('S-1-5-18'))) {
    $rule = [Security.AccessControl.FileSystemAccessRule]::new($principal, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
    $acl.AddAccessRule($rule)
}
Set-Acl -LiteralPath $directory -AclObject $acl
if (Test-Path -LiteralPath $target) { Write-Output 'Existing private environment retained.'; exit 0 }
function New-Secret { return [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLowerInvariant() }
$rootPassword = New-Secret
$appPassword = New-Secret
$adminPassword = New-Secret
$elasticPassword = New-Secret
$readPassword = New-Secret
$indexPassword = New-Secret
$minioPassword = New-Secret
$cluster = [Convert]::ToBase64String([Guid]::NewGuid().ToByteArray()).TrimEnd('=').Replace('+','-').Replace('/','_')
$values = [ordered]@{
    MYSQL_ROOT_PASSWORD=$rootPassword; MYSQL_DATABASE=$Database; MYSQL_APP_USER='radio_app'; MYSQL_APP_PASSWORD=$appPassword
    MYSQL_ADMIN_USER='radio_admin'; MYSQL_ADMIN_PASSWORD=$adminPassword
    MYSQL_DOCKER_PORT='13307'; KAFKA_DOCKER_PORT='19092'; REDIS_DOCKER_PORT='16379'; ES_DOCKER_PORT='19200'
    MINIO_DOCKER_PORT='19000'; MINIO_CONSOLE_PORT='19001'; KAFKA_CLUSTER_ID=$cluster
    KAFKA_TEST_DATABASE_URL="mysql+pymysql://radio_app:${appPassword}@127.0.0.1:13307/$Database"
    KAFKA_TEST_ROOT_DATABASE_URL="mysql+pymysql://root:${rootPassword}@127.0.0.1:13307/$Database"
    RADIO_DOCKER_DATABASE_URL="mysql+pymysql://radio_app:${appPassword}@mysql:3306/$Database"
    RADIO_DOCKER_MIGRATION_DATABASE_URL="mysql+pymysql://radio_admin:${adminPassword}@mysql:3306/$Database"
    ELASTIC_PASSWORD=$elasticPassword; SEARCH_ES_URL='http://127.0.0.1:19200'; SEARCH_ES_INDEX='radio-content-v1'
    RADIO_DOCKER_SEARCH_ES_URL='http://elasticsearch:9200'; RADIO_DOCKER_MINIO_ENDPOINT='http://host.docker.internal:19000'
    SEARCH_ES_READ_USER='radio_search'; SEARCH_ES_READ_PASSWORD=$readPassword
    SEARCH_ES_WRITE_USER='radio_indexer'; SEARCH_ES_WRITE_PASSWORD=$indexPassword
    SEARCH_ES_ADMIN_USER='elastic'; SEARCH_ES_ADMIN_PASSWORD=$elasticPassword
    KAFKA_TEST_ES_URL='http://127.0.0.1:19200'; KAFKA_TEST_REDIS_URL='redis://127.0.0.1:16379/0'
    KAFKA_TEST_BOOTSTRAP_SERVERS='127.0.0.1:19092'
    KAFKA_BOOTSTRAP_SERVERS='127.0.0.1:19092'
    MINIO_ROOT_USER='radio_media_admin'; MINIO_ROOT_PASSWORD=$minioPassword
    MINIO_ACCESS_KEY='radio_media_admin'; MINIO_SECRET_KEY=$minioPassword
    APP_SECRET_KEY=(New-Secret)
    METRICS_BEARER_TOKEN=(New-Secret); GRAFANA_ADMIN_PASSWORD=(New-Secret)
}
$lines = $values.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }
[IO.File]::WriteAllLines($target, $lines, [Text.UTF8Encoding]::new($false))
Write-Output 'Created ACL-protected Docker environment; credentials were not printed.'
