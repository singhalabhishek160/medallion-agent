# Stop PostgreSQL
$PG_BIN = "$PSScriptRoot\pgsql\bin"
$PG_DATA = "$PSScriptRoot\pgdata"

& "$PG_BIN\pg_ctl.exe" stop -D $PG_DATA -w
Write-Host "PostgreSQL stopped." -ForegroundColor Green
