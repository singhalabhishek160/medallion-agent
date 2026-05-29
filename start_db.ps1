# Start PostgreSQL (portable) for the medallion pipeline
$PG_DIR = "$PSScriptRoot\pgsql"
$PG_DATA = "$PSScriptRoot\pgdata"
$PG_BIN = "$PG_DIR\bin"
$PG_PORT = 5432

# Check PostgreSQL binaries exist
if (-not (Test-Path "$PG_BIN\pg_ctl.exe")) {
    Write-Host "ERROR: PostgreSQL not found at $PG_BIN" -ForegroundColor Red
    Write-Host "Run setup first or extract pgsql.zip to $PG_DIR"
    exit 1
}

# Initialize data directory if needed
if (-not (Test-Path "$PG_DATA\PG_VERSION")) {
    Write-Host "Initializing PostgreSQL data directory..."
    & "$PG_BIN\initdb.exe" -D $PG_DATA -U pipeline -E UTF8 --no-locale
    
    # Set password
    & "$PG_BIN\pg_ctl.exe" start -D $PG_DATA -l "$PSScriptRoot\pg.log" -w
    & "$PG_BIN\psql.exe" -U pipeline -d postgres -c "ALTER USER pipeline PASSWORD 'pipeline123';"
    & "$PG_BIN\psql.exe" -U pipeline -d postgres -c "CREATE DATABASE medallion OWNER pipeline;"
    & "$PG_BIN\psql.exe" -U pipeline -d medallion -f "$PSScriptRoot\init.sql"
    & "$PG_BIN\pg_ctl.exe" stop -D $PG_DATA -w
    Write-Host "Database initialized." -ForegroundColor Green
}

# Start server
Write-Host "Starting PostgreSQL on port $PG_PORT..."
& "$PG_BIN\pg_ctl.exe" start -D $PG_DATA -l "$PSScriptRoot\pg.log" -w
Write-Host "PostgreSQL running. Stop with: .\stop_db.ps1" -ForegroundColor Green
