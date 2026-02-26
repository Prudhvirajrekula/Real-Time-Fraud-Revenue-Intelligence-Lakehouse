#!/usr/bin/env bash
# =============================================================================
# Create multiple PostgreSQL databases on initialization
# Called by docker-entrypoint-initdb.d
# =============================================================================
set -e

function create_db() {
    local db=$1
    echo "Creating database: $db"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        SELECT 'CREATE DATABASE $db'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')
        \gexec
        GRANT ALL PRIVILEGES ON DATABASE $db TO $POSTGRES_USER;
EOSQL
}

# Create the analytics DB
create_db "${POSTGRES_DB:-fraud_platform}"

# Create Airflow metadata DB
create_db "${POSTGRES_AIRFLOW_DB:-airflow_metadata}"

echo "Database initialization complete."
