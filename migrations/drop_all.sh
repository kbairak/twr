#!/bin/bash
set -e  # Exit immediately if any command fails

# Database connection parameters
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="twr"
DB_USER="twr_user"
DB_PASS="twr_password"

# Set PGPASSWORD environment variable to avoid password prompt
export PGPASSWORD=$DB_PASS

echo "Dropping and recreating database..."

# Connect to postgres database to drop and recreate twr database
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -v ON_ERROR_STOP=1 <<EOF
-- Terminate any existing connections to the database
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = '$DB_NAME'
  AND pid <> pg_backend_pid();

-- Drop and recreate the database
DROP DATABASE IF EXISTS $DB_NAME;
CREATE DATABASE $DB_NAME OWNER $DB_USER;
EOF

echo "✓ Database dropped and recreated successfully!"
