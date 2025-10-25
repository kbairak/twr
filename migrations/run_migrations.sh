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

echo "Running migrations..."

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run each migration file in order
for sql_file in "$SCRIPT_DIR"/*.sql; do
    if [ -f "$sql_file" ]; then
        echo "Executing $(basename "$sql_file")..."
        # Use -v ON_ERROR_STOP=1 to make psql exit on first error
        if psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -v ON_ERROR_STOP=1 -f "$sql_file"; then
            echo "✓ $(basename "$sql_file") completed successfully"
        else
            echo "✗ $(basename "$sql_file") failed"
            exit 1
        fi
    fi
done

echo "All migrations completed successfully!"
