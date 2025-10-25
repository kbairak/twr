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

echo "Refreshing materialized views..."

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -v ON_ERROR_STOP=1 <<EOF
REFRESH MATERIALIZED VIEW user_product_performance_realtime;
EOF

echo "✓ Materialized views refreshed successfully!"
