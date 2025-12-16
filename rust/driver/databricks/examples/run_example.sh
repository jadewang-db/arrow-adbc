#!/bin/bash
# Helper script to run examples with environment variables loaded
# Usage: ./run_example.sh simple_query
#        ./run_example.sh demo_app
#        ./run_example.sh table_operations

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Check if .env file exists
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo "Error: .env file not found in $SCRIPT_DIR"
    echo "Please create .env from .env.template and configure your credentials"
    exit 1
fi

# Load environment variables
echo "Loading environment variables from .env..."
set -a  # Mark all variables for export
source "$SCRIPT_DIR/.env"
set +a

# Verify required variables are set
if [ -z "$DATABRICKS_HOST" ] || [ -z "$DATABRICKS_WAREHOUSE_ID" ] || [ -z "$DATABRICKS_TOKEN" ]; then
    echo "Error: Required environment variables not set"
    echo "Please check your .env file has:"
    echo "  - DATABRICKS_HOST"
    echo "  - DATABRICKS_WAREHOUSE_ID"
    echo "  - DATABRICKS_TOKEN"
    exit 1
fi

echo "Environment configured:"
echo "  Host: $DATABRICKS_HOST"
echo "  Warehouse ID: $DATABRICKS_WAREHOUSE_ID"
echo "  Token: ${DATABRICKS_TOKEN:0:15}..."
if [ -n "$DATABRICKS_CATALOG" ]; then
    echo "  Catalog: $DATABRICKS_CATALOG"
fi
if [ -n "$DATABRICKS_SCHEMA" ]; then
    echo "  Schema: $DATABRICKS_SCHEMA"
fi
echo ""

# Get example name from argument
EXAMPLE_NAME="${1:-simple_query}"

# Run the example
echo "Running example: $EXAMPLE_NAME"
echo "-----------------------------------"
cd "$PROJECT_ROOT"
cargo run --example "$EXAMPLE_NAME"
