#!/bin/bash

# ============================================================================
# DATABASE AUDIT RUNNER
# ============================================================================
# This script runs the comprehensive database audit
# ============================================================================

echo "🔍 COEQWAL Database Audit Runner"
echo "================================="

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo ""
    echo "❌ DATABASE_URL environment variable is not set!"
    echo ""
    echo "Please set it with your RDS connection string:"
    echo ""
    echo "For bash/zsh:"
    echo 'export DATABASE_URL="postgresql://username:password@your-rds-endpoint:5432/coeqwal_scenario"'
    echo ""
    echo "For example:"
    echo 'export DATABASE_URL="postgresql://postgres:mypassword@coeqwal-scenario-database-1.abc123.us-west-2.rds.amazonaws.com:5432/coeqwal_scenario"'
    echo ""
    echo "Then run this script again."
    exit 1
fi

echo "✅ DATABASE_URL is set"
echo "🔗 Connection: $(echo $DATABASE_URL | sed 's/:[^:]*@/:***@/')"
echo ""

# Check if required Python packages are available
echo "📦 Checking Python dependencies..."

# Check for required packages
python3 -c "import psycopg2, pandas, json" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ Missing required Python packages!"
    echo ""
    echo "Please install them:"
    echo "pip install psycopg2-binary pandas"
    echo ""
    echo "Or if using conda:"
    echo "conda install psycopg2 pandas"
    exit 1
fi

echo "✅ Python dependencies available"
echo ""

# Run the audit
echo "🚀 Starting database audit..."
echo ""

cd "$(dirname "$0")"

python3 utils/audit_database_comprehensive.py

echo ""
echo "✅ Audit complete!"
echo ""
echo "📁 Files created in: $(pwd)"
echo "   - database_audit_YYYYMMDD_HHMMSS.json (detailed report)"
echo "   - database_tables_summary_YYYYMMDD_HHMMSS.csv (tables overview)"
echo ""
echo "🔍 Review the summary above and the detailed files for complete analysis."
