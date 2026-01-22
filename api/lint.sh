#!/bin/bash
# Lint the API code with ruff
# Usage: ./api/lint.sh

set -e

echo "🔍 Running ruff linter on API code..."

cd "$(dirname "$0")/.."

ruff check api/coeqwal-api/ --exclude="api/coeqwal-api/routes/_archive"

echo "✅ All checks passed!"

