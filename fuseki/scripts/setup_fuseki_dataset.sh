#!/bin/bash
# Properly set up Fuseki TDB2 dataset with persistent storage

set -e

FUSEKI_URL="http://localhost:3030"
DATASET="ontologies"

echo "🔧 Setting up Fuseki persistent dataset..."
echo ""

# Create TDB2 dataset with persistent storage using the correct API
echo "📦 Creating persistent TDB2 dataset..."
curl -X POST "$FUSEKI_URL/$/datasets" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data "dbName=$DATASET&dbType=tdb2" \
  -w "\nHTTP Status: %{http_code}\n"

echo ""
echo "✅ Dataset '$DATASET' created"
echo ""

# Verify dataset exists
echo "🔍 Verifying dataset..."
curl -s "$FUSEKI_URL/$/datasets" | grep -q "$DATASET" && echo "✅ Dataset verified" || echo "⚠️  Dataset not found"
echo ""

echo "📊 Dataset info:"
curl -s "$FUSEKI_URL/$/datasets/$DATASET"
echo ""
