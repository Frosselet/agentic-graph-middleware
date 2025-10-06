#!/bin/bash
## ============================================================
## Load Ontology into Fuseki - Production Script
## ============================================================

set -e  # Exit on error

FUSEKI_URL="${FUSEKI_URL:-http://localhost:3030}"
DATASET="${DATASET:-ontologies}"
FUSEKI_USER="${FUSEKI_USER:-admin}"
FUSEKI_PASSWORD="${FUSEKI_PASSWORD:-admin123}"
ONTOLOGY_FILE="$1"

if [ -z "$ONTOLOGY_FILE" ]; then
    echo "Usage: $0 <ontology-file.owl>"
    echo "Example: $0 ../ontologies/ontologies/bridge/gist_dbc_bridge.owl"
    exit 1
fi

if [ ! -f "$ONTOLOGY_FILE" ]; then
    echo "Error: File not found: $ONTOLOGY_FILE"
    exit 1
fi

echo "============================================================"
echo "Loading Ontology into Fuseki"
echo "============================================================"
echo "Fuseki URL: $FUSEKI_URL"
echo "Dataset:    $DATASET"
echo "File:       $ONTOLOGY_FILE"
echo "============================================================"

# Step 1: Check Fuseki is running
echo ""
echo "Step 1/4: Checking Fuseki health..."
if ! curl -sf "$FUSEKI_URL/\$/ping" > /dev/null; then
    echo "Error: Fuseki is not responding at $FUSEKI_URL"
    exit 1
fi
echo "✓ Fuseki is running"

# Step 2: Clear existing data (optional - comment out to preserve data)
echo ""
echo "Step 2/4: Clearing existing data..."
curl -X POST "$FUSEKI_URL/$DATASET/update" \
    -u "$FUSEKI_USER:$FUSEKI_PASSWORD" \
    -H "Content-Type: application/sparql-update" \
    --data "CLEAR DEFAULT" \
    -w "\nHTTP Status: %{http_code}\n"

# Step 3: Upload ontology file
echo ""
echo "Step 3/4: Uploading ontology..."
RESPONSE=$(curl -s -X POST "$FUSEKI_URL/$DATASET/data?default" \
    -u "$FUSEKI_USER:$FUSEKI_PASSWORD" \
    --data-binary "@$ONTOLOGY_FILE" \
    -H "Content-Type: application/rdf+xml" \
    -w "\nHTTP_STATUS:%{http_code}")

HTTP_STATUS=$(echo "$RESPONSE" | grep "HTTP_STATUS" | cut -d: -f2)
BODY=$(echo "$RESPONSE" | grep -v "HTTP_STATUS")

if [ "$HTTP_STATUS" != "200" ] && [ "$HTTP_STATUS" != "201" ]; then
    echo "Error: Upload failed with status $HTTP_STATUS"
    echo "$BODY"
    exit 1
fi
echo "✓ Upload successful (HTTP $HTTP_STATUS)"

# Step 4: Verify data was loaded
echo ""
echo "Step 4/4: Verifying data..."
TRIPLE_COUNT=$(curl -s "$FUSEKI_URL/$DATASET/sparql" \
    --data-urlencode "query=SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }" \
    -H "Accept: application/sparql-results+json" | \
    python3 -c "import sys, json; data=json.load(sys.stdin); print(data['results']['bindings'][0]['count']['value'])")

if [ "$TRIPLE_COUNT" = "0" ]; then
    echo "✗ Error: No triples found after upload!"
    exit 1
fi

echo "✓ Verification successful: $TRIPLE_COUNT triples loaded"

# Count classes
echo ""
echo "Counting OWL classes..."
CLASS_COUNT=$(curl -s "$FUSEKI_URL/$DATASET/sparql" \
    --data-urlencode "query=PREFIX owl: <http://www.w3.org/2002/07/owl#> SELECT (COUNT(DISTINCT ?c) as ?count) WHERE { ?c a owl:Class }" \
    -H "Accept: application/sparql-results+json" | \
    python3 -c "import sys, json; data=json.load(sys.stdin); print(data['results']['bindings'][0]['count']['value'])")

echo "✓ OWL Classes: $CLASS_COUNT"

# Count properties
echo ""
echo "Counting Object Properties..."
PROP_COUNT=$(curl -s "$FUSEKI_URL/$DATASET/sparql" \
    --data-urlencode "query=PREFIX owl: <http://www.w3.org/2002/07/owl#> SELECT (COUNT(DISTINCT ?p) as ?count) WHERE { ?p a owl:ObjectProperty }" \
    -H "Accept: application/sparql-results+json" | \
    python3 -c "import sys, json; data=json.load(sys.stdin); print(data['results']['bindings'][0]['count']['value'])")

echo "✓ ObjectProperties: $PROP_COUNT"

echo ""
echo "============================================================"
echo "✅ SUCCESS: Ontology loaded and verified"
echo "============================================================"
echo "Total Triples:        $TRIPLE_COUNT"
echo "OWL Classes:          $CLASS_COUNT"
echo "Object Properties:    $PROP_COUNT"
echo "============================================================"
