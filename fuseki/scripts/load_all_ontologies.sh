#!/bin/bash
# Load all ontologies into Fuseki triple store

set -e

FUSEKI_URL="http://localhost:3030"
DATASET="ontologies"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ONTOLOGIES_DIR="$SCRIPT_DIR/../../../ontologies/ontologies"

echo "🔄 Loading ontologies into Fuseki..."
echo "Dataset: $DATASET"
echo ""

# Create dataset if it doesn't exist
echo "📦 Creating dataset '$DATASET'..."
curl -s -X POST "$FUSEKI_URL/$/datasets" \
  --data "dbName=$DATASET&dbType=tdb2" || echo "Dataset may already exist"

echo ""

# Load GIST Core ontology
echo "📚 Loading GIST Core ontology..."
curl -X POST "$FUSEKI_URL/$DATASET/data?graph=http://ontologies.semanticarts.com/gist" \
  -H "Content-Type: text/turtle" \
  --data-binary @"$ONTOLOGIES_DIR/core/gist-core.ttl"
echo "  ✅ GIST Core loaded"

# Load GIST-DBC Bridge
echo "📚 Loading GIST-DBC Bridge ontology..."
curl -X POST "$FUSEKI_URL/$DATASET/data?graph=http://agentic.local/ontologies/bridge/gist-dbc" \
  -H "Content-Type: application/rdf+xml" \
  --data-binary @"$ONTOLOGIES_DIR/bridge/gist_dbc_bridge.owl"
echo "  ✅ GIST-DBC Bridge loaded"

# Load SOW Complete ontology
echo "📚 Loading SOW Complete ontology..."
curl -X POST "$FUSEKI_URL/$DATASET/data?graph=http://agentic.local/ontologies/sow/complete" \
  -H "Content-Type: application/rdf+xml" \
  --data-binary @"$ONTOLOGIES_DIR/sow/complete_sow_ontology.owl"
echo "  ✅ SOW Complete loaded"

# Load SOW Inference Rules
echo "📚 Loading SOW Inference Rules..."
curl -X POST "$FUSEKI_URL/$DATASET/data?graph=http://agentic.local/ontologies/sow/inference" \
  -H "Content-Type: application/rdf+xml" \
  --data-binary @"$ONTOLOGIES_DIR/sow/sow_inference_rules.owl"
echo "  ✅ SOW Inference Rules loaded"

echo ""
echo "🎉 All ontologies loaded successfully!"
echo ""
echo "📊 Get triple count:"
echo "  curl '$FUSEKI_URL/$DATASET/sparql' --data-urlencode 'query=SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }'"
echo ""
echo "🌐 Access Fuseki UI:"
echo "  http://localhost:3030"
