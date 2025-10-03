#!/bin/bash
# Load ontologies with authentication

cd /Volumes/WD\ Green/dev/git/agentic-data-scraper

echo "Loading GIST Core..."
curl -u admin:admin -X POST 'http://localhost:3030/ontologies/data?graph=http://ontologies.semanticarts.com/gist' \
  -H "Content-Type: text/turtle" \
  --data-binary @ontologies/ontologies/core/gist-core.ttl
echo "✅ GIST loaded"

echo "Loading GIST-DBC Bridge..."
curl -u admin:admin -X POST 'http://localhost:3030/ontologies/data?graph=http://agentic.local/ontologies/bridge/gist-dbc' \
  -H "Content-Type: application/rdf+xml" \
  --data-binary @ontologies/ontologies/bridge/gist_dbc_bridge.owl
echo "✅ Bridge loaded"

echo "Loading SOW Complete..."
curl -u admin:admin -X POST 'http://localhost:3030/ontologies/data?graph=http://agentic.local/ontologies/sow/complete' \
  -H "Content-Type: application/rdf+xml" \
  --data-binary @ontologies/ontologies/sow/complete_sow_ontology.owl
echo "✅ SOW Complete loaded"

echo "Loading SOW Inference..."
curl -u admin:admin -X POST 'http://localhost:3030/ontologies/data?graph=http://agentic.local/ontologies/sow/inference' \
  -H "Content-Type: application/rdf+xml" \
  --data-binary @ontologies/ontologies/sow/sow_inference_rules.owl
echo "✅ SOW Inference loaded"

echo ""
echo "Checking total triples..."
curl -s -u admin:admin -G 'http://localhost:3030/ontologies/sparql' \
  --data-urlencode 'query=SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }' \
  -H "Accept: application/sparql-results+json"
