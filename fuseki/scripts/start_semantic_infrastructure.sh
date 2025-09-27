#!/bin/bash

# Start Semantic Infrastructure for Agentic Data Scraper
# This script starts the complete Gist-DBC-SOW-Contract semantic infrastructure

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "🚀 Starting Agentic Data Scraper Semantic Infrastructure"
echo "Project root: $PROJECT_ROOT"
echo "=" * 60

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

echo "✅ Docker is running"

# Change to project root
cd "$PROJECT_ROOT"

# Start the semantic infrastructure
echo "🔄 Starting semantic services..."
docker-compose -f docker-compose.semantic.yml up -d

# Wait for Fuseki to be ready
echo "⏳ Waiting for Fuseki to be ready..."
timeout=60
counter=0

until curl -s http://localhost:3030/$/ping >/dev/null 2>&1; do
    if [ $counter -ge $timeout ]; then
        echo "❌ Timeout waiting for Fuseki to start"
        echo "Check logs with: docker-compose -f docker-compose.semantic.yml logs fuseki"
        exit 1
    fi

    echo "  Waiting... ($counter/$timeout)"
    sleep 2
    counter=$((counter + 2))
done

echo "✅ Fuseki is ready!"

# Run validation
echo "🔍 Running semantic validation..."
if python3 scripts/semantic_validation.py; then
    echo ""
    echo "🎉 Semantic infrastructure is ready!"
    echo ""
    echo "📋 Access Points:"
    echo "  • Fuseki Web UI:      http://localhost:3030"
    echo "  • SPARQL Notebook:    http://localhost:8888"
    echo "  • Main SPARQL:        http://localhost:3030/gist-dbc-sow/sparql"
    echo "  • Ontologies SPARQL:  http://localhost:3030/ontologies/sparql"
    echo ""
    echo "🔧 Useful Commands:"
    echo "  • View logs:          docker-compose -f docker-compose.semantic.yml logs -f"
    echo "  • Stop services:      docker-compose -f docker-compose.semantic.yml down"
    echo "  • Restart Fuseki:     docker-compose -f docker-compose.semantic.yml restart fuseki"
    echo "  • Re-run validation:  python3 scripts/semantic_validation.py"
    echo ""
    echo "📖 Documentation: docs/semantic/SEMANTIC_INFRASTRUCTURE_SETUP.md"
else
    echo ""
    echo "⚠️  Semantic validation failed. Check the output above for issues."
    echo ""
    echo "🔧 Troubleshooting:"
    echo "  • Check Fuseki logs:  docker-compose -f docker-compose.semantic.yml logs fuseki"
    echo "  • Check loader logs:  docker-compose -f docker-compose.semantic.yml logs jena-load"
    echo "  • Restart services:   docker-compose -f docker-compose.semantic.yml restart"
    echo ""
    exit 1
fi