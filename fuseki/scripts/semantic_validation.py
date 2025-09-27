#!/usr/bin/env python3
"""
Semantic Validation Script for Gist-DBC-SOW-Contract Connected Graph

This script validates the semantic connectivity across all 4 levels:
1. Gist Upper Ontology
2. Data Business Canvas Bridge
3. SOW Contracts
4. Data Contracts

Usage:
    python semantic_validation.py [--fuseki-url http://localhost:3030]
"""

import requests
import json
import time
import argparse
from pathlib import Path
from typing import Dict, List, Any
import sys

class SemanticValidator:
    def __init__(self, fuseki_url: str = "http://localhost:3030"):
        self.fuseki_url = fuseki_url.rstrip('/')
        self.datasets = {
            'ontologies': f"{self.fuseki_url}/ontologies",
            'gist-dbc-sow': f"{self.fuseki_url}/gist-dbc-sow",
            'test-data': f"{self.fuseki_url}/test-data"
        }

    def check_fuseki_health(self) -> bool:
        """Check if Fuseki is running and accessible"""
        try:
            response = requests.get(f"{self.fuseki_url}/$/ping", timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def load_test_data(self) -> bool:
        """Load minimal test instances for validation"""
        test_file = Path(__file__).parent.parent / "schemas/test-data/minimal_semantic_validation.ttl"

        if not test_file.exists():
            print(f"❌ Test data file not found: {test_file}")
            return False

        try:
            with open(test_file, 'r', encoding='utf-8') as f:
                test_data = f.read()

            # Load into test-data dataset
            response = requests.post(
                f"{self.datasets['test-data']}/data",
                data=test_data,
                headers={'Content-Type': 'text/turtle'},
                timeout=30
            )

            if response.status_code in [200, 201]:
                print("✅ Test data loaded successfully")
                return True
            else:
                print(f"❌ Failed to load test data: {response.status_code}")
                print(response.text)
                return False

        except Exception as e:
            print(f"❌ Error loading test data: {e}")
            return False

    def run_sparql_query(self, query: str, dataset: str = 'gist-dbc-sow') -> Dict[str, Any]:
        """Run a SPARQL query against the specified dataset"""
        try:
            response = requests.get(
                f"{self.datasets[dataset]}/sparql",
                params={
                    'query': query,
                    'format': 'json'
                },
                headers={'Accept': 'application/sparql-results+json'},
                timeout=30
            )

            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ Query failed with status {response.status_code}")
                print(f"Query: {query[:100]}...")
                print(f"Response: {response.text}")
                return {'results': {'bindings': []}}

        except Exception as e:
            print(f"❌ Error running query: {e}")
            return {'results': {'bindings': []}}

    def count_triples(self, dataset: str = 'gist-dbc-sow') -> int:
        """Count total triples in dataset"""
        query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"
        result = self.run_sparql_query(query, dataset)

        if result['results']['bindings']:
            return int(result['results']['bindings'][0]['count']['value'])
        return 0

    def validate_ontology_imports(self, dataset: str = 'ontologies') -> bool:
        """Validate that all ontologies are properly loaded and linked"""
        print("\n🔍 Validating Ontology Imports...")

        # Check for each ontology namespace
        namespaces = {
            'Gist': 'https://w3id.org/semanticarts/ontology/gistCore#',
            'DBC Bridge': 'https://agentic-data-scraper.com/ontology/gist-dbc-bridge#',
            'SOW': 'https://agentic-data-scraper.com/ontology/sow#',
            'Complete SOW': 'https://agentic-data-scraper.com/ontology/complete-sow#'
        }

        all_valid = True

        for name, namespace in namespaces.items():
            query = f"""
            SELECT (COUNT(?class) as ?count) WHERE {{
                ?class a owl:Class .
                FILTER(STRSTARTS(STR(?class), "{namespace}"))
            }}
            """

            result = self.run_sparql_query(query, dataset)
            if result['results']['bindings']:
                count = int(result['results']['bindings'][0]['count']['value'])
                if count > 0:
                    print(f"  ✅ {name}: {count} classes found")
                else:
                    print(f"  ❌ {name}: No classes found")
                    all_valid = False
            else:
                print(f"  ❌ {name}: Query failed")
                all_valid = False

        return all_valid

    def validate_inheritance_chain(self, dataset: str = 'ontologies') -> bool:
        """Validate that our classes properly extend Gist classes"""
        print("\n🔗 Validating Inheritance Chain...")

        query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?subclass ?superclass WHERE {
            ?subclass rdfs:subClassOf ?superclass .
            FILTER(
                STRSTARTS(STR(?subclass), "https://agentic-data-scraper.com/ontology/") &&
                STRSTARTS(STR(?superclass), "https://w3id.org/semanticarts/ontology/gistCore#")
            )
        }
        ORDER BY ?subclass
        """

        result = self.run_sparql_query(query, dataset)
        bindings = result['results']['bindings']

        if bindings:
            print(f"  ✅ Found {len(bindings)} inheritance relationships:")
            for binding in bindings[:10]:  # Show first 10
                subclass = binding['subclass']['value'].split('#')[-1]
                superclass = binding['superclass']['value'].split('#')[-1]
                print(f"    {subclass} → gist:{superclass}")

            if len(bindings) > 10:
                print(f"    ... and {len(bindings) - 10} more")

            return True
        else:
            print("  ❌ No inheritance relationships found")
            return False

    def validate_cross_level_connectivity(self, dataset: str = 'test-data') -> bool:
        """Validate that instances can be connected across all 4 levels"""
        print("\n🌉 Validating Cross-Level Connectivity...")

        query = """
        PREFIX gist: <https://w3id.org/semanticarts/ontology/gistCore#>
        PREFIX bridge: <https://agentic-data-scraper.com/ontology/gist-dbc-bridge#>
        PREFIX csow: <https://agentic-data-scraper.com/ontology/complete-sow#>

        SELECT ?org ?canvas ?sow ?contract ?task WHERE {
            ?org a gist:Organization .
            ?org bridge:hasBusinessModel ?canvas .
            ?canvas a bridge:DataBusinessCanvas .
            ?canvas bridge:implementedBySOW ?sow .
            ?sow a csow:SemanticStatementOfWork .
            ?sow bridge:realizesContract ?contract .
            ?contract a bridge:DataContract .
            ?contract bridge:executedByTask ?task .
            ?task a bridge:DataProcessingTask .
        }
        """

        result = self.run_sparql_query(query, dataset)
        bindings = result['results']['bindings']

        if bindings:
            print(f"  ✅ Found {len(bindings)} complete 4-level connection(s)")
            for binding in bindings:
                org = binding['org']['value'].split('#')[-1]
                task = binding['task']['value'].split('#')[-1]
                print(f"    {org} → ... → {task}")
            return True
        else:
            print("  ❌ No complete 4-level connections found")
            return False

    def validate_value_chain(self, dataset: str = 'test-data') -> bool:
        """Validate business value creation chain"""
        print("\n💰 Validating Value Creation Chain...")

        query = """
        PREFIX bridge: <https://agentic-data-scraper.com/ontology/gist-dbc-bridge#>
        PREFIX gist: <https://w3id.org/semanticarts/ontology/gistCore#>

        SELECT ?task ?value ?target ?owner WHERE {
            ?task a bridge:DataProcessingTask .
            ?task bridge:createsBusinessValue ?value .
            ?value a bridge:ValueProposition .

            OPTIONAL {
                ?canvas bridge:alignsWithTarget ?target .
                ?target a bridge:ExecutiveTarget .
                ?target bridge:ownedBy ?owner .
                ?owner a gist:Person .
            }
        }
        """

        result = self.run_sparql_query(query, dataset)
        bindings = result['results']['bindings']

        if bindings:
            print(f"  ✅ Found {len(bindings)} value creation relationship(s)")
            for binding in bindings:
                task = binding['task']['value'].split('#')[-1]
                value = binding['value']['value'].split('#')[-1]
                print(f"    {task} → {value}")
            return True
        else:
            print("  ❌ No value creation relationships found")
            return False

    def run_comprehensive_validation(self) -> bool:
        """Run all validation tests"""
        print("🚀 Starting Comprehensive Semantic Validation")
        print("=" * 60)

        # 1. Health check
        if not self.check_fuseki_health():
            print("❌ Fuseki is not accessible. Make sure it's running.")
            return False

        print("✅ Fuseki is accessible")

        # 2. Load test data
        if not self.load_test_data():
            print("❌ Failed to load test data")
            return False

        # 3. Show dataset statistics
        print(f"\n📊 Dataset Statistics:")
        for name, url in self.datasets.items():
            count = self.count_triples(name)
            print(f"  {name}: {count:,} triples")

        # 4. Validate each component
        validations = [
            ('Ontology Imports', lambda: self.validate_ontology_imports()),
            ('Inheritance Chain', lambda: self.validate_inheritance_chain()),
            ('Cross-Level Connectivity', lambda: self.validate_cross_level_connectivity()),
            ('Value Chain', lambda: self.validate_value_chain())
        ]

        results = []
        for name, validation_func in validations:
            try:
                result = validation_func()
                results.append(result)
            except Exception as e:
                print(f"❌ Error in {name}: {e}")
                results.append(False)

        # 5. Summary
        print("\n" + "=" * 60)
        print("🏁 Validation Summary:")

        passed = sum(results)
        total = len(results)

        for i, (name, _) in enumerate(validations):
            status = "✅ PASS" if results[i] else "❌ FAIL"
            print(f"  {status} {name}")

        print(f"\nOverall: {passed}/{total} validations passed")

        if passed == total:
            print("🎉 All semantic validations PASSED! The 4-level connected graph is working correctly.")
            return True
        else:
            print("⚠️  Some validations FAILED. Check the ontology setup.")
            return False

def main():
    parser = argparse.ArgumentParser(description='Validate semantic connectivity')
    parser.add_argument('--fuseki-url', default='http://localhost:3030',
                       help='Fuseki server URL (default: http://localhost:3030)')

    args = parser.parse_args()

    validator = SemanticValidator(args.fuseki_url)
    success = validator.run_comprehensive_validation()

    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()