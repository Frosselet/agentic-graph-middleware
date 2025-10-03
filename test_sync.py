"""Test Fuseki to KuzuDB synchronization"""

import sys
from pathlib import Path

# Add graph submodule to path
sys.path.insert(0, str(Path(__file__).parent))

import logging
from src.agentic_graph_middleware.sync import FusekiKuzuSync

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    print("=" * 70)
    print("🔄 Fuseki to KuzuDB Synchronization Test")
    print("=" * 70)
    print()

    # Initialize sync
    sync = FusekiKuzuSync(
        fuseki_url="http://localhost:3030",
        fuseki_dataset="ontologies",
        kuzu_db_path="./test_sync.kuzu"
    )

    try:
        # Get Fuseki stats
        print("📊 Fuseki Statistics:")
        fuseki_stats = sync.get_fuseki_stats()
        for key, value in fuseki_stats.items():
            print(f"  {key}: {value:,}")
        print()

        # Sync all triples
        stats = sync.sync_all_triples(batch_size=5000)

        # Get KuzuDB stats
        print()
        print("📊 KuzuDB Statistics:")
        kuzu_stats = sync.get_kuzu_stats()
        for key, value in kuzu_stats.items():
            print(f"  {key}: {value:,}")
        print()

        # Example queries
        print("🔍 Sample Queries:")
        print()

        # Query 1: Get ontology namespaces
        print("1. Ontology Namespaces:")
        result = sync.query_graph("""
            MATCH (r:Resource)
            WHERE r.namespace <> ''
            RETURN DISTINCT r.namespace as namespace
            LIMIT 10
        """)
        for row in result:
            print(f"  - {row[0]}")
        print()

        # Query 2: Get resource types
        print("2. Resource Types (sample):")
        result = sync.query_graph("""
            MATCH (s:Resource)-[t:Triple {predicate: 'type'}]->(o:Resource)
            RETURN s.uri, o.uri
            LIMIT 10
        """)
        for row in result:
            print(f"  {row[0]} → {row[1]}")
        print()

        # Query 3: Get predicate distribution
        print("3. Top 10 Predicates:")
        result = sync.query_graph("""
            MATCH ()-[t:Triple]->()
            RETURN t.predicate, COUNT(*) as count
            ORDER BY count DESC
            LIMIT 10
        """)
        for row in result:
            print(f"  {row[0]}: {row[1]:,}")
        print()

        print("=" * 70)
        print("✅ Synchronization test completed successfully!")
        print("=" * 70)

    finally:
        sync.close()


if __name__ == "__main__":
    main()
