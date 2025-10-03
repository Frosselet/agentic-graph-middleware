"""
Fuseki to KuzuDB Synchronization Layer
Syncs RDF triples from Jena Fuseki into KuzuDB property graph
"""

import kuzu
import httpx
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class SyncStats:
    """Statistics from sync operation"""
    triples_fetched: int = 0
    nodes_created: int = 0
    relationships_created: int = 0
    errors: int = 0
    duration_seconds: float = 0.0


class FusekiKuzuSync:
    """Synchronize RDF triples from Fuseki to KuzuDB property graph"""

    def __init__(
        self,
        fuseki_url: str = "http://localhost:3030",
        fuseki_dataset: str = "ontologies",
        kuzu_db_path: str = "./semantic_graph.kuzu"
    ):
        self.fuseki_url = fuseki_url.rstrip('/')
        self.fuseki_dataset = fuseki_dataset
        self.kuzu_db_path = Path(kuzu_db_path)

        # Initialize KuzuDB
        self.db = kuzu.Database(str(self.kuzu_db_path))
        self.conn = kuzu.Connection(self.db)

        self._initialize_schema()

    def _initialize_schema(self):
        """Create KuzuDB schema for RDF data"""
        logger.info("Initializing KuzuDB schema for RDF triples...")

        # Create Resource node table (subjects and objects)
        self.conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS Resource(
                uri STRING PRIMARY KEY,
                label STRING,
                type STRING,
                namespace STRING,
                local_name STRING,
                is_literal BOOLEAN DEFAULT false,
                datatype STRING,
                language STRING
            )
        """)

        # Create Triple relationship table
        self.conn.execute("""
            CREATE REL TABLE IF NOT EXISTS Triple(
                FROM Resource TO Resource,
                predicate STRING,
                predicate_uri STRING,
                graph_uri STRING
            )
        """)

        # Create indexes for better query performance
        logger.info("✅ KuzuDB schema initialized")

    def _query_fuseki(self, sparql_query: str) -> List[Dict[str, Any]]:
        """Execute SPARQL query against Fuseki"""
        endpoint = f"{self.fuseki_url}/{self.fuseki_dataset}/sparql"

        with httpx.Client(timeout=60.0) as client:
            response = client.get(
                endpoint,
                params={'query': sparql_query},
                headers={'Accept': 'application/sparql-results+json'}
            )
            response.raise_for_status()

            results = response.json()
            return results.get('results', {}).get('bindings', [])

    def get_fuseki_stats(self) -> Dict[str, int]:
        """Get statistics from Fuseki"""
        stats = {}

        # Total triples
        query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"
        try:
            results = self._query_fuseki(query)
            if results:
                stats['total_triples'] = int(results[0]['count']['value'])
        except Exception as e:
            logger.error(f"Failed to get triple count: {e}")
            stats['total_triples'] = 0

        # Unique subjects
        query = "SELECT (COUNT(DISTINCT ?s) as ?count) WHERE { ?s ?p ?o }"
        try:
            results = self._query_fuseki(query)
            if results:
                stats['unique_subjects'] = int(results[0]['count']['value'])
        except Exception as e:
            logger.error(f"Failed to get subject count: {e}")
            stats['unique_subjects'] = 0

        # Unique predicates
        query = "SELECT (COUNT(DISTINCT ?p) as ?count) WHERE { ?s ?p ?o }"
        try:
            results = self._query_fuseki(query)
            if results:
                stats['unique_predicates'] = int(results[0]['count']['value'])
        except Exception as e:
            logger.error(f"Failed to get predicate count: {e}")
            stats['unique_predicates'] = 0

        return stats

    def sync_all_triples(self, batch_size: int = 10000) -> SyncStats:
        """Sync all triples from Fuseki to KuzuDB"""
        import time
        start_time = time.time()

        stats = SyncStats()

        logger.info(f"🔄 Starting sync from Fuseki to KuzuDB...")
        logger.info(f"Fuseki: {self.fuseki_url}/{self.fuseki_dataset}")
        logger.info(f"KuzuDB: {self.kuzu_db_path}")

        # Get total count
        fuseki_stats = self.get_fuseki_stats()
        total_triples = fuseki_stats.get('total_triples', 0)
        logger.info(f"📊 Total triples in Fuseki: {total_triples:,}")

        if total_triples == 0:
            logger.warning("⚠️  No triples found in Fuseki!")
            return stats

        # Fetch all triples in batches
        offset = 0
        while True:
            logger.info(f"📦 Fetching batch at offset {offset:,}...")

            query = f"""
            SELECT ?s ?p ?o ?g
            WHERE {{
                GRAPH ?g {{ ?s ?p ?o }}
            }}
            LIMIT {batch_size}
            OFFSET {offset}
            """

            try:
                results = self._query_fuseki(query)
                if not results:
                    break

                stats.triples_fetched += len(results)

                # Process batch
                self._process_triple_batch(results, stats)

                logger.info(f"  ✅ Processed {len(results)} triples (total: {stats.triples_fetched:,}/{total_triples:,})")

                if len(results) < batch_size:
                    break

                offset += batch_size

            except Exception as e:
                logger.error(f"❌ Error processing batch at offset {offset}: {e}")
                stats.errors += 1
                break

        stats.duration_seconds = time.time() - start_time

        logger.info(f"""
🎉 Sync completed!
  📊 Triples fetched: {stats.triples_fetched:,}
  🔵 Nodes created: {stats.nodes_created:,}
  ➡️  Relationships created: {stats.relationships_created:,}
  ⏱️  Duration: {stats.duration_seconds:.2f}s
  ❌ Errors: {stats.errors}
        """)

        return stats

    def _process_triple_batch(self, triples: List[Dict], stats: SyncStats):
        """Process a batch of triples into KuzuDB"""
        for triple in triples:
            try:
                subject = triple['s']['value']
                predicate = triple['p']['value']
                obj_data = triple['o']
                graph = triple.get('g', {}).get('value', 'default')

                # Create/update subject node
                self._upsert_resource(subject, is_literal=False)
                stats.nodes_created += 1

                # Create/update object node
                is_literal = obj_data['type'] == 'literal'
                datatype = obj_data.get('datatype')
                language = obj_data.get('xml:lang')

                self._upsert_resource(
                    obj_data['value'],
                    is_literal=is_literal,
                    datatype=datatype,
                    language=language
                )
                stats.nodes_created += 1

                # Create relationship
                self._create_triple(subject, predicate, obj_data['value'], graph)
                stats.relationships_created += 1

            except Exception as e:
                logger.error(f"Error processing triple: {e}")
                stats.errors += 1

    def _upsert_resource(
        self,
        uri: str,
        is_literal: bool = False,
        datatype: Optional[str] = None,
        language: Optional[str] = None
    ):
        """Insert or update a resource node"""
        # Extract namespace and local name
        if '#' in uri:
            namespace, local_name = uri.rsplit('#', 1)
        elif '/' in uri and not is_literal:
            namespace, local_name = uri.rsplit('/', 1)
        else:
            namespace = ""
            local_name = uri

        self.conn.execute("""
            MERGE (r:Resource {uri: $uri})
            ON CREATE SET
                r.label = $local_name,
                r.namespace = $namespace,
                r.local_name = $local_name,
                r.is_literal = $is_literal,
                r.datatype = $datatype,
                r.language = $language
        """, {
            'uri': uri,
            'local_name': local_name,
            'namespace': namespace,
            'is_literal': is_literal,
            'datatype': datatype or "",
            'language': language or ""
        })

    def _create_triple(self, subject: str, predicate: str, obj: str, graph: str):
        """Create a triple relationship"""
        # Extract predicate local name
        if '#' in predicate:
            pred_local = predicate.rsplit('#', 1)[1]
        elif '/' in predicate:
            pred_local = predicate.rsplit('/', 1)[1]
        else:
            pred_local = predicate

        self.conn.execute("""
            MATCH (s:Resource {uri: $subject})
            MATCH (o:Resource {uri: $object})
            MERGE (s)-[t:Triple {predicate_uri: $predicate_uri}]->(o)
            ON CREATE SET
                t.predicate = $predicate,
                t.graph_uri = $graph
        """, {
            'subject': subject,
            'object': obj,
            'predicate': pred_local,
            'predicate_uri': predicate,
            'graph': graph
        })

    def query_graph(self, cypher_query: str) -> List[Tuple]:
        """Execute Cypher query on KuzuDB"""
        result = self.conn.execute(cypher_query)
        return result.get_all()

    def get_kuzu_stats(self) -> Dict[str, int]:
        """Get statistics from KuzuDB"""
        stats = {}

        # Count nodes
        result = self.conn.execute("MATCH (r:Resource) RETURN COUNT(r) as count")
        stats['total_nodes'] = result.get_all()[0][0]

        # Count relationships
        result = self.conn.execute("MATCH ()-[t:Triple]->() RETURN COUNT(t) as count")
        stats['total_relationships'] = result.get_all()[0][0]

        return stats

    def close(self):
        """Close database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()
