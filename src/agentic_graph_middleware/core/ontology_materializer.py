"""
Ontology Materialization Engine for KuzuDB

Pure ontology-focused graph database layer that materializes OWL/RDF ontologies
into KuzuDB for high-performance graph operations and visualization.
"""

import kuzu
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL, SKOS

logger = logging.getLogger(__name__)


@dataclass
class OntologyNode:
    """Represents an ontology concept node in KuzuDB"""
    uri: str
    label: str
    node_type: str  # 'class', 'property', 'individual'
    description: Optional[str] = None
    namespace: Optional[str] = None


@dataclass
class OntologyRelationship:
    """Represents an ontology relationship in KuzuDB"""
    subject_uri: str
    predicate_uri: str
    object_uri: str
    relationship_type: str


class OntologyMaterializer:
    """
    Materializes OWL/RDF ontologies into KuzuDB graph structure
    for high-performance semantic operations and visualization
    """

    def __init__(self, kuzu_db_path: str):
        self.db = kuzu.Database(kuzu_db_path)
        self.conn = kuzu.Connection(self.db)
        self._initialize_ontology_schema()

    def _initialize_ontology_schema(self):
        """Initialize KuzuDB schema for pure ontology storage"""

        # Create node table for ontology concepts
        self.conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS OntologyConcept(
                uri STRING,
                label STRING,
                concept_type STRING,
                description STRING,
                namespace STRING,
                created_at TIMESTAMP,
                PRIMARY KEY (uri)
            )
        """)

        # Create relationship table for ontology relationships
        self.conn.execute("""
            CREATE REL TABLE IF NOT EXISTS OntologyRelationship(
                FROM OntologyConcept TO OntologyConcept,
                predicate_uri STRING,
                relationship_type STRING,
                created_at TIMESTAMP
            )
        """)

        logger.info("KuzuDB ontology schema initialized")

    def materialize_rdf_graph(self, rdf_graph: Graph) -> Dict[str, int]:
        """
        Materialize an RDF graph into KuzuDB
        Returns statistics about materialized nodes and relationships
        """

        nodes_created = 0
        relationships_created = 0

        # Extract and materialize ontology concepts (nodes)
        for subj, pred, obj in rdf_graph:
            if pred == RDF.type:
                # This is a class/property/individual declaration
                concept_type = self._determine_concept_type(obj)
                label = self._extract_label(rdf_graph, subj)
                description = self._extract_description(rdf_graph, subj)
                namespace = self._extract_namespace(subj)

                self._create_ontology_concept(
                    uri=str(subj),
                    label=label,
                    concept_type=concept_type,
                    description=description,
                    namespace=namespace
                )
                nodes_created += 1

        # Extract and materialize relationships
        for subj, pred, obj in rdf_graph:
            if pred != RDF.type:  # Skip type declarations
                relationship_type = self._determine_relationship_type(pred)

                self._create_ontology_relationship(
                    subject_uri=str(subj),
                    predicate_uri=str(pred),
                    object_uri=str(obj),
                    relationship_type=relationship_type
                )
                relationships_created += 1

        return {
            "nodes_created": nodes_created,
            "relationships_created": relationships_created
        }

    def _create_ontology_concept(self, uri: str, label: str, concept_type: str,
                                 description: Optional[str], namespace: Optional[str]):
        """Create an ontology concept node in KuzuDB"""

        self.conn.execute("""
            CREATE (c:OntologyConcept {
                uri: $uri,
                label: $label,
                concept_type: $concept_type,
                description: $description,
                namespace: $namespace,
                created_at: $timestamp
            })
        """, {
            "uri": uri,
            "label": label,
            "concept_type": concept_type,
            "description": description or "",
            "namespace": namespace or "",
            "timestamp": datetime.now()
        })

    def _create_ontology_relationship(self, subject_uri: str, predicate_uri: str,
                                      object_uri: str, relationship_type: str):
        """Create an ontology relationship in KuzuDB"""

        self.conn.execute("""
            MATCH (s:OntologyConcept {uri: $subject_uri})
            MATCH (o:OntologyConcept {uri: $object_uri})
            CREATE (s)-[:OntologyRelationship {
                predicate_uri: $predicate_uri,
                relationship_type: $relationship_type,
                created_at: $timestamp
            }]->(o)
        """, {
            "subject_uri": subject_uri,
            "object_uri": object_uri,
            "predicate_uri": predicate_uri,
            "relationship_type": relationship_type,
            "timestamp": datetime.now()
        })

    def _determine_concept_type(self, rdf_type: URIRef) -> str:
        """Determine the type of ontology concept"""
        type_str = str(rdf_type)

        if "Class" in type_str:
            return "class"
        elif "Property" in type_str:
            return "property"
        elif "Individual" in type_str:
            return "individual"
        else:
            return "concept"

    def _determine_relationship_type(self, predicate: URIRef) -> str:
        """Determine the type of ontology relationship"""
        pred_str = str(predicate)

        if "subClassOf" in pred_str:
            return "subclass"
        elif "subPropertyOf" in pred_str:
            return "subproperty"
        elif "domain" in pred_str:
            return "domain"
        elif "range" in pred_str:
            return "range"
        elif "inverseOf" in pred_str:
            return "inverse"
        elif "seeAlso" in pred_str:
            return "reference"
        else:
            return "semantic_relation"

    def _extract_label(self, graph: Graph, uri: URIRef) -> str:
        """Extract human-readable label from RDF graph"""
        for label in graph.objects(uri, RDFS.label):
            return str(label)
        for label in graph.objects(uri, SKOS.prefLabel):
            return str(label)
        # Fallback to local name
        return uri.split("#")[-1].split("/")[-1]

    def _extract_description(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract description from RDF graph"""
        for desc in graph.objects(uri, RDFS.comment):
            return str(desc)
        for desc in graph.objects(uri, SKOS.definition):
            return str(desc)
        return None

    def _extract_namespace(self, uri: URIRef) -> str:
        """Extract namespace from URI"""
        uri_str = str(uri)
        if "#" in uri_str:
            return uri_str.split("#")[0] + "#"
        else:
            return "/".join(uri_str.split("/")[:-1]) + "/"

    def query_ontology(self, cypher_query: str, parameters: Dict[str, Any] = None) -> List[Dict]:
        """Execute Cypher query on materialized ontology"""

        result = self.conn.execute(cypher_query, parameters or {})
        return [dict(record) for record in result]

    def get_ontology_statistics(self) -> Dict[str, int]:
        """Get statistics about materialized ontology"""

        # Count concepts
        concept_count = self.conn.execute("MATCH (c:OntologyConcept) RETURN count(c) as count")
        concept_count = list(concept_count)[0]["count"]

        # Count relationships
        rel_count = self.conn.execute("MATCH ()-[r:OntologyRelationship]->() RETURN count(r) as count")
        rel_count = list(rel_count)[0]["count"]

        # Count by concept type
        type_counts = {}
        type_result = self.conn.execute("""
            MATCH (c:OntologyConcept)
            RETURN c.concept_type as type, count(c) as count
        """)
        for record in type_result:
            type_counts[record["type"]] = record["count"]

        return {
            "total_concepts": concept_count,
            "total_relationships": rel_count,
            "concept_types": type_counts
        }


def load_ontology_from_file(file_path: str) -> Graph:
    """Load RDF/OWL ontology from file"""

    graph = Graph()
    graph.parse(file_path)
    return graph