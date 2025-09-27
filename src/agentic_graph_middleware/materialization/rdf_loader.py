"""
RDF/OWL Ontology Loader for KuzuDB Materialization

Loads RDF/OWL ontology files and materializes them into KuzuDB
for high-performance graph operations and visualization.
"""

import logging
from typing import Dict, List, Any, Optional, Set
from pathlib import Path
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, OWL, SKOS, XSD
from datetime import datetime

from ..core.ontology_materializer import OntologyMaterializer
from ..schemas.ontology_schema import ConceptType, RelationshipType

logger = logging.getLogger(__name__)


class RDFLoader:
    """
    Loads RDF/OWL ontologies and materializes them in KuzuDB
    Handles standard ontology formats and namespaces
    """

    def __init__(self, materializer: OntologyMaterializer):
        self.materializer = materializer
        self.processed_uris: Set[str] = set()

    def load_ontology_file(self, file_path: str) -> Dict[str, int]:
        """
        Load ontology from RDF/OWL file
        Returns statistics about loaded concepts and relationships
        """

        if not Path(file_path).exists():
            raise FileNotFoundError(f"Ontology file not found: {file_path}")

        logger.info(f"Loading ontology from {file_path}")

        # Parse RDF graph
        rdf_graph = Graph()
        try:
            rdf_graph.parse(file_path)
        except Exception as e:
            logger.error(f"Failed to parse ontology file {file_path}: {e}")
            raise

        logger.info(f"Parsed {len(rdf_graph)} triples from {file_path}")

        # Materialize into KuzuDB
        return self._materialize_rdf_graph(rdf_graph)

    def load_multiple_ontologies(self, file_paths: List[str]) -> Dict[str, Any]:
        """
        Load multiple ontology files into single KuzuDB instance
        Returns combined statistics
        """

        total_stats = {"total_concepts": 0, "total_relationships": 0, "files_loaded": []}

        for file_path in file_paths:
            try:
                stats = self.load_ontology_file(file_path)
                total_stats["total_concepts"] += stats.get("concepts_created", 0)
                total_stats["total_relationships"] += stats.get("relationships_created", 0)
                total_stats["files_loaded"].append({
                    "file": file_path,
                    "concepts": stats.get("concepts_created", 0),
                    "relationships": stats.get("relationships_created", 0)
                })
            except Exception as e:
                logger.error(f"Failed to load {file_path}: {e}")
                total_stats["files_loaded"].append({
                    "file": file_path,
                    "error": str(e)
                })

        return total_stats

    def _materialize_rdf_graph(self, rdf_graph: Graph) -> Dict[str, int]:
        """Materialize RDF graph into KuzuDB"""

        concepts_created = 0
        relationships_created = 0

        # Phase 1: Extract and create concept nodes
        for subject, predicate, obj in rdf_graph:
            if self._is_concept_declaration(predicate, obj):
                if str(subject) not in self.processed_uris and not isinstance(subject, BNode):
                    concept_data = self._extract_concept_data(rdf_graph, subject, predicate, obj)
                    if concept_data:
                        self.materializer._create_ontology_concept(**concept_data)
                        self.processed_uris.add(str(subject))
                        concepts_created += 1

        # Phase 2: Create relationships between concepts
        for subject, predicate, obj in rdf_graph:
            if not self._is_concept_declaration(predicate, obj) and not isinstance(subject, BNode) and not isinstance(obj, BNode):
                relationship_data = self._extract_relationship_data(subject, predicate, obj)
                if relationship_data and str(obj) in self.processed_uris:
                    try:
                        self.materializer._create_ontology_relationship(**relationship_data)
                        relationships_created += 1
                    except Exception as e:
                        # Skip relationships where target concept doesn't exist
                        logger.debug(f"Skipped relationship {predicate}: {e}")

        return {
            "concepts_created": concepts_created,
            "relationships_created": relationships_created
        }

    def _is_concept_declaration(self, predicate: URIRef, obj: URIRef) -> bool:
        """Check if this triple declares a concept"""
        if predicate != RDF.type:
            return False

        return str(obj) in [
            str(OWL.Class),
            str(OWL.ObjectProperty),
            str(OWL.DatatypeProperty),
            str(OWL.AnnotationProperty),
            str(OWL.NamedIndividual),
            str(RDFS.Class),
            str(RDF.Property),
            str(SKOS.Concept),
            str(SKOS.ConceptScheme)
        ]

    def _extract_concept_data(self, graph: Graph, subject: URIRef, predicate: URIRef, obj: URIRef) -> Optional[Dict[str, Any]]:
        """Extract concept data for KuzuDB storage"""

        uri = str(subject)
        concept_type = self._determine_concept_type(obj)
        label = self._extract_label(graph, subject)
        description = self._extract_description(graph, subject)
        namespace = self._extract_namespace(subject)

        return {
            "uri": uri,
            "label": label,
            "concept_type": concept_type,
            "description": description,
            "namespace": namespace
        }

    def _extract_relationship_data(self, subject: URIRef, predicate: URIRef, obj: URIRef) -> Optional[Dict[str, Any]]:
        """Extract relationship data for KuzuDB storage"""

        # Skip literal objects for now (focusing on concept-to-concept relationships)
        if isinstance(obj, Literal):
            return None

        subject_uri = str(subject)
        object_uri = str(obj)
        predicate_uri = str(predicate)
        relationship_type = self._determine_relationship_type(predicate)

        return {
            "subject_uri": subject_uri,
            "predicate_uri": predicate_uri,
            "object_uri": object_uri,
            "relationship_type": relationship_type
        }

    def _determine_concept_type(self, rdf_type: URIRef) -> str:
        """Determine concept type from RDF type"""
        type_str = str(rdf_type)

        if str(OWL.Class) in type_str or str(RDFS.Class) in type_str:
            return ConceptType.CLASS.value
        elif "Property" in type_str:
            return ConceptType.PROPERTY.value
        elif str(OWL.NamedIndividual) in type_str:
            return ConceptType.INDIVIDUAL.value
        else:
            return ConceptType.CONCEPT.value

    def _determine_relationship_type(self, predicate: URIRef) -> str:
        """Determine relationship type from predicate"""
        pred_str = str(predicate)

        if str(RDFS.subClassOf) in pred_str:
            return RelationshipType.SUBCLASS.value
        elif str(RDFS.subPropertyOf) in pred_str:
            return RelationshipType.SUBPROPERTY.value
        elif str(RDFS.domain) in pred_str:
            return RelationshipType.DOMAIN.value
        elif str(RDFS.range) in pred_str:
            return RelationshipType.RANGE.value
        elif str(OWL.inverseOf) in pred_str:
            return RelationshipType.INVERSE.value
        elif str(RDFS.seeAlso) in pred_str:
            return RelationshipType.REFERENCE.value
        else:
            return RelationshipType.SEMANTIC_RELATION.value

    def _extract_label(self, graph: Graph, uri: URIRef) -> str:
        """Extract human-readable label"""
        for label in graph.objects(uri, RDFS.label):
            return str(label)
        for label in graph.objects(uri, SKOS.prefLabel):
            return str(label)
        # Fallback to local name
        return self._get_local_name(uri)

    def _extract_description(self, graph: Graph, uri: URIRef) -> Optional[str]:
        """Extract description/comment"""
        for desc in graph.objects(uri, RDFS.comment):
            return str(desc)
        for desc in graph.objects(uri, SKOS.definition):
            return str(desc)
        for desc in graph.objects(uri, SKOS.scopeNote):
            return str(desc)
        return None

    def _extract_namespace(self, uri: URIRef) -> str:
        """Extract namespace from URI"""
        uri_str = str(uri)
        if "#" in uri_str:
            return uri_str.split("#")[0] + "#"
        else:
            return "/".join(uri_str.split("/")[:-1]) + "/"

    def _get_local_name(self, uri: URIRef) -> str:
        """Get local name from URI"""
        uri_str = str(uri)
        if "#" in uri_str:
            return uri_str.split("#")[-1]
        else:
            return uri_str.split("/")[-1]