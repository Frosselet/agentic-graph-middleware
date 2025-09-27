"""
Pure Ontology Schema for KuzuDB

Defines the canonical schema for storing and querying ontology concepts
and relationships in KuzuDB without any domain-specific use cases.
"""

from enum import Enum
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime


class ConceptType(Enum):
    """Standard ontology concept types"""
    CLASS = "class"
    PROPERTY = "property"
    INDIVIDUAL = "individual"
    CONCEPT = "concept"


class RelationshipType(Enum):
    """Standard ontology relationship types"""
    SUBCLASS = "subclass"
    SUBPROPERTY = "subproperty"
    DOMAIN = "domain"
    RANGE = "range"
    INVERSE = "inverse"
    REFERENCE = "reference"
    SEMANTIC_RELATION = "semantic_relation"


@dataclass
class OntologyConfig:
    """Configuration for ontology storage in KuzuDB"""
    enable_full_text_search: bool = True
    enable_spatial_indexing: bool = False
    enable_temporal_indexing: bool = False
    max_relationship_depth: int = 10


class OntologySchema:
    """
    Pure ontology schema definition for KuzuDB
    No domain-specific use cases - only generic ontology structures
    """

    @staticmethod
    def get_node_table_ddl() -> str:
        """Get DDL for ontology concept node table"""
        return """
            CREATE NODE TABLE IF NOT EXISTS OntologyConcept(
                uri STRING,
                label STRING,
                concept_type STRING,
                description STRING,
                namespace STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                PRIMARY KEY (uri)
            )
        """

    @staticmethod
    def get_relationship_table_ddl() -> str:
        """Get DDL for ontology relationship table"""
        return """
            CREATE REL TABLE IF NOT EXISTS OntologyRelationship(
                FROM OntologyConcept TO OntologyConcept,
                predicate_uri STRING,
                relationship_type STRING,
                confidence_score DOUBLE,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """

    @staticmethod
    def get_index_ddl() -> List[str]:
        """Get DDL for performance indices"""
        return [
            "CREATE INDEX IF NOT EXISTS concept_type_idx ON OntologyConcept(concept_type)",
            "CREATE INDEX IF NOT EXISTS namespace_idx ON OntologyConcept(namespace)",
            "CREATE INDEX IF NOT EXISTS relationship_type_idx ON OntologyRelationship(relationship_type)"
        ]

    @staticmethod
    def get_common_queries() -> Dict[str, str]:
        """Get common query patterns for ontology exploration"""
        return {
            "all_classes": """
                MATCH (c:OntologyConcept)
                WHERE c.concept_type = 'class'
                RETURN c.uri, c.label, c.namespace
                ORDER BY c.label
            """,

            "class_hierarchy": """
                MATCH (parent:OntologyConcept)-[r:OntologyRelationship]->(child:OntologyConcept)
                WHERE r.relationship_type = 'subclass'
                RETURN parent.uri as parent_uri, parent.label as parent_label,
                       child.uri as child_uri, child.label as child_label
            """,

            "concept_neighbors": """
                MATCH (c:OntologyConcept {uri: $concept_uri})-[r:OntologyRelationship]-(neighbor:OntologyConcept)
                RETURN neighbor.uri, neighbor.label, neighbor.concept_type,
                       r.relationship_type, r.predicate_uri
            """,

            "namespace_stats": """
                MATCH (c:OntologyConcept)
                RETURN c.namespace, count(c) as concept_count,
                       collect(DISTINCT c.concept_type) as types
            """,

            "relationship_stats": """
                MATCH ()-[r:OntologyRelationship]->()
                RETURN r.relationship_type, count(r) as count
                ORDER BY count DESC
            """
        }


def validate_concept_type(concept_type: str) -> bool:
    """Validate that concept type is supported"""
    return concept_type in [ct.value for ct in ConceptType]


def validate_relationship_type(relationship_type: str) -> bool:
    """Validate that relationship type is supported"""
    return relationship_type in [rt.value for rt in RelationshipType]