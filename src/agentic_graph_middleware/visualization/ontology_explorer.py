"""
Ontology Visualization and Development Debugging

Interactive visualization tools for exploring materialized ontologies in KuzuDB.
Provides development-friendly interfaces for ontology debugging and exploration.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class VisualizationNode:
    """Node representation for visualization"""
    id: str
    label: str
    type: str
    namespace: str
    description: Optional[str] = None


@dataclass
class VisualizationEdge:
    """Edge representation for visualization"""
    source: str
    target: str
    relationship_type: str
    predicate_uri: str


class OntologyExplorer:
    """
    Interactive ontology exploration and visualization
    for development debugging and understanding
    """

    def __init__(self, ontology_materializer):
        self.materializer = ontology_materializer

    def get_visualization_data(self, namespace_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Get ontology data formatted for visualization
        Optionally filter by namespace for focused exploration
        """

        # Query nodes
        node_query = """
            MATCH (c:OntologyConcept)
            RETURN c.uri as uri, c.label as label, c.concept_type as type,
                   c.namespace as namespace, c.description as description
        """
        if namespace_filter:
            node_query += f" WHERE c.namespace CONTAINS '{namespace_filter}'"

        nodes_result = self.materializer.query_ontology(node_query)

        # Query relationships
        edge_query = """
            MATCH (s:OntologyConcept)-[r:OntologyRelationship]->(o:OntologyConcept)
            RETURN s.uri as source, o.uri as target,
                   r.relationship_type as relationship_type, r.predicate_uri as predicate_uri
        """
        if namespace_filter:
            edge_query += f" WHERE s.namespace CONTAINS '{namespace_filter}' OR o.namespace CONTAINS '{namespace_filter}'"

        edges_result = self.materializer.query_ontology(edge_query)

        # Format for visualization
        nodes = [
            VisualizationNode(
                id=node["uri"],
                label=node["label"],
                type=node["type"],
                namespace=node["namespace"],
                description=node["description"]
            ) for node in nodes_result
        ]

        edges = [
            VisualizationEdge(
                source=edge["source"],
                target=edge["target"],
                relationship_type=edge["relationship_type"],
                predicate_uri=edge["predicate_uri"]
            ) for edge in edges_result
        ]

        return {
            "nodes": [asdict(node) for node in nodes],
            "edges": [asdict(edge) for edge in edges],
            "statistics": self.materializer.get_ontology_statistics()
        }

    def export_for_web_visualization(self, output_path: str, namespace_filter: Optional[str] = None):
        """Export ontology data for web-based visualization tools"""

        viz_data = self.get_visualization_data(namespace_filter)

        # Format for common visualization libraries (D3.js, vis.js, etc.)
        web_format = {
            "nodes": viz_data["nodes"],
            "links": viz_data["edges"],  # D3.js expects "links" not "edges"
            "metadata": viz_data["statistics"]
        }

        with open(output_path, 'w') as f:
            json.dump(web_format, f, indent=2, default=str)

        logger.info(f"Exported ontology visualization data to {output_path}")

    def find_concept_neighbors(self, concept_uri: str, depth: int = 1) -> Dict[str, Any]:
        """
        Find neighboring concepts for focused exploration
        Useful for understanding concept relationships during development
        """

        query = f"""
            MATCH (c:OntologyConcept {{uri: '{concept_uri}'}})-[r:OntologyRelationship*1..{depth}]-(neighbor:OntologyConcept)
            RETURN DISTINCT neighbor.uri as uri, neighbor.label as label,
                   neighbor.concept_type as type, neighbor.namespace as namespace
        """

        neighbors = self.materializer.query_ontology(query)

        return {
            "center_concept": concept_uri,
            "neighbors": neighbors,
            "depth": depth
        }

    def get_namespace_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all namespaces in the ontology"""

        query = """
            MATCH (c:OntologyConcept)
            RETURN c.namespace as namespace, count(c) as concept_count,
                   collect(DISTINCT c.concept_type) as types
        """

        namespaces = self.materializer.query_ontology(query)

        return [
            {
                "namespace": ns["namespace"],
                "concept_count": ns["concept_count"],
                "concept_types": ns["types"]
            } for ns in namespaces
        ]

    def debug_ontology_structure(self) -> Dict[str, Any]:
        """
        Comprehensive debugging information about ontology structure
        Useful for development and troubleshooting
        """

        stats = self.materializer.get_ontology_statistics()
        namespaces = self.get_namespace_summary()

        # Find potential issues
        issues = []

        # Check for orphaned concepts (no relationships)
        orphaned_query = """
            MATCH (c:OntologyConcept)
            WHERE NOT (c)-[:OntologyRelationship]-()
            RETURN count(c) as orphaned_count
        """
        orphaned_result = self.materializer.query_ontology(orphaned_query)
        orphaned_count = orphaned_result[0]["orphaned_count"] if orphaned_result else 0

        if orphaned_count > 0:
            issues.append(f"{orphaned_count} orphaned concepts (no relationships)")

        # Check for concepts without labels
        unlabeled_query = """
            MATCH (c:OntologyConcept)
            WHERE c.label = '' OR c.label IS NULL
            RETURN count(c) as unlabeled_count
        """
        unlabeled_result = self.materializer.query_ontology(unlabeled_query)
        unlabeled_count = unlabeled_result[0]["unlabeled_count"] if unlabeled_result else 0

        if unlabeled_count > 0:
            issues.append(f"{unlabeled_count} concepts without proper labels")

        return {
            "statistics": stats,
            "namespaces": namespaces,
            "issues": issues,
            "health_score": max(0, 100 - len(issues) * 10)  # Simple health scoring
        }