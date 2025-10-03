"""
KuzuDB HTTP Client
Provides HTTP-based connection to containerized KuzuDB API
"""

import requests
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class KuzuQueryResult:
    """Result from KuzuDB HTTP query"""
    success: bool
    data: List[Dict[str, Any]]
    error: Optional[str] = None
    count: int = 0

class KuzuHTTPClient:
    """HTTP client for containerized KuzuDB"""

    def __init__(self, base_url: str = "http://localhost:8080"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()

    def health_check(self) -> bool:
        """Check if KuzuDB API is healthy"""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def get_status(self) -> Dict[str, Any]:
        """Get database status and statistics"""
        try:
            response = self.session.get(f"{self.base_url}/status", timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to get status: {e}")
            return {"status": "error", "error": str(e)}

    def execute_query(self, cypher: str, parameters: Optional[Dict[str, Any]] = None) -> KuzuQueryResult:
        """Execute Cypher query via HTTP API"""
        try:
            payload = {"cypher": cypher}
            if parameters:
                payload["parameters"] = parameters

            response = self.session.post(
                f"{self.base_url}/query",
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            result_data = response.json()
            return KuzuQueryResult(
                success=result_data["success"],
                data=result_data["data"],
                error=result_data.get("error"),
                count=result_data["count"]
            )

        except requests.RequestException as e:
            logger.error(f"Query execution failed: {e}")
            return KuzuQueryResult(
                success=False,
                data=[],
                error=str(e),
                count=0
            )

    def get_nodes(self, limit: int = 100, offset: int = 0, node_type: Optional[str] = None) -> KuzuQueryResult:
        """Get ontology nodes with optional filtering"""
        try:
            params = {"limit": limit, "offset": offset}
            if node_type:
                params["node_type"] = node_type

            response = self.session.get(f"{self.base_url}/nodes", params=params, timeout=30)
            response.raise_for_status()

            result_data = response.json()
            return KuzuQueryResult(
                success=result_data["success"],
                data=result_data["data"],
                error=result_data.get("error"),
                count=result_data["count"]
            )

        except requests.RequestException as e:
            logger.error(f"Failed to get nodes: {e}")
            return KuzuQueryResult(
                success=False,
                data=[],
                error=str(e),
                count=0
            )

    def get_node_count(self) -> int:
        """Get total number of nodes in the database"""
        result = self.execute_query("MATCH (n) RETURN COUNT(n) as count")
        if result.success and result.data:
            return result.data[0].get("col_0", 0)
        return 0

    def get_ontology_statistics(self) -> Dict[str, Any]:
        """Get comprehensive ontology statistics"""
        stats = {}

        # Get total node count
        stats["total_nodes"] = self.get_node_count()

        # Get node type distribution
        type_result = self.execute_query("""
            MATCH (n:OntologyConcept)
            RETURN n.concept_type as type, COUNT(n) as count
        """)

        if type_result.success:
            stats["node_types"] = {
                row.get("col_0", "unknown"): row.get("col_1", 0)
                for row in type_result.data
            }
        else:
            stats["node_types"] = {}

        return stats