"""
Unified Semantic Infrastructure Manager

Coordinates both KuzuDB and Jena Fuseki for comprehensive
semantic graph operations and SPARQL endpoint services.
"""

import subprocess
import logging
import time
import requests
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime

from ..core.ontology_materializer import OntologyMaterializer
from ..materialization.rdf_loader import RDFLoader

logger = logging.getLogger(__name__)


class SemanticInfrastructure:
    """
    Unified manager for KuzuDB and Jena Fuseki semantic infrastructure
    Provides high-level interface for ontology operations and SPARQL services
    """

    def __init__(self,
                 kuzu_db_path: str = "./ontology.kuzu",
                 fuseki_endpoint: str = "http://localhost:3030"):
        self.kuzu_db_path = kuzu_db_path
        self.fuseki_endpoint = fuseki_endpoint
        self.kuzu_materializer = None
        self.rdf_loader = None

    def initialize_kuzu(self) -> bool:
        """Initialize KuzuDB for high-performance graph operations"""
        try:
            self.kuzu_materializer = OntologyMaterializer(self.kuzu_db_path)
            self.rdf_loader = RDFLoader(self.kuzu_materializer)
            logger.info(f"KuzuDB initialized at {self.kuzu_db_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize KuzuDB: {e}")
            return False

    def start_fuseki(self, docker_compose_path: Optional[str] = None) -> bool:
        """Start Jena Fuseki triplestore using Docker Compose"""
        try:
            if docker_compose_path is None:
                # Use the one in graph middleware
                docker_compose_path = str(Path(__file__).parent.parent.parent / "docker-compose.yml")

            # Start Fuseki container
            subprocess.run([
                "docker-compose", "-f", docker_compose_path, "up", "-d"
            ], check=True, capture_output=True)

            # Wait for Fuseki to be ready
            return self._wait_for_fuseki()

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to start Fuseki: {e}")
            return False

    def stop_fuseki(self, docker_compose_path: Optional[str] = None) -> bool:
        """Stop Jena Fuseki triplestore"""
        try:
            if docker_compose_path is None:
                docker_compose_path = str(Path(__file__).parent.parent.parent / "docker-compose.yml")

            subprocess.run([
                "docker-compose", "-f", docker_compose_path, "down"
            ], check=True, capture_output=True)

            logger.info("Fuseki stopped successfully")
            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to stop Fuseki: {e}")
            return False

    def _wait_for_fuseki(self, timeout: int = 60) -> bool:
        """Wait for Fuseki to be ready"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"{self.fuseki_endpoint}/$/ping", timeout=2)
                if response.status_code == 200:
                    logger.info("Fuseki is ready!")
                    return True
            except requests.RequestException:
                pass

            time.sleep(2)

        logger.error("Timeout waiting for Fuseki to start")
        return False

    def load_ontology_both_engines(self, ontology_path: str) -> Dict[str, Any]:
        """
        Load ontology into both KuzuDB and Fuseki for comprehensive access
        KuzuDB for high-performance graph operations, Fuseki for SPARQL
        """
        results = {
            "kuzu_stats": None,
            "fuseki_loaded": False,
            "errors": []
        }

        # Load into KuzuDB
        if self.rdf_loader:
            try:
                results["kuzu_stats"] = self.rdf_loader.load_ontology_file(ontology_path)
                logger.info(f"Loaded ontology into KuzuDB: {results['kuzu_stats']}")
            except Exception as e:
                error_msg = f"Failed to load into KuzuDB: {e}"
                logger.error(error_msg)
                results["errors"].append(error_msg)

        # Load into Fuseki via SPARQL UPDATE
        try:
            self._load_ontology_to_fuseki(ontology_path)
            results["fuseki_loaded"] = True
            logger.info("Loaded ontology into Fuseki")
        except Exception as e:
            error_msg = f"Failed to load into Fuseki: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)

        return results

    def _load_ontology_to_fuseki(self, ontology_path: str):
        """Load ontology file to Fuseki dataset"""
        # Read ontology file
        with open(ontology_path, 'r', encoding='utf-8') as f:
            ontology_content = f.read()

        # Determine format
        if ontology_path.endswith('.owl') or ontology_path.endswith('.rdf'):
            content_type = 'application/rdf+xml'
        elif ontology_path.endswith('.ttl'):
            content_type = 'text/turtle'
        elif ontology_path.endswith('.n3'):
            content_type = 'text/n3'
        else:
            content_type = 'application/rdf+xml'  # Default

        # POST to Fuseki
        url = f"{self.fuseki_endpoint}/ontologies/data"
        headers = {'Content-Type': content_type}

        response = requests.post(url, data=ontology_content, headers=headers)
        response.raise_for_status()

    def query_kuzu(self, cypher_query: str, parameters: Dict[str, Any] = None) -> List[Dict]:
        """Execute Cypher query on KuzuDB"""
        if not self.kuzu_materializer:
            raise RuntimeError("KuzuDB not initialized")

        return self.kuzu_materializer.query_ontology(cypher_query, parameters)

    def query_fuseki(self, sparql_query: str) -> Dict[str, Any]:
        """Execute SPARQL query on Fuseki"""
        url = f"{self.fuseki_endpoint}/ontologies/sparql"

        response = requests.post(
            url,
            data={'query': sparql_query},
            headers={'Accept': 'application/json'}
        )
        response.raise_for_status()

        return response.json()

    def get_infrastructure_status(self) -> Dict[str, Any]:
        """Get status of both KuzuDB and Fuseki"""
        status = {
            "kuzu_ready": self.kuzu_materializer is not None,
            "fuseki_ready": False,
            "kuzu_stats": None,
            "fuseki_stats": None,
            "timestamp": datetime.now().isoformat()
        }

        # Check KuzuDB
        if self.kuzu_materializer:
            try:
                status["kuzu_stats"] = self.kuzu_materializer.get_ontology_statistics()
            except Exception as e:
                logger.error(f"Failed to get KuzuDB stats: {e}")

        # Check Fuseki
        try:
            response = requests.get(f"{self.fuseki_endpoint}/$/ping", timeout=5)
            status["fuseki_ready"] = response.status_code == 200

            if status["fuseki_ready"]:
                # Get dataset info
                datasets_response = requests.get(f"{self.fuseki_endpoint}/$/datasets")
                if datasets_response.status_code == 200:
                    status["fuseki_stats"] = datasets_response.json()

        except requests.RequestException as e:
            logger.debug(f"Fuseki not accessible: {e}")

        return status

    def create_dataset(self, dataset_name: str) -> bool:
        """Create a new dataset in Fuseki"""
        try:
            url = f"{self.fuseki_endpoint}/$/datasets"
            data = {
                'dbName': dataset_name,
                'dbType': 'tdb2'
            }

            response = requests.post(url, data=data)
            response.raise_for_status()

            logger.info(f"Created Fuseki dataset: {dataset_name}")
            return True

        except requests.RequestException as e:
            logger.error(f"Failed to create dataset {dataset_name}: {e}")
            return False

    def backup_kuzu_data(self, backup_path: str) -> bool:
        """Backup KuzuDB data"""
        try:
            import shutil
            shutil.copytree(self.kuzu_db_path, backup_path, dirs_exist_ok=True)
            logger.info(f"KuzuDB backed up to {backup_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to backup KuzuDB: {e}")
            return False