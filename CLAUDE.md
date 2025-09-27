# Claude Code Configuration - Graph Middleware

## Project: Agentic Graph Middleware
Unified semantic graph infrastructure combining KuzuDB and Jena Fuseki for comprehensive ontology operations and SPARQL services.

### Scope
This repository provides the graph database layer for:
- **Development Visualization**: Linked ontology debugging and exploration
- **Semantic Materialization**: Bridge between raw data and ontologies
- **Virtual Standardization**: On-the-fly data transformation
- **Future Marimo Integration**: Graph-based UI components

### Architecture Role
The graph middleware sits as a **bridge layer** in the architecture:
```
Raw Data → Graph Middleware → Semantic Ontologies
```
- **Input**: Semantic-agnostic raw data from collectors
- **Process**: Materialize ontologies and standardize on-the-fly
- **Output**: Semantically enriched, queryable graph structures

### Key Files
**KuzuDB Layer:**
- `src/agentic_graph_middleware/core/ontology_materializer.py` - Core KuzuDB ontology materialization
- `src/agentic_graph_middleware/schemas/ontology_schema.py` - Pure ontology schema definitions
- `src/agentic_graph_middleware/materialization/rdf_loader.py` - RDF/OWL file loading and processing
- `src/agentic_graph_middleware/visualization/ontology_explorer.py` - Development debugging and exploration tools

**Jena Fuseki Layer:**
- `docker-compose.yml` - Fuseki triplestore container setup
- `fuseki/config/` - Fuseki configuration files
- `fuseki/scripts/` - Infrastructure startup and validation scripts
- `fuseki/queries/` - SPARQL query templates and tests

**Unified Infrastructure:**
- `src/agentic_graph_middleware/infrastructure/semantic_infrastructure.py` - Coordinates both KuzuDB and Fuseki

### Development Standards
- **Performance First**: KuzuDB chosen for high-performance graph operations
- **Semantic Bridge**: Maintain clear mapping between raw data and ontological concepts
- **Development Friendly**: Rich visualization and debugging capabilities
- **Future Ready**: Design for Marimo integration patterns

### Cross-Repository Dependencies
- **Depends On**: `agentic-semantic-ontologies` for ontology definitions, `agentic-core-engine` for base models
- **Used By**: `agentic-data-collectors` for data storage, `agentic-data-pipelines` for transformation, future Marimo UI
- **Related**: Bridge between raw data layer and semantic knowledge layer

### Key Commands
```bash
# Start unified semantic infrastructure (KuzuDB + Fuseki)
docker-compose up -d
bash fuseki/scripts/start_semantic_infrastructure.sh

# Initialize KuzuDB ontology database
python -c "from agentic_graph_middleware.infrastructure.semantic_infrastructure import SemanticInfrastructure; infra = SemanticInfrastructure(); infra.initialize_kuzu()"

# Load ontologies into both engines
python -c "infra.load_ontology_both_engines('path/to/ontology.owl')"

# Query KuzuDB with Cypher
python -c "infra.query_kuzu('MATCH (c:OntologyConcept) RETURN c.label LIMIT 10')"

# Query Fuseki with SPARQL
python -c "infra.query_fuseki('SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10')"

# Get infrastructure status
python -c "status = infra.get_infrastructure_status(); print(status)"

# Export visualization data
python -c "from agentic_graph_middleware.visualization.ontology_explorer import OntologyExplorer; explorer.export_for_web_visualization('viz_data.json')"
```

### Dual Engine Architecture
**KuzuDB Strengths:**
1. **High-Performance Graph Operations**: Fast traversals and analytics
2. **Development Debugging**: Interactive graph exploration
3. **Complex Graph Algorithms**: Advanced pattern matching
4. **Marimo Integration**: Future graph-based notebook components

**Jena Fuseki Strengths:**
1. **Standard SPARQL Endpoint**: W3C compliant semantic queries
2. **RDF Triple Store**: Native semantic web standards support
3. **Federated Queries**: Cross-dataset semantic integration
4. **Production SPARQL**: Enterprise-grade triplestore capabilities

**Combined Benefits:**
- Load once, query both ways (Cypher + SPARQL)
- Performance optimization based on query type
- Standards compliance with high-speed operations
- Comprehensive semantic infrastructure

### Technical Architecture
- **KuzuDB Core**: High-performance graph database engine
- **Semantic Mapping**: Bidirectional translation between ontologies and graph schemas
- **Visualization Engine**: Interactive debugging and exploration interfaces
- **Materialization Pipeline**: Real-time ontology-to-graph transformation
- **Query Interface**: Optimized graph queries for semantic navigation

This middleware enables **virtual semantic standardization** - transforming any raw data into semantically enriched, ontology-compliant structures without permanent storage overhead.