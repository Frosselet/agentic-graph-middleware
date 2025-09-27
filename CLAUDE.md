# Claude Code Configuration - Graph Middleware

## Project: Agentic Graph Middleware
KuzuDB-powered graph database middleware serving as the bridge between raw data and semantic ontologies.

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
- `src/agentic_graph_middleware/core/semantic_collectors.py` - Core KuzuDB integration
- `src/agentic_graph_middleware/schemas/kuzu_sow_schema.py` - SOW data models for KuzuDB
- `src/agentic_graph_middleware/schemas/kuzu_navigation_schema.py` - Navigation and query schemas
- `src/agentic_graph_middleware/visualization/` - Development debugging and exploration tools
- `src/agentic_graph_middleware/materialization/` - Ontology materialization engine

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
# Initialize KuzuDB instance
python -m agentic_graph_middleware.core.init_database

# Materialize ontologies
python -m agentic_graph_middleware.materialization.ontology_materializer

# Launch visualization server
python -m agentic_graph_middleware.visualization.debug_server

# Test semantic mapping
python -m agentic_graph_middleware.core.test_mapping
```

### KuzuDB Use Cases
1. **Ontology Materialization**: Convert RDF/OWL to queryable graph structures
2. **Data Standardization**: Apply semantic transformations to raw data streams
3. **Development Debugging**: Visual exploration of semantic relationships
4. **Performance Optimization**: High-speed graph queries vs. SPARQL for large datasets
5. **Marimo Integration**: Future graph-based notebook and UI components

### Technical Architecture
- **KuzuDB Core**: High-performance graph database engine
- **Semantic Mapping**: Bidirectional translation between ontologies and graph schemas
- **Visualization Engine**: Interactive debugging and exploration interfaces
- **Materialization Pipeline**: Real-time ontology-to-graph transformation
- **Query Interface**: Optimized graph queries for semantic navigation

This middleware enables **virtual semantic standardization** - transforming any raw data into semantically enriched, ontology-compliant structures without permanent storage overhead.