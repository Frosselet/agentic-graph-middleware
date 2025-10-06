"""
Ontology Explorer - Marimo + yFiles Visualization
Professional graph visualization with zero JavaScript configuration
"""

import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import httpx
    import networkx as nx
    from yfiles_jupyter_graphs import GraphWidget
    from collections import defaultdict
    return GraphWidget, httpx, mo, nx


@app.cell
def _(mo):
    mo.md(
        """
    # 🔍 Ontology Explorer

    Interactive visualization of semantic ontologies using yFiles and Marimo.

    **Features:**
    - Professional layout algorithms (organic, hierarchical, circular)
    - Interactive exploration with zoom, pan, search
    - Color-coded by ontology namespace
    - Relationship filtering
    """
    )
    return


@app.cell
def _():
    # Configuration
    FUSEKI_URL = "http://localhost:3030"
    DATASET = "ontologies"
    USERNAME = "admin"
    PASSWORD = "admin123"

    ONTOLOGY_COLORS = {
        'gist': '#3498db',
        'dbc': '#e74c3c',
        'sow': '#2ecc71',
        'bridge': '#f39c12',
        'owl': '#9b59b6',
        'rdf': '#95a5a6',
        'unknown': '#34495e'
    }
    return DATASET, FUSEKI_URL, ONTOLOGY_COLORS, PASSWORD, USERNAME


@app.cell
def _(DATASET, FUSEKI_URL, PASSWORD, USERNAME, httpx):
    def query_fuseki(query: str):
        """Execute SPARQL query"""
        client = httpx.Client(auth=(USERNAME, PASSWORD), timeout=30.0)
        endpoint = f"{FUSEKI_URL}/{DATASET}/sparql"
        response = client.post(
            endpoint,
            data={'query': query},
            headers={'Accept': 'application/sparql-results+json'}
        )
        response.raise_for_status()
        results = response.json()
        client.close()
        return results['results']['bindings']
    return (query_fuseki,)


@app.cell
def _():
    def get_namespace(uri: str, local_name: str) -> str:
        """Determine ontology namespace"""
        uri_lower = uri.lower()
        local_lower = local_name.lower()

        if any(x in local_lower for x in ['databusinesscanvas', 'customersegment', 'valueproposition',
                                            'revenuestream', 'coststructure', 'dataasset', 'intelligencecapability']):
            return 'dbc'
        elif 'bridge' in uri_lower or any(x in local_lower for x in ['channel', 'customerrelationship', 'keyresource',
                                                                       'keyactivity', 'keypartner', 'dataproduct',
                                                                       'dataservice', 'executivetarget']):
            return 'bridge'
        elif 'sow' in uri_lower or 'semanticsowcontract' in local_lower:
            return 'sow'
        elif 'gist' in uri_lower and 'bridge' not in uri_lower:
            return 'gist'
        elif 'owl' in uri_lower or 'www.w3.org/2002/07/owl' in uri:
            return 'owl'
        elif 'rdf' in uri_lower or 'www.w3.org/1999/02/22-rdf-syntax-ns' in uri:
            return 'rdf'
        else:
            return 'unknown'

    def get_local_name(uri: str) -> str:
        """Extract local name from URI"""
        if '#' in uri:
            return uri.split('#')[-1]
        elif '/' in uri:
            return uri.split('/')[-1]
        return uri
    return get_local_name, get_namespace


@app.cell
def _(mo):
    mo.md("""## Loading Ontology Data...""")
    return


@app.cell
def _(ONTOLOGY_COLORS, get_local_name, get_namespace, nx, query_fuseki):
    # Fetch classes
    classes_query = """
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?class ?label ?comment
    WHERE {
      ?class a owl:Class .
      OPTIONAL { ?class rdfs:label ?label }
      OPTIONAL { ?class rdfs:comment ?comment }
    }
    LIMIT 500
    """

    classes = query_fuseki(classes_query)

    # Build NetworkX graph
    G = nx.DiGraph()

    # Add nodes
    namespace_counts = {}
    for cls in classes:
        uri = cls['class']['value']
        label = cls.get('label', {}).get('value', get_local_name(uri))
        comment = cls.get('comment', {}).get('value', '')

        local_name = get_local_name(uri)
        namespace = get_namespace(uri, local_name)

        namespace_counts[namespace] = namespace_counts.get(namespace, 0) + 1

        G.add_node(
            uri,
            label=label,
            namespace=namespace,
            description=comment[:200] if comment else label,
            color=ONTOLOGY_COLORS.get(namespace, ONTOLOGY_COLORS['unknown'])
        )

    # Fetch relationships
    relationships_query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>

    SELECT DISTINCT ?subject ?predicate ?object
    WHERE {
      {
        ?subject rdfs:subClassOf ?object .
        BIND(rdfs:subClassOf as ?predicate)
      } UNION {
        ?subject owl:equivalentClass ?object .
        BIND(owl:equivalentClass as ?predicate)
      } UNION {
        ?subject rdfs:seeAlso ?object .
        BIND(rdfs:seeAlso as ?predicate)
      }
      FILTER(isURI(?object))
    }
    LIMIT 1000
    """

    relationships = query_fuseki(relationships_query)

    # Add edges
    edge_counts = {}
    for rel in relationships:
        subject = rel['subject']['value']
        predicate = rel['predicate']['value']
        obj = rel['object']['value']

        if subject in G.nodes and obj in G.nodes:
            rel_type = get_local_name(predicate)
            edge_counts[rel_type] = edge_counts.get(rel_type, 0) + 1

            G.add_edge(subject, obj, relationship=rel_type)

    ontology_graph = G
    stats = {
        'nodes': len(G.nodes),
        'edges': len(G.edges),
        'namespaces': namespace_counts,
        'relationships': edge_counts
    }
    return ontology_graph, stats


@app.cell
def _(mo, stats):
    mo.md(
        f"""
    ## Statistics

    - **Nodes:** {stats['nodes']}
    - **Edges:** {stats['edges']}

    ### Ontologies
    {chr(10).join([f"- **{ns.upper()}:** {count}" for ns, count in stats['namespaces'].items()])}

    ### Relationships
    {chr(10).join([f"- **{rel}:** {count}" for rel, count in stats['relationships'].items()])}
    """
    )
    return


@app.cell
def _(mo):
    mo.md("""## Interactive Graph Visualization""")
    return


@app.cell
def _(GraphWidget, ontology_graph):
    # Create yFiles widget
    widget = GraphWidget(graph=ontology_graph)

    # Configure node appearance
    widget.set_node_color_mapping(lambda node: node['color'])
    widget.set_node_label_mapping(lambda node: node['label'])
    widget.set_node_property_mapping(lambda node: {
        'description': node.get('description', ''),
        'namespace': node.get('namespace', '')
    })

    # Configure edge appearance
    widget.set_edge_label_mapping(lambda edge: edge.get('relationship', ''))

    # Set layout - organic is best for semantic graphs
    widget.organic_layout()

    widget
    return (widget,)


@app.cell
def _(mo):
    mo.md(
        """
    ## Controls

    - **Zoom:** Scroll wheel or pinch
    - **Pan:** Click and drag
    - **Search:** Use the search box in the widget
    - **Layout:** Click layout buttons to reorganize
    - **Node Info:** Hover over nodes for details

    ---

    **Powered by:** yFiles Graphs for Jupyter + Marimo
    """
    )
    return


if __name__ == "__main__":
    app.run()
