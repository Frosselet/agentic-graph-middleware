"""
Ontology Explorer - Marimo + Graphistry
Professional graph visualization with interactive exploration
"""

import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import httpx
    import pandas as pd
    import graphistry
    return graphistry, httpx, mo, pd


@app.cell
def _(mo):
    mo.md(
        """
    # 🔍 Ontology Explorer

    Interactive visualization of semantic ontologies using Graphistry and Marimo.

    **Features:**
    - High-performance graph visualization
    - Interactive filtering and exploration
    - Color-coded by ontology namespace
    - GPU-accelerated rendering
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
        'gist': 0x3498db,
        'dbc': 0xe74c3c,
        'sow': 0x2ecc71,
        'bridge': 0xf39c12,
        'owl': 0x9b59b6,
        'rdf': 0x95a5a6,
        'unknown': 0x34495e
    }
    return DATASET, FUSEKI_URL, ONTOLOGY_COLORS, PASSWORD, USERNAME


@app.cell
def _(DATASET, FUSEKI_URL, PASSWORD, USERNAME, httpx):
    def query_fuseki(query: str):
        """Execute SPARQL query"""
        _client = httpx.Client(auth=(USERNAME, PASSWORD), timeout=30.0)
        _endpoint = f"{FUSEKI_URL}/{DATASET}/sparql"
        _response = _client.post(
            _endpoint,
            data={'query': query},
            headers={'Accept': 'application/sparql-results+json'}
        )
        _response.raise_for_status()
        _results = _response.json()
        _client.close()
        return _results['results']['bindings']
    return (query_fuseki,)


@app.cell
def _():
    def get_namespace(uri: str, local_name: str) -> str:
        """Determine ontology namespace"""
        _uri_lower = uri.lower()
        _local_lower = local_name.lower()

        if any(_x in _local_lower for _x in ['databusinesscanvas', 'customersegment', 'valueproposition',
                                            'revenuestream', 'coststructure', 'dataasset', 'intelligencecapability']):
            return 'dbc'
        elif 'bridge' in _uri_lower or any(_x in _local_lower for _x in ['channel', 'customerrelationship', 'keyresource',
                                                                       'keyactivity', 'keypartner', 'dataproduct',
                                                                       'dataservice', 'executivetarget']):
            return 'bridge'
        elif 'sow' in _uri_lower or 'semanticsowcontract' in _local_lower:
            return 'sow'
        elif 'gist' in _uri_lower and 'bridge' not in _uri_lower:
            return 'gist'
        elif 'owl' in _uri_lower or 'www.w3.org/2002/07/owl' in uri:
            return 'owl'
        elif 'rdf' in _uri_lower or 'www.w3.org/1999/02/22-rdf-syntax-ns' in uri:
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
def _(ONTOLOGY_COLORS, get_local_name, get_namespace, pd, query_fuseki):
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
    """

    _classes = query_fuseki(classes_query)

    # Build nodes dataframe
    _nodes_data = []
    for _cls in _classes:
        _uri = _cls['class']['value']
        _label = _cls.get('label', {}).get('value', get_local_name(_uri))
        _comment = _cls.get('comment', {}).get('value', '')

        _local_name = get_local_name(_uri)
        _ns = get_namespace(_uri, _local_name)

        _nodes_data.append({
            'node': _uri,
            'label': _label,
            'namespace': _ns,
            'description': _comment[:200] if _comment else _label,
            'color': ONTOLOGY_COLORS.get(_ns, ONTOLOGY_COLORS['unknown'])
        })

    nodes_df = pd.DataFrame(_nodes_data)

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
    """

    _relationships = query_fuseki(relationships_query)

    # Build edges dataframe
    _edges_data = []
    _valid_nodes = set(nodes_df['node'])
    for _rel in _relationships:
        _subject = _rel['subject']['value']
        _predicate = _rel['predicate']['value']
        _obj = _rel['object']['value']

        if _subject in _valid_nodes and _obj in _valid_nodes:
            _rel_type = get_local_name(_predicate)
            _edges_data.append({
                'src': _subject,
                'dst': _obj,
                'relationship': _rel_type
            })

    edges_df = pd.DataFrame(_edges_data)

    return edges_df, nodes_df


@app.cell
def _(edges_df, mo, nodes_df):
    mo.md(
        f"""
    ## Statistics

    - **Nodes:** {len(nodes_df)}
    - **Edges:** {len(edges_df)}

    ### Ontologies
    {chr(10).join([f"- **{_ns.upper()}:** {_count}" for _ns, _count in nodes_df['namespace'].value_counts().items()])}

    ### Relationships
    {chr(10).join([f"- **{_rel}:** {_count}" for _rel, _count in edges_df['relationship'].value_counts().items()])}
    """
    )
    return


@app.cell
def _(mo):
    mo.md("""## Interactive Graph Visualization""")
    return


@app.cell
def _(edges_df, graphistry, nodes_df):
    # Register for local mode (no API key needed)
    graphistry.register(api=3, protocol='http', server='localhost')

    # Create Graphistry visualization
    g = graphistry.edges(edges_df, 'src', 'dst').nodes(nodes_df, 'node')

    # Configure appearance
    g2 = (g
        .bind(point_color='color')
        .bind(point_title='label')
        .bind(edge_title='relationship')
    )

    # Return the plot - local mode
    visualization = g2.plot(render=False)
    return g, g2, visualization


@app.cell
def _(mo, visualization):
    mo.Html(visualization)
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Controls

    - **Zoom:** Scroll wheel
    - **Pan:** Click and drag
    - **Select:** Click nodes
    - **Filter:** Use toolbar controls

    ---

    **Powered by:** Graphistry + Marimo
    """
    )
    return


if __name__ == "__main__":
    app.run()
