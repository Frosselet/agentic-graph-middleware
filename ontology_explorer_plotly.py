"""
Ontology Explorer - Marimo + Plotly
Pure Python graph visualization with native marimo support
"""

import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import httpx
    import networkx as nx
    import plotly.graph_objects as go
    from collections import defaultdict
    return go, httpx, mo, nx


@app.cell
def _(mo):
    mo.md(
        """
    # 🔍 Ontology Explorer

    Interactive visualization of semantic ontologies using Plotly and Marimo.

    **Features:**
    - Native marimo support (no JavaScript config)
    - Interactive zoom, pan, hover
    - Color-coded by ontology namespace
    - Pure Python - just works
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
    """

    classes = query_fuseki(classes_query)

    # Build NetworkX graph
    G = nx.DiGraph()

    # Add nodes
    namespace_counts = {}
    for _cls in classes:
        _uri = _cls['class']['value']
        _label = _cls.get('label', {}).get('value', get_local_name(_uri))
        _comment = _cls.get('comment', {}).get('value', '')

        _local_name = get_local_name(_uri)
        _namespace = get_namespace(_uri, _local_name)

        namespace_counts[_namespace] = namespace_counts.get(_namespace, 0) + 1

        G.add_node(
            _uri,
            label=_label,
            namespace=_namespace,
            description=_comment[:200] if _comment else _label,
            color=ONTOLOGY_COLORS.get(_namespace, ONTOLOGY_COLORS['unknown'])
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
    """

    relationships = query_fuseki(relationships_query)

    # Add edges
    edge_counts = {}
    for _rel in relationships:
        _subject = _rel['subject']['value']
        _predicate = _rel['predicate']['value']
        _obj = _rel['object']['value']

        if _subject in G.nodes and _obj in G.nodes:
            _rel_type = get_local_name(_predicate)
            edge_counts[_rel_type] = edge_counts.get(_rel_type, 0) + 1

            G.add_edge(_subject, _obj, relationship=_rel_type)

    ontology_graph = G
    graph_stats = {
        'nodes': len(G.nodes),
        'edges': len(G.edges),
        'namespaces': namespace_counts,
        'relationships': edge_counts
    }
    return graph_stats, ontology_graph


@app.cell
def _(graph_stats, mo):
    mo.md(
        f"""
    ## Statistics

    - **Nodes:** {graph_stats['nodes']}
    - **Edges:** {graph_stats['edges']}

    ### Ontologies
    {chr(10).join([f"- **{ns.upper()}:** {count}" for ns, count in graph_stats['namespaces'].items()])}

    ### Relationships
    {chr(10).join([f"- **{rel}:** {count}" for rel, count in graph_stats['relationships'].items()])}
    """
    )
    return


@app.cell
def _(mo):
    mo.md("""## Interactive Graph Visualization""")
    return


@app.cell
def _(go, mo, nx, ontology_graph):
    # Use spring layout for positioning
    pos = nx.spring_layout(ontology_graph, k=2, iterations=50, seed=42)

    # Create edge traces
    edge_traces = []
    for _edge in ontology_graph.edges():
        _x0, _y0 = pos[_edge[0]]
        _x1, _y1 = pos[_edge[1]]
        _edge_trace = go.Scatter(
            x=[_x0, _x1, None],
            y=[_y0, _y1, None],
            mode='lines',
            line=dict(width=1, color='#888'),
            hoverinfo='none',
            showlegend=False
        )
        edge_traces.append(_edge_trace)

    # Create node traces (one per namespace for legend)
    node_traces = {}
    for _node in ontology_graph.nodes():
        _data = ontology_graph.nodes[_node]
        _viz_ns = _data['namespace']

        if _viz_ns not in node_traces:
            node_traces[_viz_ns] = {
                'x': [],
                'y': [],
                'text': [],
                'color': _data['color'],
                'name': _viz_ns.upper()
            }

        _x, _y = pos[_node]
        node_traces[_viz_ns]['x'].append(_x)
        node_traces[_viz_ns]['y'].append(_y)
        node_traces[_viz_ns]['text'].append(
            f"<b>{_data['label']}</b><br>{_data['namespace'].upper()}<br>{_data['description'][:100]}..."
        )

    # Create plotly figure
    fig = go.Figure()

    # Add edges
    for _trace in edge_traces:
        fig.add_trace(_trace)

    # Add nodes by namespace
    for _ns_data in node_traces.values():
        fig.add_trace(go.Scatter(
            x=_ns_data['x'],
            y=_ns_data['y'],
            mode='markers+text',
            name=_ns_data['name'],
            text=[_t.split('<br>')[0].replace('<b>', '').replace('</b>', '') for _t in _ns_data['text']],
            textposition='top center',
            textfont=dict(size=9),
            hovertext=_ns_data['text'],
            hoverinfo='text',
            marker=dict(
                size=20,
                color=_ns_data['color'],
                line=dict(width=2, color='white')
            )
        ))

    # Update layout
    fig.update_layout(
        title='Ontology Graph Visualization',
        showlegend=True,
        hovermode='closest',
        height=800,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor='white',
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor='rgba(255,255,255,0.8)'
        )
    )

    mo.ui.plotly(fig)
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Controls

    - **Zoom:** Scroll or box select
    - **Pan:** Click and drag
    - **Hover:** See node details
    - **Legend:** Click to show/hide categories

    ---

    **Powered by:** Plotly + Marimo (Pure Python!)
    """
    )
    return


if __name__ == "__main__":
    app.run()
