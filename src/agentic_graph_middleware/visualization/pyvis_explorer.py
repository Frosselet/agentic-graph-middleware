"""
Rich Interactive Ontology Explorer using Pyvis
Visualize ontology relationships directly from Fuseki, identify gaps, and explore connections
"""

import httpx
from pyvis.network import Network
from typing import Dict, List, Optional
import logging
from pathlib import Path
from collections import defaultdict

logger = logging.getLogger(__name__)


class PyvisOntologyExplorer:
    """Interactive ontology visualization and exploration using Pyvis"""

    # Color scheme for different ontologies
    ONTOLOGY_COLORS = {
        'gist': '#3498db',      # Blue - GIST Core
        'dbc': '#e74c3c',       # Red - DBC (Data Business Canvas)
        'sow': '#2ecc71',       # Green - SOW
        'bridge': '#f39c12',    # Orange - Bridge concepts
        'owl': '#9b59b6',       # Purple - OWL/RDF Schema
        'rdf': '#95a5a6',       # Gray - RDF
        'unknown': '#34495e'    # Dark gray - Unknown
    }

    RELATIONSHIP_COLORS = {
        'type': '#e74c3c',           # Red - rdf:type
        'subClassOf': '#3498db',     # Blue - rdfs:subClassOf
        'subPropertyOf': '#9b59b6',  # Purple
        'domain': '#2ecc71',         # Green
        'range': '#f39c12',          # Orange
        'sameAs': '#1abc9c',         # Teal
        'equivalentClass': '#16a085',# Dark teal
        'seeAlso': '#e67e22',        # Carrot orange - Related concepts
        'default': '#bdc3c7'         # Light gray
    }

    def __init__(
        self,
        fuseki_url: str = "http://localhost:3030",
        fuseki_dataset: str = "ontologies",
        username: str = "admin",
        password: str = "admin"
    ):
        self.fuseki_url = fuseki_url.rstrip('/')
        self.fuseki_dataset = fuseki_dataset
        self.client = httpx.Client(
            timeout=60.0,
            auth=(username, password) if username else None
        )

    def _query_sparql(self, query: str) -> List[Dict]:
        """Execute SPARQL query against Fuseki"""
        endpoint = f"{self.fuseki_url}/{self.fuseki_dataset}/sparql"

        try:
            response = self.client.get(
                endpoint,
                params={'query': query},
                headers={'Accept': 'application/sparql-results+json'}
            )
            response.raise_for_status()
            results = response.json()
            return results.get('results', {}).get('bindings', [])
        except Exception as e:
            logger.error(f"SPARQL query failed: {e}")
            logger.error(f"Endpoint: {endpoint}")
            return []

    def _get_namespace(self, uri: str) -> str:
        """Extract namespace from URI"""
        uri_lower = uri.lower()

        # Get local name to check for DBC-specific concepts
        local_name = self._get_local_name(uri).lower()

        # DBC concepts (even if in bridge file) - check local name for DBC-specific terms
        dbc_terms = ['databusinesscanvas', 'valueproposition', 'customersegment', 'dataasset',
                     'intelligencecapability', 'revenuestream', 'coststructure', 'keyadaptation',
                     'dataqualitystandard', 'dataprocessingtask', 'dataprovider', 'dataconsumer']
        if any(term in local_name for term in dbc_terms):
            return 'dbc'

        # SOW ontology
        if '/sow' in uri_lower or 'complete-sow' in uri_lower:
            return 'sow'

        # Bridge ontology (concepts that bridge but aren't DBC-specific)
        if 'gist-dbc-bridge' in uri_lower or '/bridge' in uri_lower:
            return 'bridge'

        # GIST ontology
        if '/gist' in uri_lower or 'semanticarts' in uri_lower:
            return 'gist'

        # OWL/RDFS namespaces
        if 'w3.org/2002/07/owl' in uri:
            return 'owl'
        if 'w3.org/1999/02/22-rdf-syntax' in uri or 'w3.org/2000/01/rdf-schema' in uri:
            return 'rdf'

        return 'unknown'

    def _get_local_name(self, uri: str) -> str:
        """Extract local name from URI"""
        if '#' in uri:
            return uri.split('#')[-1]
        elif '/' in uri:
            parts = uri.rstrip('/').split('/')
            return parts[-1]
        return uri

    def create_interactive_graph(
        self,
        output_file: str = "ontology_explorer.html",
        height: str = "900px",
        width: str = "100%",
        max_concepts: int = 500
    ) -> str:
        """Create comprehensive interactive ontology visualization"""

        logger.info("🎨 Creating interactive ontology visualization...")

        # Initialize Pyvis network
        net = Network(
            height=height,
            width=width,
            bgcolor='#ffffff',
            font_color='#000000',
            notebook=False,
            directed=True
        )

        # Configure physics for better layout
        net.set_options("""
        {
          "physics": {
            "barnesHut": {
              "gravitationalConstant": -50000,
              "centralGravity": 0.5,
              "springLength": 250,
              "springConstant": 0.05,
              "damping": 0.1,
              "avoidOverlap": 0.2
            },
            "minVelocity": 0.75,
            "solver": "barnesHut",
            "stabilization": {
              "enabled": true,
              "iterations": 1000,
              "updateInterval": 25
            }
          },
          "interaction": {
            "hover": true,
            "tooltipDelay": 100,
            "navigationButtons": true,
            "keyboard": {
              "enabled": true
            },
            "zoomView": true,
            "dragView": true
          },
          "manipulation": {
            "enabled": false
          },
          "nodes": {
            "font": {
              "size": 16,
              "face": "arial",
              "bold": {
                "size": 18
              }
            },
            "borderWidth": 2,
            "borderWidthSelected": 4
          },
          "edges": {
            "font": {
              "size": 12,
              "align": "middle"
            },
            "arrows": {
              "to": {
                "enabled": true,
                "scaleFactor": 0.5
              }
            },
            "smooth": {
              "type": "continuous",
              "roundness": 0.5
            }
          }
        }
        """)

        # Fetch all classes
        logger.info("📊 Fetching ontology classes...")
        classes_query = f"""
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?class ?label ?comment
        WHERE {{
          ?class a owl:Class .
          OPTIONAL {{ ?class rdfs:label ?label }}
          OPTIONAL {{ ?class rdfs:comment ?comment }}
        }}
        LIMIT {max_concepts}
        """

        classes = self._query_sparql(classes_query)
        logger.info(f"  Found {len(classes)} classes")

        # Store nodes to add (will add after we know which have edges)
        nodes_to_add = {}
        node_stats = defaultdict(int)

        for cls in classes:
            uri = cls['class']['value']
            label = cls.get('label', {}).get('value', self._get_local_name(uri))
            comment = cls.get('comment', {}).get('value', '')

            namespace = self._get_namespace(uri)
            color = self.ONTOLOGY_COLORS.get(namespace, self.ONTOLOGY_COLORS['unknown'])

            # Create rich tooltip
            tooltip = f"""<div style='max-width:400px'>
            <b style='font-size:16px'>{label}</b><br><br>
            <b>Type:</b> Class<br>
            <b>Namespace:</b> {namespace.upper()}<br>
            <b>URI:</b> <small>{uri}</small><br>
            {f"<br><b>Description:</b><br>{comment[:300]}..." if comment else ''}
            </div>"""

            nodes_to_add[uri] = {
                'label': label,
                'title': tooltip,
                'color': color,
                'size': 30,
                'shape': 'dot',
                'namespace': namespace
            }

        # Fetch properties
        logger.info("📊 Fetching ontology properties...")
        properties_query = f"""
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?prop ?label ?comment
        WHERE {{
          {{
            ?prop a owl:ObjectProperty .
          }} UNION {{
            ?prop a owl:DatatypeProperty .
          }}
          OPTIONAL {{ ?prop rdfs:label ?label }}
          OPTIONAL {{ ?prop rdfs:comment ?comment }}
        }}
        LIMIT {max_concepts // 2}
        """

        properties = self._query_sparql(properties_query)
        logger.info(f"  Found {len(properties)} properties")

        # Store property nodes to add
        for prop in properties:
            uri = prop['prop']['value']
            label = prop.get('label', {}).get('value', self._get_local_name(uri))
            comment = prop.get('comment', {}).get('value', '')

            namespace = self._get_namespace(uri)
            color = self.ONTOLOGY_COLORS.get(namespace, self.ONTOLOGY_COLORS['unknown'])

            tooltip = f"""<div style='max-width:400px'>
            <b style='font-size:16px'>{label}</b><br><br>
            <b>Type:</b> Property<br>
            <b>Namespace:</b> {namespace.upper()}<br>
            <b>URI:</b> <small>{uri}</small><br>
            {f"<br><b>Description:</b><br>{comment[:300]}..." if comment else ''}
            </div>"""

            nodes_to_add[uri] = {
                'label': label,
                'title': tooltip,
                'color': color,
                'size': 20,
                'shape': 'diamond',
                'namespace': namespace
            }

        # Fetch relationships
        logger.info("🔗 Fetching class relationships...")
        relationships_query = f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>

        SELECT DISTINCT ?subject ?predicate ?object
        WHERE {{
          {{
            ?subject rdfs:subClassOf ?object .
            BIND(rdfs:subClassOf as ?predicate)
          }} UNION {{
            ?subject owl:equivalentClass ?object .
            BIND(owl:equivalentClass as ?predicate)
          }} UNION {{
            ?subject owl:sameAs ?object .
            BIND(owl:sameAs as ?predicate)
          }} UNION {{
            ?subject rdfs:seeAlso ?object .
            BIND(rdfs:seeAlso as ?predicate)
          }}
          FILTER(isURI(?object))
        }}
        LIMIT 1000
        """

        relationships = self._query_sparql(relationships_query)
        logger.info(f"  Found {len(relationships)} class relationships")

        # Track which nodes are connected
        connected_nodes = set()
        edges_to_add = []

        # Add class relationship edges
        edge_stats = defaultdict(int)

        for rel in relationships:
            subject = rel['subject']['value']
            predicate = rel['predicate']['value']
            obj = rel['object']['value']

            # Only add edge if both nodes are available
            if subject in nodes_to_add and obj in nodes_to_add:
                pred_name = self._get_local_name(predicate)
                edge_color = self.RELATIONSHIP_COLORS.get(
                    pred_name,
                    self.RELATIONSHIP_COLORS['default']
                )
                edge_stats[pred_name] += 1

                edge_label = pred_name
                edge_title = f"{pred_name}: {self._get_local_name(subject)} → {self._get_local_name(obj)}"

                edges_to_add.append({
                    'from': subject,
                    'to': obj,
                    'label': edge_label,
                    'title': edge_title,
                    'color': edge_color,
                    'width': 2
                })

                connected_nodes.add(subject)
                connected_nodes.add(obj)

        # Fetch property relationships
        logger.info("🔗 Fetching property relationships...")
        prop_relationships_query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?prop ?domain ?range
        WHERE {
          ?prop rdfs:domain ?domain .
          OPTIONAL { ?prop rdfs:range ?range }
          FILTER(isURI(?domain))
        }
        LIMIT 500
        """

        prop_rels = self._query_sparql(prop_relationships_query)
        logger.info(f"  Found {len(prop_rels)} property relationships")

        for rel in prop_rels:
            prop = rel['prop']['value']
            domain = rel['domain']['value']
            range_val = rel.get('range', {}).get('value')

            if prop in nodes_to_add and domain in nodes_to_add:
                edges_to_add.append({
                    'from': domain,
                    'to': prop,
                    'label': "has property",
                    'title': f"domain: {self._get_local_name(domain)} → {self._get_local_name(prop)}",
                    'color': self.RELATIONSHIP_COLORS['domain'],
                    'width': 1.5,
                    'dashes': True
                })
                edge_stats['domain'] += 1
                connected_nodes.add(prop)
                connected_nodes.add(domain)

            if range_val and prop in nodes_to_add and range_val in nodes_to_add:
                edges_to_add.append({
                    'from': prop,
                    'to': range_val,
                    'label': "range",
                    'title': f"range: {self._get_local_name(prop)} → {self._get_local_name(range_val)}",
                    'color': self.RELATIONSHIP_COLORS['range'],
                    'width': 1.5,
                    'dashes': True
                })
                edge_stats['range'] += 1
                connected_nodes.add(prop)
                connected_nodes.add(range_val)

        # Add all nodes (including isolated GIST nodes to show bridging opportunities)
        logger.info("📍 Adding nodes to graph...")
        for uri, node_data in nodes_to_add.items():
            namespace = node_data['namespace']

            net.add_node(
                uri,
                label=node_data['label'],
                title=node_data['title'],
                color=node_data['color'],
                size=node_data['size'],
                shape=node_data['shape']
            )
            node_stats[namespace] += 1

        # Add all edges
        logger.info("🔗 Adding edges to graph...")
        for edge in edges_to_add:
            net.add_edge(
                edge['from'],
                edge['to'],
                label=edge['label'],
                title=edge['title'],
                color=edge['color'],
                width=edge['width'],
                dashes=edge.get('dashes', False),
                arrows={'to': {'enabled': True, 'scaleFactor': 0.5}}
            )

        # Log statistics
        logger.info("📈 Graph Statistics:")
        logger.info(f"  Total nodes: {len(net.nodes)}")
        logger.info(f"  Total edges: {len(net.edges)}")
        logger.info(f"  Nodes by ontology: {dict(node_stats)}")
        logger.info(f"  Edges by type: {dict(edge_stats)}")

        # Add custom HTML with legend
        legend_html = self._create_legend_html(node_stats, edge_stats)

        # Save visualization
        output_path = Path(output_file)
        net.save_graph(str(output_path))

        # Inject legend into HTML
        html_content = output_path.read_text(encoding='utf-8')
        html_content = html_content.replace('</body>', f'{legend_html}</body>')
        output_path.write_text(html_content, encoding='utf-8')

        logger.info(f"✅ Visualization saved to: {output_path.absolute()}")
        logger.info(f"🌐 Open in browser: file://{output_path.absolute()}")

        return str(output_path.absolute())

    def _create_legend_html(self, node_stats: dict, edge_stats: dict) -> str:
        """Create HTML legend for the visualization with interactive filtering"""
        import html
        return f"""
        <div style="position: fixed; top: 10px; right: 10px; background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.2); font-family: Arial; max-width: 300px; z-index: 1000;">
            <h3 style="margin-top: 0; color: #2c3e50;">Ontology Explorer</h3>

            <h4 style="margin-bottom: 5px; color: #34495e;">Filter Ontologies (click to toggle):</h4>
            <div style="font-size: 12px;">
                <div id="filter-gist" onclick="toggleOntology('gist')" style="cursor: pointer; padding: 3px; border-radius: 3px; margin: 2px 0; transition: background 0.2s;" onmouseover="this.style.background='#ecf0f1'" onmouseout="this.style.background='white'">
                    <span style="display:inline-block; width:15px; height:15px; background:#3498db; border-radius:50%; margin-right:5px;"></span>
                    <span id="gist-label">GIST ({node_stats.get('gist', 0)})</span>
                    <span id="gist-status" style="float: right; color: #27ae60; font-weight: bold;">✓</span>
                </div>
                <div id="filter-dbc" onclick="toggleOntology('dbc')" style="cursor: pointer; padding: 3px; border-radius: 3px; margin: 2px 0; transition: background 0.2s;" onmouseover="this.style.background='#ecf0f1'" onmouseout="this.style.background='white'">
                    <span style="display:inline-block; width:15px; height:15px; background:#e74c3c; border-radius:50%; margin-right:5px;"></span>
                    <span id="dbc-label">DBC ({node_stats.get('dbc', 0)})</span>
                    <span id="dbc-status" style="float: right; color: #27ae60; font-weight: bold;">✓</span>
                </div>
                <div id="filter-sow" onclick="toggleOntology('sow')" style="cursor: pointer; padding: 3px; border-radius: 3px; margin: 2px 0; transition: background 0.2s;" onmouseover="this.style.background='#ecf0f1'" onmouseout="this.style.background='white'">
                    <span style="display:inline-block; width:15px; height:15px; background:#2ecc71; border-radius:50%; margin-right:5px;"></span>
                    <span id="sow-label">SOW ({node_stats.get('sow', 0)})</span>
                    <span id="sow-status" style="float: right; color: #27ae60; font-weight: bold;">✓</span>
                </div>
                <div id="filter-bridge" onclick="toggleOntology('bridge')" style="cursor: pointer; padding: 3px; border-radius: 3px; margin: 2px 0; transition: background 0.2s;" onmouseover="this.style.background='#ecf0f1'" onmouseout="this.style.background='white'">
                    <span style="display:inline-block; width:15px; height:15px; background:#f39c12; border-radius:50%; margin-right:5px;"></span>
                    <span id="bridge-label">Bridge ({node_stats.get('bridge', 0)})</span>
                    <span id="bridge-status" style="float: right; color: #27ae60; font-weight: bold;">✓</span>
                </div>
                <div id="filter-owl" onclick="toggleOntology('owl')" style="cursor: pointer; padding: 3px; border-radius: 3px; margin: 2px 0; transition: background 0.2s;" onmouseover="this.style.background='#ecf0f1'" onmouseout="this.style.background='white'">
                    <span style="display:inline-block; width:15px; height:15px; background:#9b59b6; border-radius:50%; margin-right:5px;"></span>
                    <span id="owl-label">OWL/RDFS ({node_stats.get('owl', 0) + node_stats.get('rdf', 0)})</span>
                    <span id="owl-status" style="float: right; color: #27ae60; font-weight: bold;">✓</span>
                </div>
                <div id="filter-unknown" onclick="toggleOntology('unknown')" style="cursor: pointer; padding: 3px; border-radius: 3px; margin: 2px 0; transition: background 0.2s;" onmouseover="this.style.background='#ecf0f1'" onmouseout="this.style.background='white'">
                    <span style="display:inline-block; width:15px; height:15px; background:#34495e; border-radius:50%; margin-right:5px;"></span>
                    <span id="unknown-label">Unknown ({node_stats.get('unknown', 0)})</span>
                    <span id="unknown-status" style="float: right; color: #27ae60; font-weight: bold;">✓</span>
                </div>
            </div>

            <h4 style="margin: 10px 0 5px 0; color: #34495e;">Relationships:</h4>
            <div style="font-size: 11px;">
                <div>subClassOf: {edge_stats.get('subClassOf', 0)}</div>
                <div>equivalentClass: {edge_stats.get('equivalentClass', 0)}</div>
                <div>domain/range: {edge_stats.get('domain', 0) + edge_stats.get('range', 0)}</div>
            </div>

            <div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid #ecf0f1; font-size: 11px; color: #7f8c8d;">
                {html.escape('💡')} Hover over nodes for details<br>
                {html.escape('🖱️')} Click & drag to explore<br>
                {html.escape('🔍')} Scroll to zoom<br>
                {html.escape('👆')} Click colors to filter
            </div>
        </div>

        <script type="text/javascript">
            // Store original node data
            var originalNodes = nodes.get();
            var originalEdges = edges.get();

            // Track which ontologies are visible
            var visibleOntologies = {{
                'gist': true,
                'dbc': true,
                'sow': true,
                'bridge': true,
                'owl': true,
                'rdf': true,
                'unknown': true
            }};

            // Color mapping for ontologies
            var ontologyColors = {{
                '#3498db': 'gist',
                '#e74c3c': 'dbc',
                '#2ecc71': 'sow',
                '#f39c12': 'bridge',
                '#9b59b6': 'owl',
                '#95a5a6': 'rdf',
                '#34495e': 'unknown'
            }};

            function toggleOntology(ontology) {{
                // Toggle visibility
                visibleOntologies[ontology] = !visibleOntologies[ontology];

                // Update status indicator
                var statusElement = document.getElementById(ontology + '-status');
                var filterElement = document.getElementById('filter-' + ontology);
                if (visibleOntologies[ontology]) {{
                    statusElement.innerHTML = '✓';
                    statusElement.style.color = '#27ae60';
                    filterElement.style.opacity = '1';
                }} else {{
                    statusElement.innerHTML = '✗';
                    statusElement.style.color = '#e74c3c';
                    filterElement.style.opacity = '0.5';
                }}

                // Filter nodes and edges
                filterGraph();
            }}

            function filterGraph() {{
                // Filter nodes based on color
                var filteredNodes = originalNodes.filter(function(node) {{
                    var nodeOntology = ontologyColors[node.color];
                    return visibleOntologies[nodeOntology] === true;
                }});

                // Get IDs of visible nodes
                var visibleNodeIds = new Set(filteredNodes.map(n => n.id));

                // Filter edges - only show if both endpoints are visible
                var filteredEdges = originalEdges.filter(function(edge) {{
                    return visibleNodeIds.has(edge.from) && visibleNodeIds.has(edge.to);
                }});

                // Update the network
                nodes.clear();
                edges.clear();
                nodes.add(filteredNodes);
                edges.add(filteredEdges);

                // Re-fit the network
                network.fit();
            }}
        </script>
        """

    def generate_analysis_report(self, output_file: str = "ontology_analysis.html"):
        """Generate comprehensive ontology analysis report"""
        logger.info("📋 Generating ontology analysis report...")

        # Get bridge concepts
        bridges = self._find_bridge_concepts()

        # Get orphaned concepts
        orphans = self._find_orphaned_concepts()

        # Get statistics
        stats = self._get_ontology_statistics()

        html = self._create_report_html(stats, bridges, orphans)

        output_path = Path(output_file)
        output_path.write_text(html, encoding='utf-8')
        logger.info(f"✅ Analysis report saved to: {output_path.absolute()}")

        return str(output_path.absolute())

    def _find_bridge_concepts(self) -> List[Dict]:
        """Find concepts that bridge between different ontologies"""
        query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>

        SELECT DISTINCT ?concept ?parent1 ?parent2
        WHERE {
          ?concept a owl:Class .
          ?concept rdfs:subClassOf ?parent1 .
          ?concept rdfs:subClassOf ?parent2 .
          FILTER(?parent1 != ?parent2)
          FILTER(isURI(?parent1) && isURI(?parent2))
        }
        LIMIT 100
        """

        results = self._query_sparql(query)
        bridges = []

        for result in results:
            concept = result['concept']['value']
            parent1 = result['parent1']['value']
            parent2 = result['parent2']['value']

            ns1 = self._get_namespace(parent1)
            ns2 = self._get_namespace(parent2)

            if ns1 != ns2:
                bridges.append({
                    'concept': self._get_local_name(concept),
                    'concept_uri': concept,
                    'namespaces': sorted([ns1, ns2]),
                    'parents': [self._get_local_name(parent1), self._get_local_name(parent2)]
                })

        logger.info(f"  Found {len(bridges)} bridge concepts")
        return bridges

    def _find_orphaned_concepts(self) -> List[Dict]:
        """Find concepts with minimal relationships"""
        query = """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?concept
        WHERE {
          ?concept a owl:Class .
          FILTER NOT EXISTS { ?concept rdfs:subClassOf ?parent . FILTER(?parent != owl:Thing) }
          FILTER NOT EXISTS { ?child rdfs:subClassOf ?concept }
        }
        LIMIT 100
        """

        results = self._query_sparql(query)
        orphans = []

        for result in results:
            uri = result['concept']['value']
            orphans.append({
                'name': self._get_local_name(uri),
                'uri': uri,
                'namespace': self._get_namespace(uri)
            })

        logger.info(f"  Found {len(orphans)} potentially orphaned concepts")
        return orphans

    def _get_ontology_statistics(self) -> Dict:
        """Get comprehensive ontology statistics"""
        stats = {}

        # Total classes
        query = "PREFIX owl: <http://www.w3.org/2002/07/owl#> SELECT (COUNT(DISTINCT ?c) as ?count) WHERE { ?c a owl:Class }"
        results = self._query_sparql(query)
        stats['total_classes'] = int(results[0]['count']['value']) if results else 0

        # Total properties
        query = """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        SELECT (COUNT(DISTINCT ?p) as ?count) WHERE {
          { ?p a owl:ObjectProperty } UNION { ?p a owl:DatatypeProperty }
        }
        """
        results = self._query_sparql(query)
        stats['total_properties'] = int(results[0]['count']['value']) if results else 0

        return stats

    def _create_report_html(self, stats: Dict, bridges: List, orphans: List) -> str:
        """Create HTML report"""
        bridge_items = '\n'.join([
            f"""<li class="concept-item bridge">
                <strong>{b['concept']}</strong>
                <span class="badge ns-{b['namespaces'][0]}">{b['namespaces'][0].upper()}</span>
                <span class="badge ns-{b['namespaces'][1]}">{b['namespaces'][1].upper()}</span>
                <br><small>Parents: {', '.join(b['parents'])}</small>
            </li>"""
            for b in bridges[:20]
        ])

        orphan_items = '\n'.join([
            f"""<li class="concept-item orphan">
                <strong>{o['name']}</strong>
                <span class="badge ns-{o['namespace']}">{o['namespace'].upper()}</span>
                <br><small>Consider adding relationships to integrate this concept</small>
            </li>"""
            for o in orphans[:20]
        ])

        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Ontology Analysis Report</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 20px; background: #f5f7fa; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 40px; border-radius: 12px; box-shadow: 0 2px 20px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; border-bottom: 4px solid #3498db; padding-bottom: 15px; margin-bottom: 30px; }}
        h2 {{ color: #34495e; margin-top: 40px; border-left: 4px solid #3498db; padding-left: 15px; }}
        .stats {{ display: flex; gap: 20px; flex-wrap: wrap; margin: 30px 0; }}
        .stat-box {{ flex: 1; min-width: 200px; padding: 25px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; color: white; text-align: center; }}
        .stat-number {{ font-size: 48px; font-weight: bold; margin-bottom: 10px; }}
        .stat-label {{ font-size: 14px; text-transform: uppercase; letter-spacing: 1px; opacity: 0.9; }}
        .concept-list {{ list-style: none; padding: 0; }}
        .concept-item {{ padding: 15px; margin: 10px 0; background: #f8f9fa; border-left: 4px solid #3498db; border-radius: 4px; transition: all 0.3s; }}
        .concept-item:hover {{ background: #e9ecef; transform: translateX(5px); }}
        .bridge {{ border-left-color: #f39c12; }}
        .orphan {{ border-left-color: #e74c3c; }}
        .badge {{ display: inline-block; padding: 4px 10px; border-radius: 4px; font-size: 11px; margin: 0 3px; font-weight: bold; }}
        .ns-gist {{ background: #3498db; color: white; }}
        .ns-dbc {{ background: #e74c3c; color: white; }}
        .ns-sow {{ background: #2ecc71; color: white; }}
        .ns-bridge {{ background: #f39c12; color: white; }}
        .ns-owl, .ns-rdf {{ background: #9b59b6; color: white; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🔍 Ontology Analysis Report</h1>

        <div class="stats">
            <div class="stat-box">
                <div class="stat-number">{stats.get('total_classes', 0)}</div>
                <div class="stat-label">Classes</div>
            </div>
            <div class="stat-box">
                <div class="stat-number">{stats.get('total_properties', 0)}</div>
                <div class="stat-label">Properties</div>
            </div>
            <div class="stat-box">
                <div class="stat-number">{len(bridges)}</div>
                <div class="stat-label">Bridge Concepts</div>
            </div>
            <div class="stat-box">
                <div class="stat-number">{len(orphans)}</div>
                <div class="stat-label">Isolated Concepts</div>
            </div>
        </div>

        <h2>🌉 Bridge Concepts (Cross-Ontology Connections)</h2>
        <p>Concepts that connect different ontologies, enabling semantic interoperability:</p>
        <ul class="concept-list">
            {bridge_items or '<li class="concept-item">No bridge concepts found</li>'}
        </ul>

        <h2>🔍 Isolated Concepts (Development Opportunities)</h2>
        <p>Concepts with minimal relationships - consider adding connections to improve the ontology:</p>
        <ul class="concept-list">
            {orphan_items or '<li class="concept-item">No isolated concepts found</li>'}
        </ul>
    </div>
</body>
</html>"""

    def close(self):
        """Close HTTP client"""
        if hasattr(self, 'client'):
            self.client.close()
