"""
ECharts-based Ontology Visualization
Beautiful, performant, and robust graph rendering using Apache ECharts
"""

import httpx
import json
from typing import Dict, List, Optional
import logging
from pathlib import Path
from collections import defaultdict

logger = logging.getLogger(__name__)


class EChartsOntologyExplorer:
    """Production-grade ontology visualization using Apache ECharts"""

    # Color scheme for different ontologies
    ONTOLOGY_COLORS = {
        'gist': '#3498db',      # Blue
        'dbc': '#e74c3c',       # Red
        'sow': '#2ecc71',       # Green
        'bridge': '#f39c12',    # Orange
        'owl': '#9b59b6',       # Purple
        'rdf': '#95a5a6',       # Gray
        'unknown': '#34495e'    # Dark gray
    }

    EDGE_COLORS = {
        'subClassOf': '#3498db',
        'seeAlso': '#e67e22',
        'equivalentClass': '#16a085',
        'domain': '#2ecc71',
        'range': '#f39c12',
        'default': '#95a5a6'
    }

    def __init__(
        self,
        fuseki_url: str = "http://localhost:3030",
        fuseki_dataset: str = "ontologies",
        username: str = "admin",
        password: str = "admin123"
    ):
        self.fuseki_url = fuseki_url.rstrip('/')
        self.fuseki_dataset = fuseki_dataset
        self.client = httpx.Client(
            auth=(username, password),
            timeout=30.0
        )

    def _query_sparql(self, query: str) -> List[Dict]:
        """Execute SPARQL query and return results"""
        endpoint = f"{self.fuseki_url}/{self.fuseki_dataset}/sparql"
        response = self.client.post(
            endpoint,
            data={'query': query},
            headers={'Accept': 'application/sparql-results+json'}
        )
        response.raise_for_status()
        results = response.json()
        return results['results']['bindings']

    def _get_namespace(self, uri: str) -> str:
        """Determine which ontology namespace a URI belongs to"""
        uri_lower = uri.lower()
        local_name = self._get_local_name(uri).lower()

        # Check more specific patterns first (order matters!)
        if any(x in local_name for x in ['databusinesscanvas', 'customersegment', 'valueproposition',
                                          'revenuestream', 'coststructure', 'dataasset', 'intelligencecapability']):
            return 'dbc'
        elif 'bridge' in uri_lower or any(x in local_name for x in ['channel', 'customerrelationship', 'keyresource',
                                                                      'keyactivity', 'keypartner', 'dataproduct',
                                                                      'dataservice', 'executivetarget']):
            return 'bridge'
        elif 'sow' in uri_lower or 'semanticsowcontract' in local_name or 'sowproject' in local_name:
            return 'sow'
        elif 'gist' in uri_lower and 'bridge' not in uri_lower:
            return 'gist'
        elif 'owl' in uri_lower or 'www.w3.org/2002/07/owl' in uri:
            return 'owl'
        elif 'rdf' in uri_lower or 'www.w3.org/1999/02/22-rdf-syntax-ns' in uri or 'www.w3.org/2000/01/rdf-schema' in uri:
            return 'rdf'
        else:
            return 'unknown'

    def _get_local_name(self, uri: str) -> str:
        """Extract local name from URI"""
        if '#' in uri:
            return uri.split('#')[-1]
        elif '/' in uri:
            return uri.split('/')[-1]
        return uri

    def create_interactive_graph(
        self,
        output_file: str = "ontology_echarts.html",
        max_concepts: int = 500
    ) -> str:
        """Create ECharts-based interactive visualization"""

        logger.info("🎨 Creating ECharts ontology visualization...")

        # Fetch classes
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

        # Build nodes data
        nodes = []
        node_lookup = {}
        category_counts = defaultdict(int)

        for cls in classes:
            uri = cls['class']['value']
            label = cls.get('label', {}).get('value', self._get_local_name(uri))
            comment = cls.get('comment', {}).get('value', '')

            namespace = self._get_namespace(uri)
            category_counts[namespace] += 1

            # ECharts node format
            node = {
                'id': uri,
                'name': label,
                'symbolSize': 40,
                'category': namespace,
                'value': comment[:200] if comment else label,
                'itemStyle': {
                    'color': self.ONTOLOGY_COLORS.get(namespace, self.ONTOLOGY_COLORS['unknown'])
                }
            }
            nodes.append(node)
            node_lookup[uri] = len(nodes) - 1

        # Fetch relationships
        logger.info("🔗 Fetching relationships...")
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
            ?subject rdfs:seeAlso ?object .
            BIND(rdfs:seeAlso as ?predicate)
          }}
          FILTER(isURI(?object))
        }}
        LIMIT 1000
        """

        relationships = self._query_sparql(relationships_query)
        logger.info(f"  Found {len(relationships)} relationships")

        # Build edges data
        edges = []
        edge_stats = defaultdict(int)

        for rel in relationships:
            subject = rel['subject']['value']
            predicate = rel['predicate']['value']
            obj = rel['object']['value']

            if subject in node_lookup and obj in node_lookup:
                pred_name = self._get_local_name(predicate)
                edge_stats[pred_name] += 1

                edge = {
                    'source': subject,
                    'target': obj,
                    'lineStyle': {
                        'color': self.EDGE_COLORS.get(pred_name, self.EDGE_COLORS['default']),
                        'width': 2 if pred_name == 'seeAlso' else 1.5,
                        'type': 'solid' if pred_name in ['subClassOf', 'seeAlso'] else 'dashed'
                    },
                    'label': {
                        'show': False,
                        'formatter': pred_name
                    }
                }
                edges.append(edge)

        # Create categories for legend
        categories = [
            {'name': f'{ns.upper()} ({category_counts[ns]})', 'itemStyle': {'color': color}}
            for ns, color in self.ONTOLOGY_COLORS.items()
            if category_counts[ns] > 0
        ]

        # Generate HTML with ECharts
        html_content = self._generate_html(nodes, edges, categories, edge_stats)

        output_path = Path(output_file)
        if not output_path.is_absolute():
            output_path = Path.cwd() / output_file

        output_path.write_text(html_content)
        logger.info(f"✅ Visualization saved: {output_path}")

        return str(output_path)

    def _generate_html(self, nodes: List, edges: List, categories: List, edge_stats: Dict) -> str:
        """Generate HTML with ECharts visualization"""

        nodes_json = json.dumps(nodes, indent=2)
        edges_json = json.dumps(edges, indent=2)
        categories_json = json.dumps(categories, indent=2)

        edge_stats_html = "<br>".join([f"{k}: {v}" for k, v in sorted(edge_stats.items())])

        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Ontology Explorer - ECharts</title>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
    <style>
        body {{
            margin: 0;
            padding: 0;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #f5f5f5;
        }}
        #container {{
            width: 100vw;
            height: 100vh;
            display: flex;
            flex-direction: column;
        }}
        #header {{
            background: white;
            padding: 15px 30px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            z-index: 100;
        }}
        #header h1 {{
            margin: 0;
            font-size: 24px;
            color: #2c3e50;
        }}
        #stats {{
            font-size: 12px;
            color: #7f8c8d;
            margin-top: 5px;
        }}
        #chart {{
            flex: 1;
            background: white;
            min-height: 800px;
        }}
        #info {{
            position: fixed;
            top: 80px;
            right: 20px;
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.15);
            max-width: 300px;
            font-size: 11px;
            color: #34495e;
        }}
        #info h3 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            color: #2c3e50;
        }}
        .stat-item {{
            margin: 3px 0;
        }}
    </style>
</head>
<body>
    <div id="container">
        <div id="header">
            <h1>🔍 Ontology Explorer</h1>
            <div id="stats">
                Nodes: {len(nodes)} | Edges: {len(edges)} | Interactive Force-Directed Graph
            </div>
        </div>
        <div id="chart"></div>
    </div>

    <div id="info">
        <h3>Relationships</h3>
        <div style="line-height: 1.6;">
            {edge_stats_html}
        </div>
        <div style="margin-top: 15px; padding-top: 10px; border-top: 1px solid #ecf0f1; color: #95a5a6;">
            💡 Click & drag nodes<br>
            🔍 Scroll to zoom<br>
            🖱️ Hover for details
        </div>
    </div>

    <script type="text/javascript">
        var chartDom = document.getElementById('chart');
        var myChart = echarts.init(chartDom);

        var option = {{
            title: {{
                show: false
            }},
            tooltip: {{
                formatter: function(params) {{
                    if (params.dataType === 'node') {{
                        return '<strong>' + params.data.name + '</strong><br>' +
                               '<span style="color: #7f8c8d">' + params.data.category.toUpperCase() + '</span><br>' +
                               '<div style="max-width: 300px; margin-top: 8px; color: #34495e;">' +
                               params.data.value + '</div>';
                    }} else if (params.dataType === 'edge') {{
                        return params.data.label.formatter;
                    }}
                }},
                backgroundColor: 'rgba(255, 255, 255, 0.95)',
                borderColor: '#ddd',
                borderWidth: 1,
                textStyle: {{
                    color: '#333',
                    fontSize: 12
                }},
                padding: 12
            }},
            legend: [{{
                orient: 'vertical',
                left: 20,
                top: 20,
                data: {categories_json}.map(function(cat) {{
                    return {{
                        name: cat.name,
                        itemStyle: cat.itemStyle
                    }};
                }}),
                backgroundColor: 'rgba(255, 255, 255, 0.9)',
                padding: 15,
                borderRadius: 8,
                borderColor: '#ddd',
                borderWidth: 1,
                textStyle: {{
                    fontSize: 11
                }}
            }}],
            animationDuration: 1500,
            animationEasingUpdate: 'quinticInOut',
            series: [{{
                type: 'graph',
                layout: 'force',
                data: {nodes_json},
                links: {edges_json},
                categories: {categories_json},
                roam: true,
                label: {{
                    show: true,
                    position: 'right',
                    formatter: '{{b}}',
                    fontSize: 10,
                    color: '#333'
                }},
                emphasis: {{
                    focus: 'adjacency',
                    lineStyle: {{
                        width: 4
                    }},
                    label: {{
                        fontSize: 12,
                        fontWeight: 'bold'
                    }}
                }},
                force: {{
                    repulsion: 1000,
                    gravity: 0.05,
                    edgeLength: 150,
                    layoutAnimation: true,
                    friction: 0.6
                }},
                lineStyle: {{
                    curveness: 0.1,
                    opacity: 0.5
                }},
                edgeSymbol: ['none', 'arrow'],
                edgeSymbolSize: 8
            }}]
        }};

        console.log('Setting ECharts option...');
        console.log('Nodes:', {len(nodes)});
        console.log('Edges:', {len(edges)});
        console.log('Chart container dimensions:', chartDom.offsetWidth, 'x', chartDom.offsetHeight);

        myChart.setOption(option);

        console.log('Option set successfully');

        // Force initial resize
        setTimeout(function() {{
            console.log('Forcing resize...');
            myChart.resize();
        }}, 100);

        // Responsive resize
        window.addEventListener('resize', function() {{
            myChart.resize();
        }});
    </script>
</body>
</html>"""

    def close(self):
        """Close HTTP client"""
        if hasattr(self, 'client'):
            self.client.close()
