"""
Visualize Ontologies with Pyvis
Create interactive graph visualization of all loaded ontologies
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import logging
from src.agentic_graph_middleware.visualization.pyvis_explorer import PyvisOntologyExplorer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    print("=" * 80)
    print("🎨 Interactive Ontology Visualization with Pyvis")
    print("=" * 80)
    print()

    # Initialize explorer
    explorer = PyvisOntologyExplorer(
        fuseki_url="http://localhost:3030",
        fuseki_dataset="ontologies"
    )

    try:
        print("📊 Creating interactive graph visualization...")
        print()

        # Create main visualization
        viz_file = explorer.create_interactive_graph(
            output_file="ontology_explorer.html",
            height="1000px",
            max_concepts=300
        )

        print()
        print("✅ Interactive visualization created!")
        print(f"📁 File: {viz_file}")
        print()

        # Generate analysis report
        print("📋 Generating analysis report...")
        report_file = explorer.generate_analysis_report(
            output_file="ontology_analysis.html"
        )

        print()
        print("✅ Analysis report generated!")
        print(f"📁 File: {report_file}")
        print()

        print("=" * 80)
        print("🎉 Visualization Complete!")
        print("=" * 80)
        print()
        print("🌐 Open these files in your browser:")
        print(f"   1. {viz_file}")
        print(f"   2. {report_file}")
        print()
        print("💡 Tips:")
        print("   • Hover over nodes to see details")
        print("   • Click and drag to explore relationships")
        print("   • Use mouse wheel to zoom in/out")
        print("   • Different colors represent different ontologies")
        print("   • Look for orange nodes - they bridge ontologies!")
        print()

    finally:
        explorer.close()


if __name__ == "__main__":
    main()
