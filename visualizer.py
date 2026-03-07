"""Visualizer agent: builds graph and tabular visuals for knowledge base state."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import json

try:
    import matplotlib.pyplot as plt  # type: ignore
    import networkx as nx  # type: ignore
except Exception:  # pragma: no cover
    plt = None
    nx = None

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None


class VisualizerAgent:
    """Renders relation graph and entity summary chart; exports fallback JSON if libs unavailable."""

    def __init__(self, out_dir: str = "output"):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def visualize(self, kb: Dict[str, Any]) -> Dict[str, str]:
        if plt is not None and nx is not None:
            graph_path = self._plot_graph(kb)
            table_path = self._plot_entity_distribution(kb)
            return {"graph": graph_path, "distribution": table_path}

        fallback = self.out_dir / "visualization_fallback.json"
        fallback.write_text(
            json.dumps(
                {
                    "message": "matplotlib/networkx unavailable, exported graph data instead",
                    "nodes": [e["id"] for e in kb.get("entities", [])] + [ev["id"] for ev in kb.get("events", [])],
                    "edges": kb.get("relations", []),
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return {"graph": str(fallback), "distribution": ""}

    def _plot_graph(self, kb: Dict[str, Any]) -> str:
        G = nx.DiGraph()

        for ent in kb.get("entities", []):
            label = f"{ent['name']}\n({ent['type']})"
            if any(v == "missing" for v in ent.get("placeholders", {}).values()):
                label += "\n[placeholder]"
            G.add_node(ent["id"], label=label, kind="entity")

        for evt in kb.get("events", []):
            G.add_node(evt["id"], label=f"{evt['id']}\n(event)", kind="event")

        for rel in kb.get("relations", []):
            G.add_edge(rel["source"], rel["target"], relation=rel["relation"])

        plt.figure(figsize=(10, 7))
        pos = nx.spring_layout(G, seed=42)
        node_labels = nx.get_node_attributes(G, "label")
        node_colors = ["#8dd3c7" if G.nodes[n]["kind"] == "entity" else "#fb8072" for n in G.nodes]

        nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=1700)
        nx.draw_networkx_edges(G, pos, arrows=True, alpha=0.7)
        nx.draw_networkx_labels(G, pos, labels=node_labels, font_size=8)
        edge_labels = {(u, v): d["relation"] for u, v, d in G.edges(data=True)}
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=7)

        plt.title("Knowledge Base Relationship Graph")
        plt.axis("off")
        graph_path = str(self.out_dir / "knowledge_graph.png")
        plt.tight_layout()
        plt.savefig(graph_path, dpi=180)
        plt.close()
        return graph_path

    def _plot_entity_distribution(self, kb: Dict[str, Any]) -> str:
        if pd is None:
            return ""
        df = pd.DataFrame(kb.get("entities", []))
        if df.empty:
            return ""
        counts = df["type"].value_counts().sort_values(ascending=False)

        plt.figure(figsize=(7, 4))
        counts.plot(kind="bar", color="#80b1d3")
        plt.title("Entity Type Distribution")
        plt.xlabel("Entity Type")
        plt.ylabel("Count")
        plt.tight_layout()

        out_path = str(self.out_dir / "entity_distribution.png")
        plt.savefig(out_path, dpi=180)
        plt.close()
        return out_path
