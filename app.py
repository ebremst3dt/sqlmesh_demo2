import streamlit as st
import networkx as nx
import pandas as pd
import plotly.graph_objects as go
from pyvis.network import Network
import json
import tempfile

def create_network_graph(df):
    """Create a NetworkX graph from parent-child relationships"""
    G = nx.DiGraph()

    # Add all nodes and edges
    for _, row in df.iterrows():
        G.add_edge(row['parent_name'], row['name'])

    return G

def plot_network_plotly(G):
    """Create an interactive network visualization using Plotly"""
    pos = nx.spring_layout(G)

    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines')

    node_x = []
    node_y = []
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        hoverinfo='text',
        text=[node for node in G.nodes()],
        textposition="bottom center",
        marker=dict(
            showscale=True,
            colorscale='YlGnBu',
            size=20,
            colorbar=dict(
                thickness=15,
                title='Node Connections',
                xanchor='left',
                titleside='right'
            )
        )
    )

    # Color nodes by number of connections
    node_adjacencies = []
    for node in G.nodes():
        node_adjacencies.append(len(list(G.neighbors(node))))

    node_trace.marker.color = node_adjacencies

    fig = go.Figure(data=[edge_trace, node_trace],
                   layout=go.Layout(
                       showlegend=False,
                       hovermode='closest',
                       margin=dict(b=0, l=0, r=0, t=0),
                       xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                       yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                   )

    return fig

def create_sankey(df):
    """Create a Sankey diagram from parent-child relationships"""
    # Create unique node list
    nodes = list(set(df['parent_name'].tolist() + df['name'].tolist()))
    node_indices = {node: i for i, node in enumerate(nodes)}

    # Create source and target lists
    sources = [node_indices[parent] for parent in df['parent_name']]
    targets = [node_indices[child] for child in df['name']]

    # Create Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=nodes,
            color="blue"
        ),
        link=dict(
            source=sources,
            target=targets,
            value=[1] * len(sources)  # Equal weight for all connections
        )
    )])

    fig.update_layout(title_text="Parent-Child Relationships", font_size=10)
    return fig

def main():
    st.title("Parent-Child Relationship Visualizer")    # File upload
    uploaded_file = st.file_uploader("Upload CSV file with 'name' and 'parent_name' columns", type="csv")

    # Sample data option
    if st.checkbox("Use sample data"):
        sample_data = {
            'name': ['test', 'detail_item_group', 'main_item_group'],
            'parent_name': ['sllclockdb01_dc_sll_se_rainbow_ds_rainbow_dig',
                          'sllclockdb01_dc_sll_se_rainbow_ds_rainbow_dig',
                          'sllclockdb01_dc_sll_se_rainbow_ds_rainbow_mig']
        }
        df = pd.DataFrame(sample_data)
    elif uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
    else:
        st.info("Please upload a CSV file or use sample data")
        return

    # Visualization type SELECT TOP 1000or
    viz_type = st.SELECT TOP 1000box(
        "SELECT TOP 1000 visualization type",
        ["Network Graph", "Sankey Diagram"]
    )

    # Create and display visualization
    if viz_type == "Network Graph":
        G = create_network_graph(df)
        fig = plot_network_plotly(G)
        st.plotly_chart(fig, use_container_width=True)
    else:
        fig = create_sankey(df)
        st.plotly_chart(fig, use_container_width=True)

    # Display data table
    st.subheader("Data Preview")
    st.dataframe(df)

if __name__ == "__main__":
    main()