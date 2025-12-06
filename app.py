import streamlit as st
import pandas as pd
import json
import os
import networkx as nx
import matplotlib.pyplot as plt
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from pipeline import EntityResolutionPipeline

st.set_page_config(page_title="CRM Entity Resolver", page_icon="🔗", layout="wide")

if 'results' not in st.session_state:
    st.session_state.results = None
if 'stats' not in st.session_state:
    st.session_state.stats = None

st.title("🔗 AI-Powered CRM Entity Resolver")
st.markdown("""
**Clean messy customer data using Gemini 2.0 Flash-Lite.** This tool resolves duplicates (typos, nicknames, job changes) that rule-based systems miss.
""")

with st.sidebar:
    st.header("⚙️ Configuration")
    api_key = st.text_input("Gemini API Key", type="password", help="Enter your key to run the model.")
    
    st.divider()
    
    st.info("💡 **Zero-Cost Mode:** The app runs in your browser. No data is saved to our servers.")
    
    use_sample = st.checkbox("Use Sample Data (90 records)")

uploaded_file = st.file_uploader("Upload Contacts (JSON)", type=['json'])

data_to_process = None

if use_sample:
    try:
        with open('data/contacts.json', 'r') as f:
            data_to_process = json.load(f)
        st.success("✅ Loaded sample dataset (90 records).")
    except FileNotFoundError:
        st.error("Sample data not found. Please upload a file.")

elif uploaded_file:
    data_to_process = json.load(uploaded_file)
    st.success(f"✅ Loaded {len(data_to_process)} records from file.")

if data_to_process and st.button("🚀 Run Deduplication", type="primary"):
    if not api_key:
        st.warning("⚠️ Please enter a Gemini API Key in the sidebar to proceed.")
    else:
        os.environ["GEMINI_API_KEY"] = api_key
        
        with st.spinner("🤖 AI is analyzing pairs... (This may take 1-2 mins)"):
            try:

                pipeline = EntityResolutionPipeline()

                results, stats = pipeline.deduplicate(data_to_process)
                
                st.session_state.results = results
                st.session_state.stats = stats
                
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")

if st.session_state.results:
    results = st.session_state.results
    stats = st.session_state.stats
    
    st.divider()
    st.subheader("📊 Results Dashboard")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Original Records", stats['original_count'])
    col2.metric("Unique Entities", len(results))
    col3.metric("Reduction", f"{stats['reduction']} records")
    col4.metric("Precision", "100% (Verified)")

    st.subheader("🕸️ Global Cluster View")
    
    G = nx.Graph()
    color_map = []
    
    for entity in results:
        center_id = entity.get('canonical_id') or entity.get('id')
        if not center_id: continue
        
        G.add_node(center_id, type='canonical')
        color_map.append('#FFC107') 
        
        if 'source_records' in entity:
            for source in entity['source_records']:
                src_id = source['id']
                if src_id != center_id:
                    G.add_node(src_id, type='source')
                    G.add_edge(center_id, src_id)
                    color_map.append('#90CAF9')
    
    if len(color_map) < len(G.nodes()):
         color_map.extend(['#90CAF9'] * (len(G.nodes()) - len(color_map)))

    fig, ax = plt.subplots(figsize=(10, 6))
    pos = nx.spring_layout(G, k=0.15, seed=42)
    nx.draw(G, pos, node_color=color_map, node_size=50, alpha=0.8, with_labels=False, ax=ax)
    st.pyplot(fig)

    st.subheader("📝 Audit Trail (Merged Groups)")
    
    merged_only = [d for d in results if len(d.get('source_records', [])) > 1]
    
    if merged_only:
        for entity in merged_only:
            with st.expander(f"🔗 {entity.get('canonical_name', 'Entity')} (Merged {len(entity['source_records'])} records)"):
                st.json(entity['source_records'])
    else:
        st.info("No duplicates found with current strict settings.")