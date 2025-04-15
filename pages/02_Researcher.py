import streamlit as st

from ptoolctext import load_extras

st.set_page_config(
    page_title="Researcher Page",
    page_icon="🔬",  # Ícone para a página do Pesquisador
    layout="wide"
)

header = st.container()
header.image('assets/Banner.png', width=400)
header.write("""<div class='fixed-header'/>""", unsafe_allow_html=True)

### Custom CSS for the sticky header
with open('assets/css/overides.css') as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

config, options = load_extras()

st.title("Researcher Page")
st.write("This page is designed for researchers to find specific studies.")

researcher_tabs = st.tabs([
    "Tab 2.1: PICOTSS Framework",
    "Tab 2.2: Observational Studies",
    "Tab 2.3: Interventional Studies",
    "Tab 2.4: Make my day"
])

with researcher_tabs[0]:
    st.header("PICOTSS Framework")
    st.write("Details of the framework:")
    st.write(
        "- **Population (P)**, **Intervention (I)**, **Comparison (C)**, **Outcome (O)**, **Timing (T)**, **Setting (S)** and **Study Design (S)**")
    st.write("*(Placeholder for framework information and/or charts)*")

with researcher_tabs[1]:
    st.header("Observational Studies")
    st.write("Information and structure of observational studies (Placeholder).")

with researcher_tabs[2]:
    st.header("Interventional Studies")
    st.write("Information and structure of interventional studies (Placeholder).")

with researcher_tabs[3]:
    st.header("Make my day")
    research_question = st.text_input("Enter the main research question:")
    if st.button("Search"):
        st.write(f"Results for the question: '{research_question}' (Placeholder)")