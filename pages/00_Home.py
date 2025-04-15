import streamlit as st
from streamlit_extras.badges import badge
from streamlit_extras.bottom_container import bottom

st.set_page_config(
    page_title="PToolCTex",
    page_icon="🏠",  # Ícone para a página inicial
    layout="wide"
)

### Custom CSS for the sticky header
with open('assets/css/overides.css') as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Página principal - Home
st.image('assets/Main.png')
st.title("Welcome to PToolCText")
st.write("""
    A project issued for **LAB-HIDA**.
    
    ***PhD programme HEADS** of **FMUP** in collaboration with **CINTESIS***
    
    ## **Objective**
    
    This application allows explore clinical research that is done in Portugal.
    Use the menu on the left to navigate through the available sections:
    - **Infography:** View information and trends from the studies, either in EDA mode or in a visual representation.
    - **Researcher POV:** Interactive tool to locate specific studies based on research questions.
    - **Clinician POV:** Interactive tool to find studies based on patient characteristics, conditions, or 
    specific interventions.
    
""")

with st.sidebar:
    with st.expander("Updater"):
        st.button("European CT")
        st.button("USA CT")
        st.button("INFARMED PAPs")

with bottom():
    badge(type='github', name='HenrikPereira/PToolCTEx')
