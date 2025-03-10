import streamlit as st

from ptoolctext import load_extras

st.set_page_config(
    page_title="Clinician Page",
    page_icon="🩺",  # Ícone para a página do Clínico
    layout="wide"
)

header = st.container()
header.image('assets/Banner.png', width=400)
header.write("""<div class='fixed-header'/>""", unsafe_allow_html=True)

### Custom CSS for the sticky header
with open('assets/css/overides.css') as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

config, options = load_extras()

st.title("Clinician Page")
st.write(
    "This page is designed for clinicians to find studies related to a specific patient, condition, or intervention.")

clinician_tabs = st.tabs([
    "Tab 3.1: Main Filters",
    "Tab 3.2: Make my day"
])

with clinician_tabs[0]:
    st.header("Main Filters")
    st.write("Use the filters to refine your study search:")
    age = st.slider("Age", 0, 100, 30)
    sex = st.selectbox("Gender", options=["Male", "Female", "Other"])
    condition = st.text_input("Condition/Disease")
    intervention = st.text_input("Intervention")
    other_term = st.text_input("Other search term")
    outcome = st.text_input("Outcome measure")
    study_type = st.selectbox("Study Type", options=["Observational", "Interventional"])
    study_status = st.selectbox("Study Status", options=["Recruiting", "Ongoing", "Completed", "Expanded Access"])
    result_availability = st.selectbox("Results Availability", options=["Yes", "No"])
    if st.button("Search Studies"):
        st.write("Displaying studies matching the selected filters (Placeholder)")

with clinician_tabs[1]:
    st.header("Make my day")
    patient_problem = st.text_input("Enter the patient's primary problem:")
    if st.button("Search Patient Problem"):
        st.write(f"Results for the problem: '{patient_problem}' (Placeholder)")