import streamlit as st

home = st.Page("pages/00_Home.py", title="Home | PToolCTex", icon="🏠", default=True)

settings = st.Page("pages/XX_Settings.py", title="Settings", icon="📦")

infography = st.Page("pages/01_Infography.py", title="Infography", icon="📊")

researcher = st.Page("pages/02_Researcher.py", title="Researcher", icon="🔬")

#clinic = st.Page("pages/03_Clinician.py", title="Clinician", icon="🩺")

pg = st.navigation(
    {
        "Home": [home, settings],
        "Infography": [infography],
        "POV": [researcher],
    }
)

pg.run()
