import streamlit as st
import tomllib

# function to load extra options and overrides
@st.cache_data
def load_extras():
    with open(".streamlit/config.toml", "rb") as f:
        _config = tomllib.load(f)
    with open(".streamlit/options.toml", "rb") as f:
        _options = tomllib.load(f)

    return _config, _options

home = st.Page("pages/00_Home.py", title="Home | PToolCTex", icon="🏠", default=True)

infography = st.Page("pages/01_Infography.py", title="Infography", icon="📊")

researcher = st.Page("pages/02_Researcher.py", title="Researcher", icon="🔬")

#clinic = st.Page("pages/03_Clinician.py", title="Clinician", icon="🩺")

pg = st.navigation(
    {
        "Home": [home],
        "Infography": [infography],
        "POV": [researcher],
    }
)

pg.run()
