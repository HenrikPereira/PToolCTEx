import os
import pandas as pd
import streamlit as st
import plotly.express as px
from utils.auxiliary import *

st.set_page_config(layout="wide")

header = st.container()
header.image('assets/Banner.png', width=400)
header.write("""<div class='fixed-header'/>""", unsafe_allow_html=True)

### Custom CSS for the sticky header
with open('assets/css/overides.css') as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

config, options = load_extras()

st.title("Researcher Page")

st.write("This page helps researchers explore clinical trials based on different frameworks, study types and even by "
         "typing a custom research question.")


# Load Data
data_path = os.path.join("sources", "full_df.parquet")
try:
    df = pd.read_parquet(data_path)
    CT_data = load_df_parquet(data_path)
except Exception as e:
    st.error(f"Error loading dataset: {e}")
    st.stop()

# Tabs Configuration
researcher_tabs = st.tabs([
    "Tab 2.1: PICOTSS Framework",
    "Tab 2.2: Make my day"
])

# TAB 2.1: PICOTSS Framework
with researcher_tabs[0]:
    st.header("PICOTSS Framework")
    st.write("Explore clinical studies based on Population (P), Intervention (I), Comparison (C), Outcome (O), Timing (T), Setting (S) and Study Design (S).")

    # Filters
    st.subheader("🔎 Filters")
    colf1, colf2, colf3, colf4 = st.columns(4)

    age_filter = colf1.selectbox("Age group", ["All", "0-17 years", "18-64 years", "65+ years"])
    sex_filter = colf2.selectbox("Sex", ["All", "Female", "Male"])
    condition_filter = colf3.text_input("Condition / Disease")
    intervention_filter = colf4.text_input("Intervention")

    colf5, colf6 = st.columns(2)
    other_term = colf5.text_input("Other search term (keywords)")
    outcome_filter = colf6.text_input("Outcome measure")

    colf7, colf8 = st.columns(2)
    study_type_filter = colf7.selectbox("Study Type", ["All", "Interventional", "Observational"])
    study_status_filter = colf8.selectbox("Study Status", ["All", "Recruiting", "Ongoing", "Completed", "Expanded Access"])

    # Applying filters
    df_filtered = df.copy()

    # Age filter
    if age_filter == "0-17 years":
        df_filtered = df_filtered[df_filtered['Age_0_17_years'] == True]
    elif age_filter == "18-64 years":
        df_filtered = df_filtered[df_filtered['Age_18_64_years'] == True]
    elif age_filter == "65+ years":
        df_filtered = df_filtered[df_filtered['Age_65p_years'] == True]

    # Sex filter
    if sex_filter == "Female":
        df_filtered = df_filtered[df_filtered['Gender_F'] == True]
    elif sex_filter == "Male":
        df_filtered = df_filtered[df_filtered['Gender_M'] == True]

    # Therapeutic area filter
    df_filtered['therapeutic_area'] = df_filtered['therapeutic_area'].apply(parse_list_str)
    if condition_filter:
        df_filtered = df_filtered[
            df_filtered['therapeutic_area'].apply(lambda x: any(condition_filter.lower() in str(i).lower() for i in x) if isinstance(x, list) else False)
        ]

    # Intervention filter
    df_filtered['interventions'] = df_filtered['interventions'].apply(parse_list_str)
    if condition_filter:
        df_filtered = df_filtered[
            df_filtered['interventions'].apply(lambda x: any(condition_filter.lower() in str(i).lower() for i in x) if isinstance(x, list) else False)
        ]

    # Other search term filter (keywords)
    if other_term:
        df_filtered = df_filtered[df_filtered['keywords'].apply(lambda x: other_term.lower() in str(x).lower())]

    # Outcome measure filter
    if outcome_filter and 'outcome_measures' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['outcome_measures'].apply(lambda x: outcome_filter.lower() in str(x).lower())]

    # Study type
    if study_type_filter != "All":
        df_filtered = df_filtered[df_filtered['study_type'].str.lower() == study_type_filter.lower()]

    # Study status
    if study_status_filter != "All":
        df_filtered = df_filtered[df_filtered['status'].str.contains(study_status_filter, case=False, na=False)]

    # Results
    st.subheader(f"Findings: {len(df_filtered):,} trials")
    with st.expander("See table with details", expanded=True):
        st.dataframe(df_filtered[['title', 'start_date', 'therapeutic_area', 'interventions', 'study_type', 'status']].sort_values(by='start_date', ascending=False))
        csv = df_filtered.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download CSV", csv, "filtered_studies.csv", "text/csv")

# TAB 2.2: Make my day
with researcher_tabs[1]:
    st.header("🔍 Make my day!")
    st.write('This section tries to match the User\'s desired clinical context to the best scoring entries in the '
             'Clinical Trials Database. '
             'Uses 2 different LLMs (Large Language Models) with appropriate prompt engineering to generate a response'
             'ranking the best matching studies based on the user\'s context.')

    with st.container(border=True):
        cols = st.columns([0.2, .8])
        with cols[0]:
            st.write("Additional parameters for inference")
            st.selectbox('Type of trial sampling', ['recent', 'sample'], key='prefilter_type_trials')
            st.number_input(
                'Chunks when prefiltering', min_value=1, max_value=1000, value=500,
                key='prefilter_chunk_size'
            )
            st.slider(
                'Proportion of dataset to preselect', min_value=0.01, max_value=1.0, value=0.75, step=0.01,
                key='prefilter_proportion'
            )
            st.slider(
                'Certainty cutoff', min_value=0.01, max_value=1.0, value=2/3, step=0.01,
                key='certainty_cutoff'
            )
            st.slider(
                'Prefilter "Temperature"', min_value=0.01, max_value=1.0, value=0.1, step=0.01,
                key='prefilter_temperature'
            )
            st.slider(
                'Final "Temperature"', min_value=0.01, max_value=1.0, value=1.0, step=0.01,
                key='final_temperature'
            )

        with cols[1]:
            avail_models = get_groq_models()

            with st.expander("Sample of full Clinical Trial Database", icon='💽', expanded=False):
                st.write(CT_data.columns)
                st.dataframe(CT_data.sample(5, random_state=123), hide_index=True, selection_mode='Row')

            with st.expander("Available Models", icon='🧑‍💻'):
                st.write('These models are provided by the GroQ platform. You can find more information about them at [GroQ Docs](https://console.groq.com/docs/models)')
                st.dataframe(avail_models, hide_index=True, selection_mode='Row')

            cols = st.columns(2)
            with cols[0]:
                st.selectbox(
                    'LLM for preanalysis of Clinical Trial Database',
                    options=avail_models['id'].to_list(),
                    index=avail_models['id'].to_list().index('llama-3.1-8b-instant'),
                    key='prefilter_model'
                )
            with cols[1]:
                st.selectbox(
                    'LLM for final inference',
                    options=avail_models['id'].to_list(),
                    index=avail_models['id'].to_list().index('deepseek-r1-distill-llama-70b'),
                    key='final_model'
                )

            with st.expander("Prompt Engineering and Role\'s atributes", icon='🧑‍🔬'):
                st.write('This is how the LLM is for the User\'s prompt:')
                st.caption(f"> {format_user_prompt_template('{prompt}')}")
                st.write('This is how the LLM is asked to make a first selection of studies to be feed to the final inference LLM:')
                st.caption(f"> {format_system_prefilter_role_template('{trials}')}")
                st.write('This is how the LLM is asked to finally select the best matches for the User\'s context:')
                st.caption(f"> {format_system_final_role_template('{trials}')}")

            st.divider()

            cols = st.columns([0.9, 0.1], vertical_alignment='center')
            with cols[0]:
                user_query = st.text_area(
                    "Enter your research question:",
                    value="Adult female with 32 years diagnosed with Triple Negative Breast Cancer, with failed first line of chemotherapy, refractory to platin based treatments",
                )
            with cols[1]:
                submit_button = st.button("GO! 🎯")

    if user_query and submit_button:
        st.write(f"Searching for studies related to: **{user_query}**")

        with st.spinner("... Making the AI do the work for you ..."):
            st.markdown("But remember... in the so called *'Human in the loop'*, "
                        "you **ARE the HUMAN**!")
            st.markdown("...So take it with a grain of salt...")
            resultado = get_trial_recommendation_groq(
                user_query,
                CT_data,
                chunk_size=st.session_state.prefilter_chunk_size,
                type_trials=st.session_state.prefilter_type_trials,
                proportion=st.session_state.prefilter_proportion,
                certainty_cutoff=st.session_state.certainty_cutoff,
                temperature_pf=st.session_state.prefilter_temperature,
                temperature_f=st.session_state.final_temperature,
            )
        if isinstance(resultado, pd.DataFrame):
            st.write(resultado)
        else:
            st.write(f"No relevant results found...")

        # st.subheader(f"🔎  {len(matched_df):,} studies found")
        # st.dataframe(matched_df[['title', 'start_date', 'therapeutic_area', 'interventions', 'study_type', 'status']].sort_values(by='start_date', ascending=False))
