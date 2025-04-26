import os
import pandas as pd
import streamlit as st
import plotly.express as px

from ptoolctext import load_extras
from utils.parsing import parse_list_str

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

st.write("This page helps researchers explore clinical trials based on different frameworks, study types and even by typing a custom research question.")


# Load Data
data_path = os.path.join("sources", "full_df.parquet")
try:
    df = pd.read_parquet(data_path)
except Exception as e:
    st.error(f"Error loading dataset: {e}")
    st.stop()

# Tabs Configuration
researcher_tabs = st.tabs([
    "Tab 2.1: PICOTSS Framework",
    "Tab 2.2: Observational Studies",
    "Tab 2.3: Interventional Studies",
    "Tab 2.4: Make my day"
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

# TAB 2.2: Observational Studies
with researcher_tabs[1]:
    st.header("Observational Studies")
    df_obs = df[df['study_type'] == 'OBSERVATIONAL'].copy()
    st.metric("Total Observational Studies", len(df_obs))

    df_obs['therapeutic_area'] = df_obs['therapeutic_area'].apply(parse_list_str)
    if condition_filter:
        df_obs = df_obs[
            df_obs['therapeutic_area'].apply(lambda x: any(condition_filter.lower() in str(i).lower() for i in x) if isinstance(x, list) else False)
        ]

    st.subheader("Table - Observational Studies")
    with st.expander("Expand table", expanded=True):
        st.dataframe(df_obs[['title', 'start_date', 'therapeutic_area', 'status']].sort_values(by='start_date', ascending=False))
        csv_obs = df_obs.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download CSV", csv_obs, "observational_studies.csv", "text/csv")

# TAB 2.3: Interventional Studies
with researcher_tabs[2]:
    st.header("Interventional Studies")
    df_int = df[df['study_type'] == 'INTERVENTIONAL'].copy()
    st.metric("Total Interventional Studies", len(df_int))
    def derive_study_phase(row):
        phases = []
        if row['trial_Early_Phase_I']: phases.append("Early Phase I")
        if row['trial_Phase_I']: phases.append("Phase I")
        if row['trial_Phase_II']: phases.append("Phase II")
        if row['trial_Phase_III']: phases.append("Phase III")
        if row['trial_Phase_IV']: phases.append("Phase IV")
        return "/".join(phases) if phases else "Not Available"

    df_int['study_phase'] = df_int.apply(derive_study_phase, axis=1)


    df_int['therapeutic_area'] = df_int['therapeutic_area'].apply(parse_list_str)
    if condition_filter:
        df_int = df_int[
            df_int['therapeutic_area'].apply(lambda x: any(condition_filter.lower() in str(i).lower() for i in x) if isinstance(x, list) else False)
        ]


    st.subheader("Table - Interventional Studies")
    with st.expander("Expand table", expanded=True):
        st.dataframe(df_int[['title', 'start_date', 'therapeutic_area', 'study_phase', 'status']].sort_values(by='start_date', ascending=False))
        csv_int = df_int.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download CSV", csv_int, "interventional_studies.csv", "text/csv")

# TAB 2.4: Make my day
with researcher_tabs[3]:
    st.header("🔍 Make my day")
    user_query = st.text_input("Enter your research question:")
    if user_query:
        st.write(f"Searching for studies related to: **{user_query}**")
        def find_relevant_studies(query, df):
            fields = ['title', 'therapeutic_area', 'interventions', 'keywords']
            mask = pd.Series([False] * len(df))
            for field in fields:
                mask = mask | df[field].apply(lambda x: query.lower() in str(x).lower())
            return df[mask]

        matched_df = find_relevant_studies(user_query, df)

        matched_df['therapeutic_area'] = matched_df['therapeutic_area'].apply(parse_list_str)
        matched_df['interventions'] = matched_df['interventions'].apply(parse_list_str)
        if condition_filter:
            matched_df = matched_df[
                matched_df['therapeutic_area'].apply(lambda x: any(condition_filter.lower() in str(i).lower() for i in x) if isinstance(x, list) else False),
                matched_df['interventions'].apply(lambda x: any(condition_filter.lower() in str(i).lower() for i in x) if isinstance(x, list) else False)
            ]

        st.subheader(f"🔎  {len(matched_df):,} studies found")
        st.dataframe(matched_df[['title', 'start_date', 'therapeutic_area', 'interventions', 'study_type', 'status']].sort_values(by='start_date', ascending=False))
