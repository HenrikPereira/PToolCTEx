import os
import pandas as pd
import streamlit as st
import plotly.express as px
from streamlit_extras.metric_cards import style_metric_cards
from utils.parsing import parse_list_str

from ptoolctext import load_extras

st.set_page_config(
    page_title="Infography",
    page_icon="📊",  # Ícone para a página de Infográficos
    layout="wide"
)

# Define the data file path (assuming the notebook is running from the "etl" folder)
data_path = os.path.join("sources", "full_df.parquet")

# Load the parquet file into a DataFrame
try:
    df = pd.read_parquet(data_path)
except Exception as e:
    st.error(f"Error loading data from {data_path}: {e}")
    st.stop()

header = st.container()
header.image('assets/Banner.png', width=400)
header.write("""<div class='fixed-header'/>""", unsafe_allow_html=True)

### Custom CSS for the sticky header
with open('assets/css/overides.css') as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

config, options = load_extras()

st.title("Infography")
st.write("This area presents a visual representation of the data.")

### Main metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total of Studies", f"{len(df):,}")
col2.metric("Total of Participants", f"{int(df['enrollment'].sum(skipna=True)):,}")
col3.metric("Randomized Clinical Trials", df['trial_design_Randomised'].sum(skipna=True))
col4.metric("Number of Sources", df['source_dataset'].nunique())

style_metric_cards(border_left_color="#1f77b4")

# Internal tabs for the Infographics section
infograph_tabs = st.tabs([
    "Tab 1.1: Overview",
    "Tab 1.2: Clinical Trials - EU",
    "Tab 1.3: Clinical Trials - GOV",
    "Tab 1.4: Infarmed/PAP"
])

with infograph_tabs[0]:
    st.header("Overview")
    st.write("General information about the presented data (brief description).")

    # -------------------------------
    # Multiple selectors for filtering
    # -------------------------------

    # Selector for study types
    with st.sidebar:
        st.subheader("Filters")
        if "study_type" in df.columns:
            study_types_options = sorted(df["study_type"].dropna().unique().tolist())
            selected_study_types = st.multiselect(
                "Select Study Types",
                study_types_options,
                default=[],
                label_visibility="visible"
            )
        else:
            st.warning("Column 'study_type' not available.")
            selected_study_types = None

        # Selector for therapeutic areas (represented as 'therapeutic_area')
        if "therapeutic_area" in df.columns:
            therapeutic_areas_options = sorted(df["therapeutic_area"].dropna().unique().tolist())
            selected_therapeutic_areas = st.multiselect(
                "Select Therapeutic Areas",
                therapeutic_areas_options,
                default=[],
                label_visibility="visible"
            )
        else:
            st.warning("Column 'therapeutic_area' not available.")
            selected_therapeutic_areas = None

    # Apply filters to DataFrame
    filtered_df = df.copy()
    if selected_study_types is not None and len(selected_study_types) > 0:
        filtered_df = filtered_df[filtered_df["study_type"].isin(selected_study_types)]
    if selected_therapeutic_areas is not None and len(selected_therapeutic_areas) > 0:
        filtered_df = filtered_df[filtered_df["therapeutic_area"].isin(selected_therapeutic_areas)]

    st.markdown("---")

    # ─── Study Phase ───────────────────────────────────────────────────────
    st.subheader("Distribution by Study Phase")
    phase_counts = pd.DataFrame({
        "Phase": ["Early I", "I", "II", "III", "IV"],
        "Total": [
            df['trial_Early_Phase_I'].sum(skipna=True),
            df['trial_Phase_I'].sum(skipna=True),
            df['trial_Phase_II'].sum(skipna=True),
            df['trial_Phase_III'].sum(skipna=True),
            df['trial_Phase_IV'].sum(skipna=True),
        ]
    })
    fig = px.bar(phase_counts, x="Phase", y="Total", text="Total")
    st.plotly_chart(fig, use_container_width=True)

    # ─── Temporal Trend ──────────────────────────────────────────────────
    st.subheader("Temporal trend of studies")
    if "study_first_submitted_date" in filtered_df.columns:
        filtered_df['study_first_submitted_date'] = pd.to_datetime(filtered_df['study_first_submitted_date'], errors='coerce')
        # Group by year and count studies
        trend = filtered_df.groupby(filtered_df['study_first_submitted_date'].dt.year).size().reset_index(name='count')
        trend = trend.dropna()
        fig_trend = px.line(trend, x='study_first_submitted_date', y='count',
                            labels={"study_first_submitted_date": "Year", "count": "Number of Studies"},
                            template="simple_white")
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.write("Column 'study_first_submitted_date' not available.")

    col3, col4 = st.columns(2)
    # ─── Study Types ──────────────────────────────────────────────────
    col3.write("Study types")
    if "study_type" in filtered_df.columns:
        study_type_counts = filtered_df["study_type"].value_counts().reset_index()
        study_type_counts.columns = ["study_type", "count"]
        fig_study = px.bar(study_type_counts, y="study_type", x="count",
                           labels={"study_type": "Study Type", "count": "Count"},
                           template="simple_white",)
        col3.plotly_chart(fig_study, use_container_width=True)
    else:
        col3.write("Column 'study_type' not available.")

    # ─── Blinding (MASKING) ──────────────────────────────────────────────────
    col4.write("Blinding Type (Masking)")
    masking_data = {
        "Open": df['masking_OPEN'].sum(skipna=True),
        "Single-blind": df['masking_SINGLE'].sum(skipna=True),
        "Double-blind": df['masking_DOUBLE'].sum(skipna=True),
    }
    fig = px.pie(names=masking_data.keys(), values=masking_data.values(), hole=0.4)
    col4.plotly_chart(fig, use_container_width=True)

    # ─── Therapeutic Areas ─────────────────────────────────────────────────────
    st.subheader("Therapeutic Areas (Top 10)")

    df['therapeutic_area'] = df['therapeutic_area'].apply(parse_list_str)

    # Explode and Count each element frequency
    ther_areas_exploded = df.explode('therapeutic_area')['therapeutic_area'].dropna()
    top_areas = ther_areas_exploded.value_counts().head(10).reset_index()
    top_areas.columns = ['Area', 'Total']

    # Plot
    fig = px.bar(top_areas, x="Area", y="Total", text="Total")
    st.plotly_chart(fig, use_container_width=True)


    # ─── DOWNLOAD ──────────────────────────────────────────────────────
    st.subheader("📥 Export table with all the studies")

    cols_to_show = ['title', 'start_date', 'source_dataset', 'therapeutic_area', 'enrollment']
    available_cols = [col for col in cols_to_show if col in df.columns]

    # Copy and format 'therapeutic_area' column as string
    df_export = df[available_cols].copy()

    if 'therapeutic_area' in df_export.columns:
        df_export['therapeutic_area'] = df_export['therapeutic_area'].apply(
            lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x
        )

    with st.expander("See table with all the studies", expanded=True):
        st.dataframe(df_export.sort_values(by='start_date', ascending=False))

        csv = df_export.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download CSV", csv, "clinical_trials.csv", "text/csv")

    st.markdown("---")


with infograph_tabs[1]:
    st.header("Clinical Trials - EU")
    df_eu = df[df['source_dataset'] == 'clinicaltrials.eu'].copy()

    st.markdown(f"**Total of EU clinical trials:** {len(df_eu):,}")

    # Sponsors
    st.markdown("#### Distribution by Sponsor")
    sponsor_counts = df_eu['Sponsor_type'].value_counts().head(10).reset_index()
    sponsor_counts.columns = ['Sponsor type', 'Total']
    st.plotly_chart(px.bar(sponsor_counts, x='Sponsor type', y='Total', text='Total'), use_container_width=True)

    # Status
    st.markdown("#### Status of the Studies")
    if 'status' in df_eu.columns:
        status_counts = df_eu['status'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Total']
        st.plotly_chart(px.pie(status_counts, names='Status', values='Total', hole=0.4), use_container_width=True)

    # Phases
    st.markdown("#### Studies by Phase")
    phases = {
        "I": df_eu['trial_Phase_I'].sum(),
        "II": df_eu['trial_Phase_II'].sum(),
        "III": df_eu['trial_Phase_III'].sum(),
        "IV": df_eu['trial_Phase_IV'].sum(),
    }
    st.plotly_chart(px.bar(x=list(phases.keys()), y=list(phases.values()), labels={'x': 'Phase', 'y': 'Total'}))

    # Temporal trend
    st.markdown("#### Temporal trend of the studies")
    df_eu['start_year'] = pd.to_datetime(df_eu['start_date'], errors='coerce').dt.year
    yearly = df_eu['start_year'].value_counts().sort_index()
    st.plotly_chart(px.line(x=yearly.index, y=yearly.values, labels={'x': 'Year', 'y': 'Number of Studies'}))

    # Inclusion and Exclusion Criteria
    st.markdown("#### Studies with defined inclusion and exclusion criteria")
    df_eu['has_criteria'] = df_eu[['inclusion_crt', 'exclusion_crt']].notna().any(axis=1)
    crit_count = df_eu['has_criteria'].value_counts().rename({True: 'Has criteria', False: 'No criteria'})
    st.plotly_chart(px.pie(names=crit_count.index, values=crit_count.values, hole=0.4))


with infograph_tabs[2]:
    st.header("Clinical Trials - GOV")
    df_gov = df[df['source_dataset'] == 'clinicaltrials.gov'].copy()

    st.markdown(f"**Total of GOV clinical trials:** {len(df_gov):,}")

    # Género e idade
    st.markdown("#### Participation by Gender and Age")
    col1, col2, col3 = st.columns(3)
    col1.metric("Female", int(df_gov['Gender_F'].sum()))
    col2.metric("Male", int(df_gov['Gender_M'].sum()))
    col3.metric("Children (0-17)", int(df_gov['Age_0_17_years'].sum()))

    # Type of Study
    st.markdown("#### Study Type")
    st.plotly_chart(px.pie(df_gov, names='study_type', title='Distribution by Type'))

    # Intervention model
    st.markdown("#### Intervention model")
    if 'intervention_model' in df_gov.columns:
        models = df_gov['intervention_model'].value_counts().reset_index()
        models.columns = ['Model', 'Total']
        st.plotly_chart(px.bar(models, x='Model', y='Total', text='Total'))

    col3, col4 = st.columns(2)
    # Masking
    col3.markdown("#### Blinding Type")
    mask_data = {
        "Open": df_gov['masking_OPEN'].sum(skipna=True),
        "Single-blind": df_gov['masking_SINGLE'].sum(skipna=True),
        "Double-blind": df_gov['masking_DOUBLE'].sum(skipna=True),
    }
    col3.plotly_chart(px.pie(names=mask_data.keys(), values=mask_data.values(), hole=0.4, title="Masking"))

    # Expanded access
    col4.markdown("#### Studies with expanded access")
    if 'has_expanded_access' in df_gov.columns:
        access = df_gov['has_expanded_access'].value_counts().rename({True: 'Yes', False: 'No'})
        col4.plotly_chart(px.pie(names=access.index, values=access.values, title="Expanded access"))

    # Keywords
    st.markdown("#### Top Keywords")
    keywords = df_gov['keywords'].dropna().apply(parse_list_str).explode()
    top_kw = keywords.value_counts().head(10).reset_index()
    top_kw.columns = ['Keyword', 'Total']
    st.plotly_chart(px.bar(top_kw, x='Keyword', y='Total', text='Total'))


with infograph_tabs[3]:
    st.header("Infarmed/PAP")
    st.write("General information about the presented DCI.")
    st.write("**Temporal Trend:** Chart of evolution of studies/decisions (Placeholder)")
    st.write("**List of DCI:** List with links to the studies/decisions (Placeholder)")
    st.write("**Filters:** Accepted/Rejected, Decision status, Year (Placeholder)")