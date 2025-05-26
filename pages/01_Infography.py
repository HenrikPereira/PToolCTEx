import os
import pandas as pd
import streamlit as st
import plotly.express as px
from streamlit_extras.metric_cards import style_metric_cards
from utils.auxiliary import parse_list_str, load_extras, normalize_list_column

st.set_page_config(layout="wide")

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

st.title("Infography & Insights")
st.subheader("Data from Clinical Trials and Early Access Programs (Infarmed/PAP)")
st.write("This area presents a visual representation of the data.")

### Main metrics
st.markdown("### 🔢 Key Metrics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Studies", f"{len(df):,}")
col2.metric("Total of Participants", f"{int(df['enrollment'].sum(skipna=True)):,}")
col3.metric("Recruiting", df['status'].str.contains("Recruiting", na=False).sum())
col4.metric("Expanded Access", df['has_expanded_access'].sum(skipna=True))
style_metric_cards()

st.divider()

# Internal tabs for the Infographics section
infograph_tabs = st.tabs([
    "Tab 1.1: Overview",
    "Tab 1.2: Infarmed/PAP"
])

with infograph_tabs[0]:
    st.header("Overview")
    st.write("General information about the presented data from clinical trials sources (EU and GOV).")

    # -------------------------------
    # Multiple selectors for filtering
    # -------------------------------

    # Selector for study types
    with st.sidebar:
        st.subheader("🔎 Filters")

        # Study Type
        if "study_type" in df.columns:
            study_type_options = sorted(df['study_type'].dropna().unique())
            selected_study_types = st.multiselect("Select Study Types", options=study_type_options)
        else:
            st.warning("Column 'study_type' not available.")
            selected_study_types = None

        # Therapeutic Area
        if "therapeutic_area" in df.columns:
            df['therapeutic_area'] = df['therapeutic_area'].apply(parse_list_str)
            therapeutic_area_options = normalize_list_column(df['therapeutic_area'])
            selected_therapeutic_areas = st.multiselect("Select Therapeutic Areas", options=therapeutic_area_options)
        else:
            st.warning("Column 'therapeutic_area' not available.")
            selected_therapeutic_areas = None

        # Interventions
        if "interventions" in df.columns:
            df['interventions'] = df['interventions'].apply(parse_list_str)
            intervention_options = normalize_list_column(df['interventions'])
            selected_interventions = st.multiselect("Select Interventions", options=intervention_options)
        else:
            st.warning("Column 'interventions' not available.")
            selected_interventions = None


    # Apply filters to DataFrame
    filtered_df = df.copy()

    if selected_study_types:
        filtered_df = filtered_df[filtered_df['study_type'].str.lower().isin([s.lower() for s in selected_study_types])]

    if selected_therapeutic_areas:
        filtered_df = filtered_df[filtered_df['therapeutic_area'].apply(
            lambda x: any(term in [str(i).strip().lower() for i in x] for term in selected_therapeutic_areas) if isinstance(x, list) else False
        )]

    if selected_interventions:
        filtered_df = filtered_df[filtered_df['interventions'].apply(
            lambda x: any(term in [str(i).strip().lower() for i in x] for term in selected_interventions) if isinstance(x, list) else False
        )]

    st.markdown("---")

    # ─── Gender and Age ────────────────────────────────────────────────────
    st.markdown("#### Participation by Gender and Age")
    col1, col2, col3 = st.columns(3)
    col1.metric("Female", int(df['Gender_F'].sum()))
    col2.metric("Male", int(df['Gender_M'].sum()))
    col3.metric("Children (0-17)", int(df['Age_0_17_years'].sum()))

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

    st.divider()

    # ─── Therapeutic Areas ─────────────────────────────────────────────────────
    st.markdown("### 🧬 Therapeutic Areas Overview")

    filtered_df['therapeutic_area'] = filtered_df['therapeutic_area'].apply(parse_list_str)
    ther_areas_exploded = filtered_df.explode('therapeutic_area')['therapeutic_area'].dropna()
    top_areas_df = ther_areas_exploded.value_counts().reset_index()
    top_areas_df.columns = ['Área', 'Total']

    fig = px.treemap(top_areas_df.head(30), path=["Área"], values="Total", color="Total",
                     color_continuous_scale="Blues")
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ─── Study Phase ───────────────────────────────────────────────────────
    st.subheader("Distribution by Study Phase")
    phase_counts = pd.DataFrame({
        "Phase": ["Early I", "I", "II", "III", "IV"],
        "Total": [
            filtered_df['trial_Early_Phase_I'].sum(skipna=True),
            filtered_df['trial_Phase_I'].sum(skipna=True),
            filtered_df['trial_Phase_II'].sum(skipna=True),
            filtered_df['trial_Phase_III'].sum(skipna=True),
            filtered_df['trial_Phase_IV'].sum(skipna=True),
        ]
    })
    fig = px.bar(phase_counts, x="Phase", y="Total", text="Total")
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    # ─── Sponsors ───────────────────────────────────────────────────────
    st.markdown("#### Distribution by Sponsor")
    sponsor_counts = filtered_df['Sponsor_type'].value_counts().head(10).reset_index()
    sponsor_counts.columns = ['Sponsor type', 'Total']
    st.plotly_chart(px.bar(sponsor_counts, x='Sponsor type', y='Total', text='Total'), use_container_width=True)

    st.divider()

    st.markdown("### Study and Blinding Types Distribution")
    col3, col4 = st.columns(2)
    # ─── Study Types ──────────────────────────────────────────────────
    col3.write("Study types")
    if "study_type" in filtered_df.columns:
        study_type_counts = filtered_df["study_type"].value_counts().reset_index()
        study_type_counts.columns = ["study_type", "count"]
        fig_study = px.bar(study_type_counts, y="study_type", x="count",
                           labels={"study_type": "Study Type", "count": "Count"},
                           template="simple_white", color="count", color_continuous_scale="Teal")
        col3.plotly_chart(fig_study, use_container_width=True)
    else:
        col3.write("Column 'study_type' not available.")

    # ─── Blinding (MASKING) ──────────────────────────────────────────────────
    col4.write("Blinding type (Masking)")

    masking_data = {
        "Open": filtered_df['masking_OPEN'].sum(skipna=True),
        "Single-blind": filtered_df['masking_SINGLE'].sum(skipna=True),
        "Double-blind": filtered_df['masking_DOUBLE'].sum(skipna=True),
    }
    masking_df = pd.DataFrame({
        "masking_type": masking_data.keys(),
        "count": masking_data.values()
    })

    fig_masking = px.bar(
        masking_df,
        y="masking_type",
        x="count",
        labels={"masking_type": "Blinding Type", "count": "Count"},
        template="simple_white", color="count", color_continuous_scale="Reds"
    )
    col4.plotly_chart(fig_masking, use_container_width=True)

    st.divider()

    # ─── Inclusion and Exclusion Criteria ──────────────────────────────────────
    st.markdown("#### Studies with defined inclusion and exclusion criteria")
    filtered_df['has_criteria'] = filtered_df[['inclusion_crt', 'exclusion_crt']].notna().any(axis=1)
    crit_count = filtered_df['has_criteria'].value_counts().rename({True: 'Has criteria', False: 'No criteria'})
    st.plotly_chart(px.pie(names=crit_count.index, values=crit_count.values, hole=0.4))

    st.divider()

    # ─── Interventional Model ───────────────────────────────────────────
    st.markdown("#### Intervention model")
    if 'intervention_model' in filtered_df.columns:
        models = filtered_df['intervention_model'].value_counts().reset_index()
        models.columns = ['Model', 'Total']
        st.plotly_chart(px.bar(models, x='Model', y='Total', text='Total'))

    col3, col4 = st.columns(2)

    st.divider()

    # ─── Enrollment Distribution ─────────────────────────────────────────────────
    st.markdown("### Enrollment Trends Over Time")

    filtered_df['enrollment'] = pd.to_numeric(filtered_df['enrollment'], errors='coerce')
    filtered_df['start_year'] = pd.to_datetime(filtered_df['start_date'], errors='coerce').dt.year
    scatter_df = filtered_df[['start_year', 'enrollment']].dropna()
    fig_enroll = px.scatter(scatter_df, x="start_year", y="enrollment",
                            size="enrollment", color="start_year",
                            labels={"start_year": "Start Year", "enrollment": "Enrollment"},
                            template="plotly_white")
    st.plotly_chart(fig_enroll, use_container_width=True)

    st.divider()

    # ─── Keywords ──────────────────────────────────────────────────────
    st.markdown("#### Top Keywords")
    keywords = filtered_df['keywords'].dropna().apply(parse_list_str).explode()
    top_kw = keywords.value_counts().head(10).reset_index()
    top_kw.columns = ['Keyword', 'Total']
    st.plotly_chart(px.bar(top_kw, x='Keyword', y='Total', text='Total'))

    st.divider()

    # ─── Download ──────────────────────────────────────────────────────
    st.subheader("📥 Export table with all the studies")

    cols_to_show = ['title', 'start_date', 'source_dataset', 'therapeutic_area', 'enrollment']
    available_cols = [col for col in cols_to_show if col in filtered_df.columns]

    # Copy and format 'therapeutic_area' column as string
    df_export = filtered_df[available_cols].copy()

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
    st.header("Early Access Programs (Infarmed)")

    # Load PAP dataset
    pap_path = os.path.join("sources", "pap_clean.parquet")
    try:
        pap_df = pd.read_parquet(pap_path)
    except Exception as e:
        st.error(f"Error loading PAP data: {e}")
        st.stop()

    # Filters
    st.subheader("🔎 Filters")
    colf1, colf2, colf3, colf4 = st.columns(4)

    anos = pap_df['ano_decisao'].dropna().sort_values().unique()
    ano_sel = colf1.multiselect("Year of Decision", options=anos, default=anos)

    decisao_sel = colf2.multiselect("Decision", options=pap_df['decisao'].dropna().unique(), default=pap_df['decisao'].dropna().unique())

    pap_act_sel = colf3.selectbox("PAP Active?", options=["Todos", "Ativo", "Inativo"])

    custos_sel = colf4.selectbox("With costs?", options=["Todos", "Com custos", "Sem custos"])

    # Applying Filters
    df_filtered = pap_df[
        (pap_df['ano_decisao'].isin(ano_sel)) &
        (pap_df['decisao'].isin(decisao_sel))
        ]
    if pap_act_sel != "Todos":
        df_filtered = df_filtered[df_filtered['PAP_act'] == (pap_act_sel == "Ativo")]
    if custos_sel != "Todos":
        df_filtered = df_filtered[df_filtered['c_custos'] == (custos_sel == "Com custos")]

    # General PAP KPIs
    st.subheader("Overview of the PAP Programs")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Programs", f"{len(df_filtered):,}")
    col2.metric("% Approved", f"{(df_filtered['decisao'].value_counts(normalize=True).get('Deferido', 0)*100):.1f}%")
    col3.metric("Active Programs", int(df_filtered['PAP_act'].sum()))
    col4.metric("With Costs", int(df_filtered['c_custos'].sum()))

    # Temporal trend
    st.subheader("Temporal Trend of PAP Decisions")
    yearly_counts = df_filtered['ano_decisao'].value_counts().sort_index()
    fig = px.line(x=yearly_counts.index, y=yearly_counts.values, markers=True, labels={'x': 'Year', 'y': 'No. of decisions'})
    st.plotly_chart(fig, use_container_width=True)

    # Distribution of decision types
    st.subheader("Distribution of decision types")
    decision_counts = df_filtered['decisao'].value_counts()
    fig = px.pie(values=decision_counts.values, names=decision_counts.index, hole=0.4)
    st.plotly_chart(fig, use_container_width=True)

    # TOP DCIs
    st.subheader("Programs - Top INNs (International Non-proprietary Names)")
    top_dcis = df_filtered['DCI'].value_counts().head(10).reset_index()
    top_dcis.columns = ['INN', 'Total']
    fig = px.bar(top_dcis, x="INN", y="Total", text="Total")
    st.plotly_chart(fig, use_container_width=True)

    # Table & Download
    with st.expander("Table with PAP data", expanded=True):
        st.dataframe(df_filtered[['Nome', 'DCI', 'decisao', 'data_decisao', 'n_doentes', 'PAP_act', 'c_custos']].sort_values(by='data_decisao', ascending=False))

        csv = df_filtered.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download CSV", csv, "early_access_programs.csv", "text/csv")
