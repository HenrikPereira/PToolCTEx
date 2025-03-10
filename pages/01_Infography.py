import os
import pandas as pd
import streamlit as st
import plotly.express as px
from streamlit_extras.metric_cards import style_metric_cards

from ptoolctext import load_extras

# Define the data file path (assuming the notebook is running from the "etl" folder)
data_path = os.path.join("sources", "full_df.parquet")

# Load the parquet file into a DataFrame
try:
    df = pd.read_parquet(data_path)
except Exception as e:
    st.error(f"Error loading data from {data_path}: {e}")
    st.stop()

st.set_page_config(
    page_title="Infography",
    page_icon="📊",  # Ícone para a página de Infográficos
    layout="wide"
)

header = st.container()
header.image('assets/Banner.png', width=400)
header.write("""<div class='fixed-header'/>""", unsafe_allow_html=True)

### Custom CSS for the sticky header
with open('assets/css/overides.css') as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

config, options = load_extras()

st.title("Infography")
st.write("This area presents a visual representation of the data.")

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

    # -------------------------------
    # Chart 1 and Chart 2: Metrics
    # -------------------------------
    col1, col2 = st.columns(2)

    # Chart 1: Total Studies
    total_studies = filtered_df.shape[0]
    col1.metric("Total Studies", total_studies)
    col1.write("**Chart 1:** Total studies (Placeholder)")

    # Chart 2: Total Completed Studies
    if "overall_status" in filtered_df.columns:
        total_completed = filtered_df[filtered_df["overall_status"].str.lower() == "completed"].shape[0]
    else:
        total_completed = "N/A"
    col2.metric("Total Completed Studies", total_completed)
    col2.write("**Chart 2:** Total completed studies (Placeholder)")

    style_metric_cards(
        background_color=options["color_options"]["silver"],
        border_size_px=2,
        border_color=options["color_options"]["indianred"],
        border_left_color=options["color_options"]["indianred"],
    )

    st.markdown("---")

    # -------------------------------
    # Chart 3 and Chart 4: Bar Charts
    # -------------------------------
    col3, col4 = st.columns(2)

    # Chart 3: Study Types
    col3.write("**Chart 3:** Study types (Placeholder)")
    if "study_type" in filtered_df.columns:
        study_type_counts = filtered_df["study_type"].value_counts().reset_index()
        study_type_counts.columns = ["study_type", "count"]
        fig_study = px.bar(study_type_counts, y="study_type", x="count",
                           labels={"study_type": "Study Type", "count": "Count"},
                           title="Study Types", template="simple_white",)
        col3.plotly_chart(fig_study, use_container_width=True)
    else:
        col3.write("Column 'study_type' not available.")

    # Chart 4: Main Represented Areas (e.g., prevention, treatment)
    col4.write("**Chart 4:** Main represented areas (e.g., prevention, treatment) (Placeholder)")
    if "therapeutic_area" in filtered_df.columns:
        area_counts = filtered_df["therapeutic_area"].value_counts().reset_index().head(15)
        area_counts.columns = ["therapeutic_area", "count"]
        fig_area = px.bar(area_counts, y="therapeutic_area", x="count",
                          labels={"therapeutic_area": "Area", "count": "Count"},
                          title="Represented Areas")
        col4.plotly_chart(fig_area, use_container_width=True)
    else:
        col4.write("Column 'represented_area' not available.")

    st.markdown("---")

    # -------------------------------
    # Chart 5: Main Condition Groups
    # -------------------------------
    st.write("**Chart 5:** Main condition groups (e.g., respiratory diseases, gastrointestinal) (Placeholder)")
    if "condition" in filtered_df.columns:
        condition_counts = filtered_df["condition"].value_counts().reset_index().head(50)
        condition_counts.columns = ["condition", "count"]
        fig_condition = px.bar(condition_counts, x="condition", y="count",
                               labels={"condition": "Condition", "count": "Count"},
                               title="Conditions")
        st.plotly_chart(fig_condition, use_container_width=True)
    else:
        st.write("Column 'condition_group' not available.")

    st.markdown("---")

    # -------------------------------
    # Chart 6: Temporal Trend of Studies
    # -------------------------------
    st.write("**Chart 6:** Temporal trend of studies (Placeholder)")
    if "study_first_submitted_date" in filtered_df.columns:
        filtered_df['study_first_submitted_date'] = pd.to_datetime(filtered_df['study_first_submitted_date'], errors='coerce')
        # Group by year and count studies
        trend = filtered_df.groupby(filtered_df['study_first_submitted_date'].dt.year).size().reset_index(name='count')
        trend = trend.dropna()
        fig_trend = px.line(trend, x='study_first_submitted_date', y='count',
                            labels={"study_first_submitted_date": "Year", "count": "Number of Studies"},
                            title="Temporal Trend of Studies", template="simple_white")
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.write("Column 'study_first_submitted_date' not available.")


with infograph_tabs[1]:
    st.header("Clinical Trials - EU")
    st.write("General information about the studies (e.g., context, relevance).")
    st.write("**Temporal Trend:** Chart of study evolution (Placeholder)")
    st.write("**List of Studies:** List with links to the studies (Placeholder)")
    st.write("**Filters:** Study status, Year, Location in Portugal (Placeholder)")

with infograph_tabs[2]:
    st.header("Clinical Trials - GOV")
    st.write("General information about the studies (e.g., context, relevance).")
    st.write("**Temporal Trend:** Chart of study evolution (Placeholder)")
    st.write("**List of Studies:** List with links to the studies (Placeholder)")
    st.write("**Filters:** Study status, Year, Location in Portugal (Placeholder)")

with infograph_tabs[3]:
    st.header("Infarmed/PAP")
    st.write("General information about the presented DCI.")
    st.write("**Temporal Trend:** Chart of evolution of studies/decisions (Placeholder)")
    st.write("**List of DCI:** List with links to the studies/decisions (Placeholder)")
    st.write("**Filters:** Accepted/Rejected, Decision status, Year (Placeholder)")