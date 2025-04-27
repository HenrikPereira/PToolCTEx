import os
import pandas as pd
import streamlit as st
import plotly.express as px
from streamlit_extras.metric_cards import style_metric_cards
from utils.auxiliary import parse_list_str, load_extras

st.set_page_config(layout="wide")

header = st.container()
header.image('assets/Banner.png', width=400)
header.write("""<div class='fixed-header'/>""", unsafe_allow_html=True)

### Custom CSS for the sticky header
with open('assets/css/overides.css') as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

config, options = load_extras()

st.title("Settings")

# Tabs Configuration
tabs = st.tabs([
    "General Settings",
    "CT Data update",
    "CT Data processing",
])

with tabs[0]:
    cols = st.columns(2)
    with cols[0]:
        st.text_input("AACT username", value='teste')
        st.text_input("AACT password", type="password", value='<PASSWORD>')
    with cols[1]:
        st.text_input("GROQ API KEY", type="password", value='<PASSWORD>', help="Obtenha sua API key em [Groq]"
                                                                                "(https://www.groq.com)"
                      )

with tabs[1]:
    st.header("Clinical Trials Data Update")
    st.write('Scraper functions that retrieve public data from European and Portuguese sources')
    st.write('All data is stored locally, for further processing and analysis')
    st.warning("⚠️ This is a lengthy process that may take several minutes to complete")

    # Container for scraper status
    status_container = st.empty()

    # Get list of available scrapers from scrapers directory
    scrapers_path = "scrapers/eu_ctr/eu_ctr/spiders"
    available_scrapers = [f for f in os.listdir(scrapers_path)
                          if f.endswith('_spider.py')]

    # Create multiselect for scrapers
    selected_scrapers = st.multiselect(
        "Select data sources to update:",
        available_scrapers,
        default=available_scrapers,
        format_func=lambda x: x.replace('_spider.py', '').title()
    )

    if st.button("Start Data Update", type="primary"):
        if not selected_scrapers:
            st.error("Please select at least one data source to update")
        else:
            import sys, os

            # Adiciona o diretório "scrapers/eu_ctr" ao sys.path para que o pacote "eu_ctr.spiders" seja encontrado
            base_dir = os.path.abspath(os.path.join("scrapers", "eu_ctr"))
            if base_dir not in sys.path:
                sys.path.insert(0, base_dir)

            progress_bar = st.progress(0)

            for idx, spider in enumerate(selected_scrapers):
                spider_name = spider.replace('_spider.py', '')
                status_container.info(f"Running {spider_name} spider...")

                try:
                    from scrapy.crawler import CrawlerProcess
                    from scrapy.utils.project import get_project_settings

                    settings = get_project_settings()
                    process = CrawlerProcess(settings)

                    # O módulo a ser importado será "eu_ctr.spiders.<spider_name>" (dentro de "scrapers/eu_ctr")
                    module_name = f"eu_ctr.spiders.{spider_name}"
                    spider_module = __import__(module_name, fromlist=[""])

                    # Supondo que a classe do spider use CamelCase com "Spider" ao final
                    spider_class_name = ''.join(word.capitalize() for word in spider_name.split('_')) + "Spider"
                    spider_class = getattr(spider_module, spider_class_name)

                    process.crawl(spider_class)
                    process.start(stop_after_crawl=True)

                    progress = (idx + 1) / len(selected_scrapers)
                    progress_bar.progress(progress)
                except Exception as e:
                    st.error(f"Error running {spider_name} spider: {str(e)}")
                    continue

            status_container.success("Data update completed!")

    # Add information about last update
    st.divider()
    st.subheader("Last Update Information")

    try:
        import os
        import pandas as pd
        from datetime import datetime

        base_path = os.path.join("scrapers", "eu_ctr", "data")
        files = {
            "Old CT European database": "trials.parquet",
            "European CTIS": "ctis.parquet",
            "Infarmed - Early Access Programs": "pap.parquet"
        }

        update_info = []

        for source, filename in files.items():
            file_path = os.path.join(base_path, filename)
            if os.path.exists(file_path):
                # Read the parquet file and count the number of records
                df = pd.read_parquet(file_path)
                records_updated = len(df)

                # Get the last modification time of the file and format it
                last_mod_timestamp = os.path.getmtime(file_path)
                last_update = datetime.fromtimestamp(last_mod_timestamp).strftime("%Y-%m-%d %H:%M:%S")

                update_info.append({
                    "source": source,
                    "last_update": last_update,
                    "total_records": records_updated
                })

        if update_info:
            update_df = pd.DataFrame(update_info)
            st.dataframe(
                update_df,
                column_config={
                    "source": "Data Source",
                    "last_update": "Last Update",
                    "total_records": "Total Records"
                },
                use_container_width=True
            )
        else:
            st.info("No update information available.")
    except Exception as e:
        st.error(f"Error loading update information: {e}")

with tabs[2]:
    import os
    import pandas as pd
    import streamlit as st

    st.header("Reprocess Clinical Trials Data")
    st.write("This process will merge data from different sources into a single DataFrame.")

    if st.button("Process Data"):
        # Define data paths
        base_data_path = os.path.join("scrapers", "eu_ctr", "data")
        ctis_path = os.path.join(base_data_path, "ctis.parquet")
        pap_path = os.path.join(base_data_path, "pap.parquet")
        trials_path = os.path.join(base_data_path, "trials.parquet")

        # Try to read the files
        try:
            ctis_df = pd.read_parquet(ctis_path)
            pap_df = pd.read_parquet(pap_path)
            trials_df = pd.read_parquet(trials_path)
        except Exception as e:
            st.error(f"Error reading data files: {e}")
            st.stop()

        st.info(f"ctis: {ctis_df.shape[0]} records | pap: {pap_df.shape[0]} records | trials: {trials_df.shape[0]} records")

        # Standardize columns so that the concatenation works even if the tables have different structures.
        all_columns = set(ctis_df.columns).union(pap_df.columns).union(trials_df.columns)
        ctis_df = ctis_df.reindex(columns=all_columns)
        pap_df = pap_df.reindex(columns=all_columns)
        trials_df = trials_df.reindex(columns=all_columns)

        # Concatenate the DataFrames vertically (stacking the records)
        combined_df = pd.concat([ctis_df, pap_df, trials_df], ignore_index=True, sort=False)

        # Example cleaning: convert "start_date" column to datetime if it exists.
        if "start_date" in combined_df.columns:
            combined_df["start_date"] = pd.to_datetime(combined_df["start_date"], errors="coerce")

        st.success(f"Processing complete. Total combined records: {combined_df.shape[0]:,}")

        # Display the first 20 rows for preview
        st.dataframe(combined_df.head(20))

        # Allow download of the combined data as CSV
        csv_data = combined_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download Combined Data (CSV)",
            data=csv_data,
            file_name="combined_ct_data.csv",
            mime="text/csv"
        )
    else:
        st.info("Click the 'Process Data' button to combine the clinical trials data.")
