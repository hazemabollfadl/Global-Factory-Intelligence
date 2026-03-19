"""
Global Factory Intelligence - Dashboard
A Streamlit web application that visualizes structured maintenance logs from Google BigQuery.
"""

import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

# AC1: Load environment variables to ensure GOOGLE_APPLICATION_CREDENTIALS is found locally
load_dotenv(override=True)

st.set_page_config(
    page_title="GFI Dashboard",
    page_icon="🏭",
    layout="wide"
)


@st.cache_data(ttl=600)
def load_data() -> pd.DataFrame:
    """AC2: Connects to BigQuery and loads the structured_logs table."""
    client = bigquery.Client()
    project_id = client.project

    query = f"""
        SELECT *
        FROM `{project_id}.gfi_maintenance.structured_logs`
    """

    df = client.query(query).to_dataframe()
    return df


def main():
    # AC3: Dashboard UI Setup
    st.title("Global Factory Intelligence")
    st.markdown(
        "Real-time AI-translated and categorized maintenance logs from the GFI pipeline.")

    try:
        with st.spinner("Fetching live data from BigQuery..."):
            df = load_data()
    except Exception as e:
        st.error(f"Failed to connect to BigQuery: {e}")
        st.info("Check your .env file and ensure your Service Account key is correct.")
        return

    if df.empty:
        st.warning(
            "No data found in the warehouse. Did the Airflow DAG run successfully?")
        return

    st.metric(label="Total Tickets Processed", value=len(df))
    st.divider()

    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("Urgency Breakdown")
        urgency_counts = df['urgency_level'].value_counts()
        st.bar_chart(urgency_counts)

        st.subheader("Original Languages")
        language_counts = df['original_language'].value_counts()
        st.bar_chart(language_counts)

    with col2:
        st.subheader("Structured Log Details")
        st.dataframe(
            df[['ticket_id', 'urgency_level', 'equipment_issue',
                'translated_log', 'original_language']],
            use_container_width=True,
            hide_index=True
        )


if __name__ == "__main__":
    main()
