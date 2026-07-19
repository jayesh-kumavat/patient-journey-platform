"""
Streamlit dashboard.
Run with: streamlit run src/dashboard/app.py
"""

import pandas as pd
import plotly.express as px
import sys
import streamlit as st
from config.settings import get_db_url
from sqlalchemy import create_engine, text
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

@st.cache_resource
def get_engine():
    return create_engine(get_db_url())


def main():
    st.set_page_config(page_title="Patient Journey Analytics", layout="wide")
    st.title("Patient Journey Analytics")

    engine = get_engine()

    # sidebar filters
    st.sidebar.header("Filters")
    try:
        regions = ["All"] + sorted(pd.read_sql("SELECT DISTINCT region FROM dim_patient", engine)["region"].tolist())
        therapies = ["All"] + sorted(pd.read_sql("SELECT DISTINCT therapy_area FROM fact_prescription", engine)["therapy_area"].tolist())
    except Exception:
        regions = ["All"]
        therapies = ["All"]

    selected_region = st.sidebar.selectbox("Region", regions)
    selected_therapy = st.sidebar.selectbox("Therapy Area", therapies)

    # KPI cards
    col1, col2, col3, col4 = st.columns(4)
    try:
        # patients - filtered by region
        if selected_region != "All":
            patient_count = pd.read_sql(
                text("SELECT COUNT(*) cnt FROM dim_patient WHERE is_current=1 AND region = :r"),
                engine, params={"r": selected_region}
            )["cnt"].iloc[0]
        else:
            patient_count = pd.read_sql(
                "SELECT COUNT(*) cnt FROM dim_patient WHERE is_current=1", engine
            )["cnt"].iloc[0]

        # prescriptions - filtered by therapy area
        if selected_therapy != "All":
            rx_count = pd.read_sql(
                text("SELECT COUNT(*) cnt FROM fact_prescription WHERE therapy_area = :ta"),
                engine, params={"ta": selected_therapy}
            )["cnt"].iloc[0]
        else:
            rx_count = pd.read_sql(
                "SELECT COUNT(*) cnt FROM fact_prescription", engine
            )["cnt"].iloc[0]

        # therapy switches - filtered by therapy area
        if selected_therapy != "All":
            switch_count = pd.read_sql(
                text("SELECT COUNT(*) cnt FROM therapy_switches WHERE therapy_area = :ta"),
                engine, params={"ta": selected_therapy}
            )["cnt"].iloc[0]
        else:
            switch_count = pd.read_sql(
                "SELECT COUNT(*) cnt FROM therapy_switches", engine
            )["cnt"].iloc[0]

        anomaly_count = pd.read_sql(
            "SELECT COUNT(*) cnt FROM anomaly_results", engine
        )["cnt"].iloc[0]

    except Exception:
        patient_count = rx_count = switch_count = anomaly_count = 0

    col1.metric("Patients", f"{patient_count:,}")
    col2.metric("Prescriptions", f"{rx_count:,}")
    col3.metric("Therapy Switches", f"{switch_count:,}")
    col4.metric("Anomalies Flagged", f"{anomaly_count:,}")

    st.divider()

    # prescription volume chart (filtered by therapy)
    st.subheader("Prescription Volume by Therapy Area")
    try:
        if selected_therapy != "All":
            rx_by_therapy = pd.read_sql(
                text("SELECT therapy_area, COUNT(*) as volume FROM fact_prescription WHERE therapy_area = :ta GROUP BY therapy_area"),
                engine, params={"ta": selected_therapy}
            )
        else:
            rx_by_therapy = pd.read_sql(
                "SELECT therapy_area, COUNT(*) as volume FROM fact_prescription GROUP BY therapy_area",
                engine
            )
        fig = px.bar(rx_by_therapy, x="therapy_area", y="volume", color="therapy_area")
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, width='stretch')
    except Exception as e:
        st.error(f"Could not load chart: {e}")

    # two column layout
    left, right = st.columns(2)

    with left:
        st.subheader("Top Therapy Switches")
        try:
            if selected_therapy != "All":
                switches = pd.read_sql(
                    text(
                        "SELECT from_drug, to_drug, COUNT(*) as count FROM therapy_switches "
                        "WHERE therapy_area = :ta GROUP BY from_drug, to_drug ORDER BY count DESC LIMIT 10"
                    ),
                    engine, params={"ta": selected_therapy}
                )
            else:
                switches = pd.read_sql(
                    "SELECT from_drug, to_drug, COUNT(*) as count FROM therapy_switches "
                    "GROUP BY from_drug, to_drug ORDER BY count DESC LIMIT 10", engine
                )

            if switches.empty:
                st.info("No therapy switches for this filter")
            else:
                st.dataframe(switches, width='stretch', hide_index=True)
        except Exception as e:
            st.warning(f"No switch data: {e}")

    with right:
        st.subheader("Top Anomalies")
        try:
            anomalies = pd.read_sql(
                "SELECT physician_id, anomaly_score, confidence, explanation "
                "FROM anomaly_results ORDER BY anomaly_score LIMIT 10", engine
            )
            if anomalies.empty:
                st.info("No anomalies detected")
            else:
                st.dataframe(anomalies, width='stretch', hide_index=True)
        except Exception as e:
            st.warning(f"No anomaly data: {e}")

    # daily trend (filtered by therapy)
    st.subheader("Daily Prescription Volume")
    try:
        if selected_therapy != "All":
            daily = pd.read_sql(
                text(
                    "SELECT prescription_date, COUNT(*) as volume FROM fact_prescription "
                    "WHERE therapy_area = :ta GROUP BY prescription_date ORDER BY prescription_date"
                ),
                engine, params={"ta": selected_therapy}
            )
        else:
            daily = pd.read_sql(
                "SELECT prescription_date, COUNT(*) as volume FROM fact_prescription "
                "GROUP BY prescription_date ORDER BY prescription_date", engine
            )

        if daily.empty:
            st.info("No prescription data for this filter")
        else:
            daily["prescription_date"] = pd.to_datetime(daily["prescription_date"])
            fig = px.line(daily, x="prescription_date", y="volume")
            st.plotly_chart(fig, width='stretch')
    except Exception as e:
        st.error(str(e))

    # patient journey lookup
    st.subheader("Patient Journey Lookup")
    patient_id = st.text_input("Enter Patient ID (e.g. PAT-000001)")
    if patient_id:
        try:
            journey = pd.read_sql(
                text("SELECT * FROM fact_patient_journey WHERE patient_id = :pid ORDER BY sequence_num"),
                engine, params={"pid": patient_id}
            )
            if journey.empty:
                st.warning("No journey data for this patient")
            else:
                st.dataframe(journey, width='stretch', hide_index=True)
        except Exception as e:
            st.error(str(e))


if __name__ == "__main__":
    main()
