import sys
import streamlit as st
from pathlib import Path
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
from config.settings import get_db_url

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))


@st.cache_resource
def get_engine():
    return create_engine(get_db_url())


@st.cache_data(ttl=300)
def load_filter_options(_engine):
    try:
        regions = ["All"] + sorted(pd.read_sql("SELECT DISTINCT region FROM dim_patient", _engine)["region"].tolist())
        therapies = ["All"] + sorted(pd.read_sql("SELECT DISTINCT therapy_area FROM fact_prescription", _engine)["therapy_area"].tolist())
    except Exception:
        regions, therapies = ["All"], ["All"]
    return regions, therapies


def build_filters(selected_region, selected_therapy):
    conditions = []
    params = {}

    if selected_region != "All":
        conditions.append("p.region = :region")
        params["region"] = selected_region

    if selected_therapy != "All":
        conditions.append("fp.therapy_area = :therapy")
        params["therapy"] = selected_therapy

    return {
        "where": ("WHERE " + " AND ".join(conditions)) if conditions else "",
        "params": params,
        "therapy_filter": "AND fp.therapy_area = :therapy" if selected_therapy != "All" else "",
        "ts_therapy_filter": "AND ts.therapy_area = :therapy" if selected_therapy != "All" else "",
    }


def main():
    st.set_page_config(page_title="Patient Journey Analytics", layout="wide")
    st.title("Patient Journey Analytics")

    engine = get_engine()

    # sidebar filters
    st.sidebar.header("Filters")
    regions, therapies = load_filter_options(engine)
    selected_region = st.sidebar.selectbox("Region", regions)
    selected_therapy = st.sidebar.selectbox("Therapy Area", therapies)

    filters = build_filters(selected_region, selected_therapy)
    where = filters["where"]
    params = filters["params"]
    therapy_filter = filters["therapy_filter"]
    ts_therapy_filter = filters["ts_therapy_filter"]

    patient_where = ("WHERE p.is_current = 1" if not where else where + " AND p.is_current = 1")

    QUERY_PATIENT_COUNT = text(
        "SELECT COUNT(DISTINCT p.patient_id) cnt FROM dim_patient p "
        "LEFT JOIN fact_prescription fp ON p.patient_id = fp.patient_id " + patient_where
    )
    QUERY_RX_COUNT = text(
        "SELECT COUNT(*) cnt FROM fact_prescription fp "
        "JOIN dim_patient p ON fp.patient_id = p.patient_id " + where
    )
    QUERY_SWITCH_COUNT = text(
        "SELECT COUNT(*) cnt FROM therapy_switches ts "
        "WHERE ts.patient_id IN ("
        "SELECT DISTINCT fp.patient_id FROM fact_prescription fp "
        "JOIN dim_patient p ON fp.patient_id = p.patient_id " + where + ") "
        + ts_therapy_filter
    )
    QUERY_ANOMALY_COUNT = text(
        "SELECT COUNT(DISTINCT ar.physician_id) cnt FROM anomaly_results ar "
        "WHERE ar.physician_id IN ("
        "SELECT DISTINCT fp.physician_id FROM fact_prescription fp "
        "JOIN dim_patient p ON fp.patient_id = p.patient_id " + where + ")"
    )

    # KPI cards
    col1, col2, col3, col4 = st.columns(4)
    try:
        patient_count = pd.read_sql(QUERY_PATIENT_COUNT, engine, params=params)["cnt"].iloc[0]
        rx_count = pd.read_sql(QUERY_RX_COUNT, engine, params=params)["cnt"].iloc[0]
        switch_count = pd.read_sql(QUERY_SWITCH_COUNT, engine, params=params)["cnt"].iloc[0]
        anomaly_count = pd.read_sql(QUERY_ANOMALY_COUNT, engine, params=params)["cnt"].iloc[0]
    except Exception:
        patient_count = rx_count = switch_count = anomaly_count = 0

    col1.metric("Patients", f"{patient_count:,}")
    col2.metric("Prescriptions", f"{rx_count:,}")
    col3.metric("Therapy Switches", f"{switch_count:,}")
    col4.metric("Anomalies Flagged", f"{anomaly_count:,}")

    st.divider()

    # prescription volume chart
    st.subheader("Prescription Volume by Therapy Area")
    try:
        rx_by_therapy = pd.read_sql(text(
            "SELECT fp.therapy_area, COUNT(*) as volume "
            "FROM fact_prescription fp "
            "JOIN dim_patient p ON fp.patient_id = p.patient_id "
            + where +
            " GROUP BY fp.therapy_area"
        ), engine, params=params)
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
            switches = pd.read_sql(text(
                "SELECT ts.from_drug, ts.to_drug, COUNT(*) as count "
                "FROM therapy_switches ts "
                "WHERE ts.patient_id IN ("
                "SELECT DISTINCT fp.patient_id FROM fact_prescription fp "
                "JOIN dim_patient p ON fp.patient_id = p.patient_id " + where + ") "
                + ts_therapy_filter +
                " GROUP BY ts.from_drug, ts.to_drug ORDER BY count DESC LIMIT 10"
            ), engine, params=params)
            if switches.empty:
                st.info("No therapy switches for this filter")
            else:
                st.dataframe(switches, width='stretch', hide_index=True)
        except Exception as e:
            st.warning(f"No switch data: {e}")

    with right:
        st.subheader("Top Anomalies")
        try:
            anomalies = pd.read_sql(text(
                "SELECT DISTINCT ar.physician_id, ar.anomaly_score, ar.confidence, ar.explanation "
                "FROM anomaly_results ar "
                "WHERE ar.physician_id IN ("
                "SELECT DISTINCT fp.physician_id FROM fact_prescription fp "
                "JOIN dim_patient p ON fp.patient_id = p.patient_id " + where + ") "
                "ORDER BY ar.anomaly_score LIMIT 10"
            ), engine, params=params)
            if anomalies.empty:
                st.info("No anomalies detected")
            else:
                st.dataframe(anomalies, width='stretch', hide_index=True)
        except Exception as e:
            st.warning(f"No anomaly data: {e}")


    # daily trend
    st.subheader("Daily Prescription Volume")
    try:
        daily = pd.read_sql(text(
            "SELECT fp.prescription_date, COUNT(*) as volume "
            "FROM fact_prescription fp "
            "JOIN dim_patient p ON fp.patient_id = p.patient_id "
            + where +
            " GROUP BY fp.prescription_date ORDER BY fp.prescription_date"
        ), engine, params=params)
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

