"""
Power Project — U.S. Large Load Interconnection Tracker
Main Streamlit application.

Run: streamlit run app.py
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
import yaml

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.storage.database import Database

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="U.S. Large Load Tracker",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Config ───────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_config() -> dict:
    try:
        with open("config.yaml") as f:
            return yaml.safe_load(f)
    except Exception:
        return {"app": {"min_mw": 100, "db_path": "data/power_project.db"}}


CONFIG = load_config()
APP_CFG = CONFIG.get("app", {})
DB_PATH = APP_CFG.get("db_path", "data/power_project.db")
MIN_MW = APP_CFG.get("min_mw", 100)
LOCK_FILE = APP_CFG.get("refresh_lock_file", "data/.refresh.lock")

# ── Database ─────────────────────────────────────────────────────────────────
@st.cache_resource
def get_db() -> Database:
    return Database(DB_PATH)


# ── Cached data loaders ───────────────────────────────────────────────────────
@st.cache_data(ttl=120, show_spinner=False)
def load_projects(
    iso=None, state=None, category=None, status=None,
    min_mw=MIN_MW, max_mw=None, in_service_year=None, search=None
) -> pd.DataFrame:
    db = get_db()
    rows = db.get_projects(
        iso=iso, state=state, category=category, status=status,
        min_mw=min_mw, max_mw=max_mw, in_service_year=in_service_year,
        search=search, limit=5000
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # Parse nested JSON fields
    for col in ["additional_sources", "field_provenance"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.loads(x) if isinstance(x, str) and x else x
            )
    return df


@st.cache_data(ttl=120, show_spinner=False)
def load_summary_stats() -> dict:
    return get_db().get_summary_stats(min_mw=MIN_MW)


@st.cache_data(ttl=120, show_spinner=False)
def load_mw_by_iso() -> pd.DataFrame:
    rows = get_db().get_mw_by_iso(min_mw=MIN_MW)
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["iso", "total_mw", "count"])


@st.cache_data(ttl=120, show_spinner=False)
def load_mw_by_state() -> pd.DataFrame:
    rows = get_db().get_mw_by_state(min_mw=MIN_MW)
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["state", "total_mw", "count"])


@st.cache_data(ttl=120, show_spinner=False)
def load_mw_by_year() -> pd.DataFrame:
    rows = get_db().get_mw_by_year(min_mw=MIN_MW)
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["year", "total_mw", "count"])


@st.cache_data(ttl=60, show_spinner=False)
def load_changelog(limit: int = 50) -> pd.DataFrame:
    rows = get_db().get_changelog(limit=limit)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_scraper_runs() -> pd.DataFrame:
    rows = get_db().get_latest_scraper_run_per_source()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_filing_docs(docket_id=None, keyword=None) -> pd.DataFrame:
    rows = get_db().get_filing_documents(docket_id=docket_id, keyword=keyword, limit=300)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=120, show_spinner=False)
def load_ferc_dockets() -> pd.DataFrame:
    rows = get_db().get_ferc_dockets()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── Refresh pipeline ──────────────────────────────────────────────────────────
def is_refresh_running() -> bool:
    return Path(LOCK_FILE).exists()


def trigger_refresh(force: bool = False):
    """Trigger the refresh pipeline in a background thread."""
    if is_refresh_running():
        st.warning("⏳ Refresh already in progress...")
        return

    def _run():
        # Create lock file
        Path(LOCK_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(LOCK_FILE).touch()
        try:
            from src.pipeline.refresh import run_refresh, setup_logging
            setup_logging("INFO")
            run_refresh(force_refresh=force)
        except Exception as e:
            st.session_state["refresh_error"] = str(e)
        finally:
            try:
                Path(LOCK_FILE).unlink(missing_ok=True)
            except Exception:
                pass
            # Clear caches after refresh
            st.cache_data.clear()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    st.session_state["refresh_started"] = datetime.utcnow().isoformat()


# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar() -> dict:
    with st.sidebar:
        st.title("⚡ Large Load Tracker")
        st.caption("U.S. Electricity Demand Projects ≥100 MW")

        st.divider()

        # Refresh controls
        stats = load_summary_stats()
        last_refresh = stats.get("last_refresh")
        if last_refresh:
            try:
                ts = datetime.fromisoformat(last_refresh)
                st.caption(f"🕐 Last updated: {ts.strftime('%b %d %Y %H:%M')} UTC")
            except Exception:
                st.caption(f"🕐 Last updated: {last_refresh}")
        else:
            st.caption("🕐 Never updated — run refresh first")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Refresh Now", use_container_width=True,
                         disabled=is_refresh_running()):
                trigger_refresh()
                st.rerun()
        with col2:
            if st.button("⚡ Force", use_container_width=True, help="Force re-download all sources",
                         disabled=is_refresh_running()):
                trigger_refresh(force=True)
                st.rerun()

        if is_refresh_running():
            st.info("⏳ Refresh in progress...")

        st.divider()

        # Filters
        st.subheader("Filters")

        all_isos = ["NYISO", "PJM", "MISO", "SPP", "CAISO", "ISO-NE", "ERCOT"]
        iso_filter = st.multiselect("ISO/RTO", all_isos, key="iso_filter")

        all_states = [
            "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
            "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
            "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
            "VA","WA","WV","WI","WY","DC"
        ]
        state_filter = st.multiselect("State", all_states, key="state_filter")

        categories = [
            ("All", None), ("Data Center", "data_center"), ("Industrial", "industrial"),
            ("Crypto Mining", "crypto_mining"), ("EV Charging", "ev_charging"),
            ("Hydrogen", "hydrogen"), ("Unknown", "unknown"),
        ]
        category_filter = st.selectbox(
            "Category",
            [c[0] for c in categories],
            key="category_filter"
        )
        category_val = dict(categories)[category_filter]

        statuses = [("All", None), ("Active", "active"), ("Withdrawn", "withdrawn"),
                    ("Completed", "completed"), ("Suspended", "suspended")]
        status_filter = st.selectbox("Status", [s[0] for s in statuses], key="status_filter")
        status_val = dict(statuses)[status_filter]

        min_mw_filter = st.number_input(
            "Min MW", value=MIN_MW, min_value=0, step=50, key="min_mw_filter"
        )
        max_mw_filter = st.number_input(
            "Max MW", value=0, min_value=0, step=100, key="max_mw_filter",
            help="0 = no upper limit"
        )

        year_filter = st.number_input(
            "In-Service Year", value=0, min_value=0, max_value=2050, step=1,
            help="0 = all years", key="year_filter"
        )

        return {
            "iso": iso_filter[0] if len(iso_filter) == 1 else None,
            "isos": iso_filter,
            "state": state_filter[0] if len(state_filter) == 1 else None,
            "states": state_filter,
            "category": category_val,
            "status": status_val,
            "min_mw": min_mw_filter if min_mw_filter > 0 else MIN_MW,
            "max_mw": max_mw_filter if max_mw_filter > 0 else None,
            "in_service_year": year_filter if year_filter > 0 else None,
        }


# ── Page: Dashboard ───────────────────────────────────────────────────────────
def page_dashboard(filters: dict):
    st.header("📊 Dashboard")

    stats = load_summary_stats()
    mw_by_year = load_mw_by_year()
    mw_by_iso = load_mw_by_iso()
    mw_by_state = load_mw_by_state()

    # KPI cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Projects ≥100 MW", f"{stats.get('total_projects', 0):,}")
    with col2:
        total_mw = stats.get("total_mw", 0)
        st.metric("Total MW in Queue", f"{total_mw:,.0f} MW" if total_mw else "—")
    with col3:
        st.metric("Changes (last 24h)", stats.get("changes_last_24h", 0))
    with col4:
        last = stats.get("last_refresh")
        st.metric("Last Refresh", last[:10] if last else "Never")

    st.divider()

    # MW by in-service year
    if not mw_by_year.empty:
        st.subheader("📅 MW by Forecasted In-Service Year")
        now_year = datetime.utcnow().year
        df_future = mw_by_year[
            mw_by_year["year"].notna() &
            (mw_by_year["year"].astype(str) >= str(now_year))
        ].copy()
        if not df_future.empty:
            df_future["year"] = df_future["year"].astype(str)
            df_future["total_gw"] = df_future["total_mw"] / 1000
            col1, col2 = st.columns([2, 1])
            with col1:
                st.bar_chart(df_future.set_index("year")["total_mw"], height=300)
            with col2:
                # KPI cards for time windows
                for years, label in [(1, "Next 12 mo"), (3, "Next 3 yr"), (5, "Next 5 yr"), (10, "Next 10 yr")]:
                    target_yr = str(now_year + years)
                    window_df = df_future[df_future["year"] <= target_yr]
                    window_mw = window_df["total_mw"].sum()
                    window_count = window_df["count"].sum()
                    st.metric(label, f"{window_mw:,.0f} MW", f"{int(window_count)} projects")
        else:
            st.info("No in-service date data available yet. Run a refresh to populate.")
    else:
        st.info("No timeline data available yet.")

    st.divider()

    col1, col2 = st.columns(2)

    # MW by ISO
    with col1:
        st.subheader("🔌 MW by ISO/RTO")
        if not mw_by_iso.empty:
            st.bar_chart(mw_by_iso.set_index("iso")["total_mw"], height=300)
        else:
            st.info("Run a refresh to see data.")

    # MW by State
    with col2:
        st.subheader("🗺️ MW by State (Top 15)")
        if not mw_by_state.empty:
            top_states = mw_by_state.head(15)
            st.bar_chart(top_states.set_index("state")["total_mw"], height=300)
        else:
            st.info("Run a refresh to see data.")

    # Recent changelog
    st.divider()
    st.subheader("📝 Recent Changes")
    changelog = load_changelog(limit=20)
    if not changelog.empty:
        display_cols = ["recorded_at", "change_type", "project_name", "iso", "summary"]
        display_cols = [c for c in display_cols if c in changelog.columns]
        st.dataframe(
            changelog[display_cols].rename(columns={
                "recorded_at": "When",
                "change_type": "Type",
                "project_name": "Project",
                "iso": "ISO",
                "summary": "Summary",
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No changelog entries yet.")


# ── Page: Projects Table ──────────────────────────────────────────────────────
def page_projects_table(filters: dict):
    st.header("📋 Projects Table")

    # Search bar
    search = st.text_input(
        "🔍 Search (name, county, substation, POI...)",
        key="search_bar",
        placeholder="e.g. data center, 500kV, Virginia..."
    )

    # Load data
    df = load_projects(
        iso=filters.get("iso"),
        state=filters.get("state"),
        category=filters.get("category"),
        status=filters.get("status"),
        min_mw=filters.get("min_mw", MIN_MW),
        max_mw=filters.get("max_mw"),
        in_service_year=filters.get("in_service_year"),
        search=search or None,
    )

    # Multi-ISO/state filter
    if filters.get("isos") and len(filters["isos"]) > 1 and not df.empty:
        df = df[df["iso"].isin(filters["isos"])]
    if filters.get("states") and len(filters["states"]) > 1 and not df.empty:
        df = df[df["state"].isin(filters["states"])]

    # Summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Projects shown", len(df))
    with col2:
        if not df.empty and "mw_requested" in df.columns:
            mw_col = df[["mw_requested", "mw_adjusted", "mw_in_service"]].bfill(axis=1).iloc[:, 0]
            total_mw = mw_col.sum()
            st.metric("Total MW shown", f"{total_mw:,.0f}")
    with col3:
        if not df.empty:
            st.download_button(
                "⬇️ Download CSV",
                data=df.to_csv(index=False),
                file_name=f"large_loads_{datetime.utcnow().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

    if df.empty:
        st.info("No projects match the current filters. Try adjusting filters or running a refresh.")
        return

    # Display columns
    display_cols = [
        "project_name", "iso", "category", "mw_requested", "in_service_date",
        "state", "county", "substation", "utility", "voltage_kv",
        "confidence", "status", "source_name",
    ]
    display_cols = [c for c in display_cols if c in df.columns]

    col_rename = {
        "project_name": "Project Name",
        "iso": "ISO",
        "category": "Category",
        "mw_requested": "MW (Requested)",
        "in_service_date": "In-Service Date",
        "state": "State",
        "county": "County",
        "substation": "Substation/POI",
        "utility": "Utility/TSP",
        "voltage_kv": "kV",
        "confidence": "Confidence",
        "status": "Status",
        "source_name": "Source",
    }

    display_df = df[display_cols].rename(columns=col_rename)

    # Color coding
    def style_confidence(val):
        colors = {"high": "#d4edda", "medium": "#fff3cd", "low": "#f8d7da"}
        return f"background-color: {colors.get(str(val).lower(), 'transparent')}"

    def style_category(val):
        colors = {
            "data_center": "#cce5ff", "industrial": "#d1ecf1",
            "crypto_mining": "#e2d9f3", "unknown": "#f8f9fa",
        }
        return f"background-color: {colors.get(str(val).lower(), 'transparent')}"

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=600,
        column_config={
            "MW (Requested)": st.column_config.NumberColumn(format="%.0f"),
            "kV": st.column_config.NumberColumn(format="%.0f"),
            "In-Service Date": st.column_config.TextColumn(),
        }
    )

    # Expandable row details
    st.divider()
    st.subheader("🔍 Project Details")
    if not df.empty:
        project_names = df["project_name"].fillna("Unknown").tolist()
        selected_name = st.selectbox(
            "Select project for details:",
            options=project_names,
            key="detail_select"
        )
        if selected_name:
            sel_rows = df[df["project_name"] == selected_name]
            if not sel_rows.empty:
                row = sel_rows.iloc[0]
                _render_project_detail(row)


def _render_project_detail(row: pd.Series):
    """Render detailed view for a single project."""
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Project Information**")
        st.write(f"**Name:** {row.get('project_name', 'Unknown')}")
        st.write(f"**ISO/RTO:** {row.get('iso', '—')}")
        st.write(f"**Category:** {row.get('category', '—')}")
        st.write(f"**Status:** {row.get('status', '—')}")
        st.write(f"**Queue ID:** {row.get('queue_id', '—')}")

        st.markdown("**Capacity**")
        st.write(f"**Requested MW:** {row.get('mw_requested', '—')}")
        st.write(f"**Adjusted MW:** {row.get('mw_adjusted', '—')}")
        st.write(f"**MW Definition:** {row.get('mw_definition', '—')}")

        st.markdown("**Dates**")
        st.write(f"**In-Service Date:** {row.get('in_service_date', '—')}")
        st.write(f"**Date Type:** {row.get('in_service_date_type', '—')}")
        st.write(f"**Queue Date:** {row.get('queue_date', '—')}")

    with col2:
        st.markdown("**Location**")
        st.write(f"**State:** {row.get('state', '—')}")
        st.write(f"**County:** {row.get('county', '—')}")
        st.write(f"**City:** {row.get('city', '—')}")
        lat, lon = row.get("latitude"), row.get("longitude")
        st.write(f"**Coordinates:** {f'{lat:.4f}, {lon:.4f}' if lat and lon else '—'}")

        st.markdown("**Point of Interconnection**")
        st.write(f"**Utility/TSP:** {row.get('utility', '—')}")
        st.write(f"**Substation:** {row.get('substation', '—')}")
        st.write(f"**Voltage:** {row.get('voltage_kv', '—')} kV")
        poi = row.get("poi_text", "")
        st.write(f"**POI Text:** {poi if poi else '—'}")
        st.write(f"**Transmission Owner:** {row.get('transmission_owner', '—')}")

        st.markdown("**Source & Confidence**")
        st.write(f"**Confidence:** {row.get('confidence', '—')}")
        st.write(f"**Source:** {row.get('source_name', '—')}")
        src_url = row.get("source_url", "")
        if src_url:
            st.markdown(f"**Source URL:** [{src_url[:60]}...]({src_url})")
        st.write(f"**Last Checked:** {row.get('last_checked', '—')}")

    if row.get("notes"):
        st.info(f"📝 Notes: {row['notes']}")


# ── Page: Map ─────────────────────────────────────────────────────────────────
def page_map(filters: dict):
    st.header("🗺️ Project Map")

    df = load_projects(
        iso=filters.get("iso"),
        state=filters.get("state"),
        category=filters.get("category"),
        status=filters.get("status"),
        min_mw=filters.get("min_mw", MIN_MW),
        max_mw=filters.get("max_mw"),
        in_service_year=filters.get("in_service_year"),
    )

    if filters.get("isos") and len(filters["isos"]) > 1 and not df.empty:
        df = df[df["iso"].isin(filters["isos"])]

    if df.empty:
        st.info("No projects to display. Run a refresh first.")
        return

    # Filter to rows with coordinates
    map_df = df[
        df["latitude"].notna() & df["longitude"].notna() &
        (df["latitude"] != 0) & (df["longitude"] != 0)
    ].copy()

    if map_df.empty:
        st.warning(
            f"No projects have geocoordinates yet. "
            f"{len(df)} projects are in the table but lack lat/lon. "
            "Coordinates are only populated when source data includes them. "
            "You can enable geocoding in config.yaml."
        )
        # Fall back to state-level aggregation
        _render_state_map(df)
        return

    st.write(f"Showing {len(map_df)} of {len(df)} projects (with coordinates)")
    _render_pydeck_map(map_df)


def _render_pydeck_map(df: pd.DataFrame):
    try:
        import pydeck as pdk

        # Normalize MW for radius
        mw_col = df[["mw_requested", "mw_adjusted", "mw_in_service"]].bfill(axis=1).iloc[:, 0].fillna(100)
        df = df.copy()
        df["_mw"] = mw_col
        df["_radius"] = (df["_mw"] / df["_mw"].max() * 50000).clip(2000, 80000)

        # Color by category
        category_colors = {
            "data_center": [0, 100, 255, 180],
            "industrial": [255, 140, 0, 180],
            "crypto_mining": [150, 50, 200, 180],
            "ev_charging": [0, 200, 100, 180],
            "hydrogen": [0, 200, 200, 180],
            "unknown": [128, 128, 128, 180],
        }
        df["_color"] = df["category"].apply(
            lambda c: category_colors.get(str(c).lower(), [128, 128, 128, 180])
        )
        df["_tooltip"] = df.apply(
            lambda r: f"{r.get('project_name', 'Unknown')} | {r.get('_mw', 0):.0f} MW | {r.get('iso', '?')} | {r.get('state', '?')}",
            axis=1
        )

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df[["latitude", "longitude", "_radius", "_color", "_tooltip", "project_name", "iso", "_mw", "state"]],
            get_position=["longitude", "latitude"],
            get_radius="_radius",
            get_fill_color="_color",
            pickable=True,
            stroked=True,
            get_line_color=[255, 255, 255],
            line_width_min_pixels=1,
        )

        view = pdk.ViewState(
            latitude=38.5,
            longitude=-96,
            zoom=3.5,
            pitch=0,
        )

        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=view,
                tooltip={"text": "{_tooltip}"},
            ),
            height=600,
        )

        # Legend
        st.markdown("""
        **Legend:**
        🔵 Data Center &nbsp; 🟠 Industrial &nbsp; 🟣 Crypto Mining &nbsp; 🟢 EV Charging &nbsp; 🔵 Hydrogen &nbsp; ⚪ Unknown
        """)

    except ImportError:
        st.error("pydeck not installed. Run: pip install pydeck")
        st.map(df.rename(columns={"latitude": "lat", "longitude": "lon"})[["lat", "lon"]])


def _render_state_map(df: pd.DataFrame):
    """Fallback: show project counts by state as a table."""
    st.subheader("Projects by State (no coordinates available)")
    if "state" in df.columns:
        state_counts = df.groupby("state").agg(
            count=("project_name", "count"),
            total_mw=("mw_requested", "sum")
        ).reset_index().sort_values("total_mw", ascending=False)
        st.dataframe(state_counts, use_container_width=True, hide_index=True)


# ── Page: Filings ─────────────────────────────────────────────────────────────
def page_filings():
    st.header("📄 FERC Filings & Docket Tracker")

    # Show dockets
    dockets_df = load_ferc_dockets()

    if not dockets_df.empty:
        st.subheader("Tracked Dockets")
        st.dataframe(
            dockets_df[["docket_id", "name", "url", "total_docs", "last_fetched"]].rename(columns={
                "docket_id": "Docket ID",
                "name": "Name",
                "url": "URL",
                "total_docs": "# Docs",
                "last_fetched": "Last Fetched",
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No dockets tracked yet. Run a refresh to fetch FERC filings.")

    st.divider()

    # Filing documents
    st.subheader("📋 Filing Documents")

    col1, col2 = st.columns([1, 2])
    with col1:
        docket_options = ["All"]
        if not dockets_df.empty:
            docket_options += dockets_df["docket_id"].tolist()
        docket_options += ["PJM-LARGE-LOAD", "CAISO-LARGE-LOADS", "SPP-LARGE-LOAD",
                           "MISO-LARGE-LOADS", "ERCOT-LARGE-LOAD"]
        docket_filter = st.selectbox("Filter by Docket", docket_options, key="docket_filter")
    with col2:
        keyword_filter = st.text_input(
            "🔍 Keyword search",
            placeholder="data center, large load, HILL, provisional...",
            key="filing_keyword"
        )

    docs_df = load_filing_docs(
        docket_id=docket_filter if docket_filter != "All" else None,
        keyword=keyword_filter or None,
    )

    if docs_df.empty:
        st.info("No filing documents yet. Run a refresh to fetch filings.")
        return

    st.write(f"Found {len(docs_df)} documents")

    display_cols = ["filed_date", "docket_id", "title", "filer", "pdf_parsed", "has_project_table", "url"]
    display_cols = [c for c in display_cols if c in docs_df.columns]

    for _, row in docs_df.head(100).iterrows():
        with st.expander(f"📄 {row.get('title', 'Untitled')} | {row.get('docket_id', '?')} | {row.get('filed_date', '?')}"):
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Docket:** {row.get('docket_id', '—')}")
                st.write(f"**Filed:** {row.get('filed_date', '—')}")
                st.write(f"**Filer:** {row.get('filer', '—')}")
                st.write(f"**PDF Parsed:** {'✅' if row.get('pdf_parsed') else '❌'}")
                st.write(f"**Has Project Table:** {'✅' if row.get('has_project_table') else '❌'}")
            with col2:
                url = row.get("url", "")
                if url:
                    st.markdown(f"[🔗 Open Document]({url})")
                kw = row.get("keywords_found")
                if isinstance(kw, list) and kw:
                    st.write(f"**Keywords:** {', '.join(kw)}")
                snippet = row.get("extracted_text_snippet")
                if snippet:
                    st.text_area("Text preview:", snippet[:300], height=100, disabled=True)


# ── Page: Sources & Methodology ───────────────────────────────────────────────
def page_sources():
    st.header("⚙️ Sources & Methodology")

    # Data dictionary
    with st.expander("📖 Data Dictionary", expanded=True):
        st.markdown("""
| Field | Description |
|-------|-------------|
| **id** | Deterministic project ID (hash of ISO + queue ID or key fields) |
| **iso** | ISO/RTO grid region (NYISO, PJM, MISO, SPP, CAISO, ISO-NE, ERCOT) |
| **queue_id** | ISO interconnection queue number, if available |
| **project_name** | Project name as provided by the source |
| **category** | Classified type: data_center / industrial / crypto_mining / ev_charging / hydrogen / unknown |
| **status** | Project status: active / withdrawn / completed / suspended |
| **mw_requested** | MW as originally filed/requested (nameplate or interconnection request) |
| **mw_adjusted** | Adjusted MW after revisions |
| **mw_in_service** | Expected final in-service capacity |
| **mw_definition** | Description of which MW definition was used |
| **in_service_date** | Target commercial operation / energization / in-service date |
| **in_service_date_type** | What the date means (e.g., "NYISO In-Service Date", "COD", "RIS") |
| **queue_date** | Date project entered the interconnection queue |
| **state** | 2-letter US state abbreviation |
| **county** | County or municipality |
| **utility** | Transmission Service Provider (TSP) or host utility |
| **substation** | Specific POI substation |
| **voltage_kv** | Interconnection voltage in kilovolts |
| **poi_text** | Full point-of-interconnection text as in source (not inferred) |
| **source_url** | Canonical URL for the primary source document |
| **confidence** | high = official queue with MW+date+POI; medium = official doc partial; low = inferred/incomplete |
| **last_checked** | UTC timestamp of last source check |
| **field_provenance** | Per-field origin (which source provided which value) |
        """)

    st.divider()

    # Confidence explanation
    with st.expander("🎯 Confidence Levels"):
        st.markdown("""
**High** — Official ISO/RTO queue row with explicit MW + in-service date + POI fields populated.
Typical source: NYISO Interconnection Queue XLSX.

**Medium** — Official regulatory document (ISO, FERC, state PUC) with MW and date but partial
location / POI data. Typical source: PJM Load Forecast Tables, ISO annual reports.

**Low** — Data inferred from unstructured PDF text or aggregate tables; or category is inferred
from name patterns; or date is approximate. **MW is never invented** — if MW cannot be parsed,
the project is omitted.

> ⚠️ News articles are used only as secondary context and are labeled as such.
> POI/transmission line fields are **never hallucinated**; they appear as `null` / `—` when not in source.
        """)

    st.divider()

    # Scraper status
    st.subheader("🔌 Data Source Status")
    runs_df = load_scraper_runs()

    if runs_df.empty:
        st.info("No scraper runs recorded. Click 'Refresh Now' to start.")
    else:
        source_map = {
            "nyiso": {"name": "NYISO Interconnection Queue", "iso": "NYISO",
                      "url": "https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx",
                      "fields": "queue_id, project_name, MW, date, state, county, substation, voltage, POI"},
            "pjm": {"name": "PJM Load Forecast + Adjustments", "iso": "PJM",
                    "url": "https://www.pjm.com/planning/load-forecast",
                    "fields": "project_name, MW, state, date, utility"},
            "caiso": {"name": "CAISO Large Loads Initiative", "iso": "CAISO",
                      "url": "https://www.caiso.com/generation-transmission/load/large-load",
                      "fields": "project_name, MW (partial), state"},
            "spp": {"name": "SPP HILL / Large Load Connection", "iso": "SPP",
                    "url": "https://www.spp.org",
                    "fields": "project_name, MW (partial), state"},
            "miso": {"name": "MISO Large Loads Program", "iso": "MISO",
                     "url": "https://www.misoenergy.org/engage/committees/large-loads/",
                     "fields": "project_name, MW (partial), state"},
            "ercot": {"name": "ERCOT Large Load Integration", "iso": "ERCOT",
                      "url": "https://www.ercot.com/services/rq/large-load-integration",
                      "fields": "project_name, MW (partial), state"},
            "ferc_filings": {"name": "FERC Filings Tracker", "iso": "FERC",
                             "url": "https://www.ferc.gov/rm26-4",
                             "fields": "filing title, date, URL, extracted MW (low confidence)"},
        }

        for _, run_row in runs_df.iterrows():
            key = run_row.get("source_key", "")
            meta = source_map.get(key, {})
            status = run_row.get("status", "unknown")
            status_icon = {"success": "✅", "partial": "⚠️", "failed": "❌", "skipped": "⏭️"}.get(status, "❓")

            with st.expander(
                f"{status_icon} **{meta.get('name', key)}** ({meta.get('iso', '?')}) — "
                f"Last run: {run_row.get('started_at', '?')}"
            ):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write(f"**Status:** {status}")
                    st.write(f"**Projects found:** {run_row.get('projects_found', 0)}")
                    st.write(f"**New:** {run_row.get('projects_new', 0)}")
                    st.write(f"**Updated:** {run_row.get('projects_updated', 0)}")
                with col2:
                    st.write(f"**Bytes downloaded:** {run_row.get('bytes_downloaded', 0):,}")
                    st.write(f"**Filings found:** {run_row.get('filings_found', 0)}")
                    fields = run_row.get("fields_produced", [])
                    if isinstance(fields, str):
                        try:
                            fields = json.loads(fields)
                        except Exception:
                            fields = []
                    st.write(f"**Fields produced:** {', '.join(fields) if fields else '—'}")
                with col3:
                    url = meta.get("url", "")
                    if url:
                        st.markdown(f"[🔗 Source URL]({url})")
                    if run_row.get("error_message"):
                        st.error(f"Error: {run_row['error_message']}")
                    if run_row.get("content_hash"):
                        st.caption(f"Content hash: {run_row['content_hash'][:12]}...")


# ── Main App ──────────────────────────────────────────────────────────────────
def main():
    # Custom CSS
    st.markdown("""
    <style>
    .stMetric { background: #f8f9fa; border-radius: 8px; padding: 8px; }
    .stExpander { border: 1px solid #e0e0e0; }
    </style>
    """, unsafe_allow_html=True)

    # Show refresh notification if one was just started
    if st.session_state.get("refresh_started"):
        st.toast("🔄 Refresh started in background...", icon="⚡")

    # Sidebar with filters
    filters = render_sidebar()

    # Navigation tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Dashboard",
        "📋 Projects Table",
        "🗺️ Map",
        "📄 Filings",
        "⚙️ Sources",
    ])

    with tab1:
        page_dashboard(filters)

    with tab2:
        page_projects_table(filters)

    with tab3:
        page_map(filters)

    with tab4:
        page_filings()

    with tab5:
        page_sources()


if __name__ == "__main__":
    main()
