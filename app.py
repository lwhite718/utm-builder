"""
Streamlit UTM Builder (MVP, Streamlit Cloud edition)
Author: ED: Dev Wizard
Version: 2.0 - NO CACHING VERSION

Persistence: Google Sheets via Service Account (Streamlit Cloud friendly)
- Tabs: campaigns, templates, utm_links
- Secrets: gcp_service_account (JSON), gsheets.spreadsheet_url OR gsheets.spreadsheet_name

Run on Streamlit Cloud or locally (if you set secrets).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit, urlencode, parse_qsl, urlparse
import time

import pandas as pd
import streamlit as st
import requests

# --- Google Sheets (gspread) ---
import gspread
from google.oauth2.service_account import Credentials

# =============================================================================
# Google Sheets Persistence Layer
# =============================================================================

REQ_TABS = {
    "campaigns": ["id", "name", "created_at"],
    "templates": ["id", "name", "category", "source", "medium", "content", "term", "created_at"],
    "utm_links": [
        "id",
        "campaign_id",
        "base_url",
        "utm_campaign",
        "utm_source",
        "utm_medium",
        "utm_content",
        "utm_term",
        "final_url",
        "created_at",
    ],
}


@dataclass
class SheetsEnv:
    client: gspread.Client
    spreadsheet: gspread.Spreadsheet


def _get_credentials() -> Credentials:
    # Expect a full service account dict in st.secrets["gcp_service_account"]
    sa_info = dict(st.secrets["gcp_service_account"])  # type: ignore[arg-type]
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(sa_info, scopes=scopes)


def _get_spreadsheet(client: gspread.Client) -> gspread.Spreadsheet:
    gs_cfg = st.secrets.get("gsheets", {})
    url = gs_cfg.get("spreadsheet_url")
    name = gs_cfg.get("spreadsheet_name")
    if url:
        return client.open_by_url(url)
    if name:
        return client.open(name)
    raise RuntimeError(
        "Set either gsheets.spreadsheet_url or gsheets.spreadsheet_name in Streamlit secrets."
    )


def connect_sheets() -> SheetsEnv:
    creds = _get_credentials()
    client = gspread.authorize(creds)
    ss = _get_spreadsheet(client)
    _ensure_tabs(ss)
    return SheetsEnv(client=client, spreadsheet=ss)


def _ensure_tabs(ss: gspread.Spreadsheet) -> None:
    existing = {ws.title for ws in ss.worksheets()}
    for tab, headers in REQ_TABS.items():
        if tab not in existing:
            ws = ss.add_worksheet(title=tab, rows=1000, cols=max(6, len(headers)))
            ws.append_row(headers)
        else:
            ws = ss.worksheet(tab)
            # Ensure headers exist (first row)
            current = ws.row_values(1)
            if current != headers:
                ws.resize(rows=1)  # clear data but preserve sheet
                ws.update([headers])


def _ws(ss: gspread.Spreadsheet, tab: str) -> gspread.Worksheet:
    return ss.worksheet(tab)


def _read_df(ss: gspread.Spreadsheet, tab: str) -> pd.DataFrame:
    ws = _ws(ss, tab)
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=REQ_TABS[tab])
    # Coerce columns to exist
    for col in REQ_TABS[tab]:
        if col not in df.columns:
            df[col] = []
    return df[REQ_TABS[tab]].copy()


def _write_df(ss: gspread.Spreadsheet, tab: str, df: pd.DataFrame) -> None:
    ws = _ws(ss, tab)
    if df.empty:
        ws.resize(rows=1)
        ws.update([REQ_TABS[tab]])
        return
    data = [REQ_TABS[tab]] + df.astype(str).values.tolist()
    ws.resize(rows=len(data), cols=len(REQ_TABS[tab]))
    ws.update(data)


def _append_rows(ss: gspread.Spreadsheet, tab: str, rows: List[List[str]]) -> None:
    ws = _ws(ss, tab)
    ws.append_rows(rows, value_input_option="USER_ENTERED")


def _next_id(df: pd.DataFrame) -> int:
    if df.empty or "id" not in df.columns or df["id"].isna().all():
        return 1
    try:
        return int(pd.to_numeric(df["id"], errors="coerce").max()) + 1
    except Exception:
        return 1


# =============================================================================
# Simple In-Memory Cache (No Streamlit Caching)
# =============================================================================

# Simple in-memory cache with timestamps
_cache = {
    "campaigns": {"data": None, "timestamp": 0, "ttl": 30},
    "templates": {"data": None, "timestamp": 0, "ttl": 30}, 
    "utm_links": {"data": {}, "timestamp": 0, "ttl": 10}  # keyed by campaign_id
}

def _is_cache_valid(cache_key: str) -> bool:
    """Check if cache is still valid"""
    cache_entry = _cache.get(cache_key, {})
    if cache_entry.get("data") is None:
        return False
    return (time.time() - cache_entry.get("timestamp", 0)) < cache_entry.get("ttl", 0)

def _set_cache(cache_key: str, data, campaign_id: int = None):
    """Set cache data"""
    if cache_key == "utm_links" and campaign_id is not None:
        if "data" not in _cache[cache_key]:
            _cache[cache_key]["data"] = {}
        _cache[cache_key]["data"][campaign_id] = data
    else:
        _cache[cache_key]["data"] = data
    _cache[cache_key]["timestamp"] = time.time()

def _get_cache(cache_key: str, campaign_id: int = None):
    """Get cache data"""
    if cache_key == "utm_links" and campaign_id is not None:
        return _cache[cache_key]["data"].get(campaign_id)
    return _cache[cache_key]["data"]

def _clear_cache(cache_key: str, campaign_id: int = None):
    """Clear cache data"""
    if cache_key == "utm_links" and campaign_id is not None:
        if "data" in _cache[cache_key] and campaign_id in _cache[cache_key]["data"]:
            del _cache[cache_key]["data"][campaign_id]
    else:
        _cache[cache_key]["data"] = None
        _cache[cache_key]["timestamp"] = 0


# =============================================================================
# High-level CRUD (campaigns / templates / utm_links)
# =============================================================================

def list_campaigns(ss) -> List[Dict]:
    # Check cache first
    if _is_cache_valid("campaigns"):
        cached_data = _get_cache("campaigns")
        if cached_data is not None:
            return cached_data
    
    # Fetch fresh data
    df = _read_df(ss, "campaigns")
    result = df.sort_values("created_at", ascending=False).to_dict(orient="records")
    
    # Cache the result
    _set_cache("campaigns", result)
    return result


def create_campaign(ss, name: str) -> Optional[int]:
    name = name.strip()
    if not name:
        return None
    cdf = _read_df(ss, "campaigns")
    if not cdf[cdf["name"].str.lower() == name.lower()].empty:
        return None  # unique name constraint
    new_id = _next_id(cdf)
    now = datetime.utcnow().isoformat()
    _append_rows(ss, "campaigns", [[str(new_id), name, now]])
    
    # Clear cache to refresh data
    _clear_cache("campaigns")
    return new_id


def delete_campaign(ss, campaign_id: int) -> None:
    cdf = _read_df(ss, "campaigns")
    cdf = cdf[cdf["id"].astype(str) != str(campaign_id)]
    _write_df(ss, "campaigns", cdf)
    # Cascade delete utm_links
    ldf = _read_df(ss, "utm_links")
    ldf = ldf[ldf["campaign_id"].astype(str) != str(campaign_id)]
    _write_df(ss, "utm_links", ldf)
    
    # Clear caches
    _clear_cache("campaigns")
    _clear_cache("utm_links", campaign_id)


def save_template(ss, name: str, category: str, source: str, medium: str, content: str = "", term: str = "") -> bool:
    name = name.strip()
    category = category.strip()
    if not name or not source.strip() or not medium.strip():
        return False
    tdf = _read_df(ss, "templates")
    if not tdf[tdf["name"].str.lower() == name.lower()].empty:
        return False
    new_id = _next_id(tdf)
    now = datetime.utcnow().isoformat()
    _append_rows(ss, "templates", [[
        str(new_id), name, category, source, medium, content, term, now
    ]])
    
    # Clear cache
    _clear_cache("templates")
    return True


def list_templates(ss) -> pd.DataFrame:
    # Check cache first
    if _is_cache_valid("templates"):
        cached_data = _get_cache("templates")
        if cached_data is not None:
            return cached_data
    
    # Fetch fresh data
    result = _read_df(ss, "templates")
    
    # Cache the result
    _set_cache("templates", result)
    return result


def delete_template(ss, template_id: int):
    tdf = _read_df(ss, "templates")
    tdf = tdf[tdf["id"].astype(str) != str(template_id)]
    _write_df(ss, "templates", tdf)
    
    # Clear cache
    _clear_cache("templates")


def insert_utm_links(ss, campaign_id: int, df: pd.DataFrame):
    ldf = _read_df(ss, "utm_links")
    next_id = _next_id(ldf)
    now = datetime.utcnow().isoformat()

    rows = []
    for _, row in df.iterrows():
        rows.append([
            str(next_id),
            str(campaign_id),
            str(row.get("base_url", "")),
            str(row.get("utm_campaign", "")),
            str(row.get("utm_source", "")),
            str(row.get("utm_medium", "")),
            str(row.get("utm_content", "")),
            str(row.get("utm_term", "")),
            str(row.get("final_url", "")),
            now,
        ])
        next_id += 1
    if rows:
        _append_rows(ss, "utm_links", rows)
        
        # Clear cache for this campaign
        _clear_cache("utm_links", campaign_id)


def load_campaign_links(ss, campaign_id: int) -> pd.DataFrame:
    # Check cache first
    if _is_cache_valid("utm_links"):
        cached_data = _get_cache("utm_links", campaign_id)
        if cached_data is not None:
            return cached_data
    
    # Fetch fresh data
    df = _read_df(ss, "utm_links")
    df = df[df["campaign_id"].astype(str) == str(campaign_id)]
    if df.empty:
        result = df
    else:
        result = df.sort_values("created_at", ascending=False).reset_index(drop=True)
    
    # Cache the result
    _set_cache("utm_links", result, campaign_id)
    return result


def update_utm_link(ss, link_id: int, updated_data: Dict) -> bool:
    """Update an existing UTM link"""
    try:
        ldf = _read_df(ss, "utm_links")
        link_idx = ldf[ldf["id"].astype(str) == str(link_id)].index
        
        if link_idx.empty:
            return False
            
        # Update the row
        for key, value in updated_data.items():
            if key in ldf.columns:
                ldf.loc[link_idx[0], key] = str(value)
        
        _write_df(ss, "utm_links", ldf)
        
        # Clear cache
        campaign_id = int(ldf.loc[link_idx[0], "campaign_id"])
        _clear_cache("utm_links", campaign_id)
        return True
    except Exception:
        return False


def delete_utm_link(ss, link_id: int) -> bool:
    """Delete a UTM link"""
    try:
        ldf = _read_df(ss, "utm_links")
        campaign_id_row = ldf[ldf["id"].astype(str) == str(link_id)]
        
        if campaign_id_row.empty:
            return False
            
        campaign_id = int(campaign_id_row["campaign_id"].iloc[0])
        ldf = ldf[ldf["id"].astype(str) != str(link_id)]
        _write_df(ss, "utm_links", ldf)
        
        # Clear cache
        _clear_cache("utm_links", campaign_id)
        return True
    except Exception:
        return False


def duplicate_campaign(ss, campaign_id: int, new_name: str) -> Optional[int]:
    """Duplicate a campaign with all its links"""
    try:
        # Get original campaign
        cdf = _read_df(ss, "campaigns")
        original_campaign = cdf[cdf["id"].astype(str) == str(campaign_id)]
        
        if original_campaign.empty:
            return None
            
        # Create new campaign
        new_campaign_id = create_campaign(ss, new_name)
        if new_campaign_id is None:
            return None
            
        # Duplicate all links
        links_df = load_campaign_links(ss, campaign_id)
        if not links_df.empty:
            # Prepare links for insertion (remove id and update campaign_id)
            links_to_duplicate = links_df.drop(columns=["id", "created_at"]).copy()
            links_to_duplicate["campaign_id"] = new_campaign_id
            
            # Regenerate final URLs with new campaign name
            new_campaign_name = generate_campaign_utm_name(new_name)
            for idx, row in links_to_duplicate.iterrows():
                if row["utm_campaign"] == generate_campaign_utm_name(original_campaign["name"].iloc[0]):
                    links_to_duplicate.at[idx, "utm_campaign"] = new_campaign_name
                    
                # Rebuild final URL
                params = {
                    "utm_campaign": links_to_duplicate.at[idx, "utm_campaign"],
                    "utm_source": links_to_duplicate.at[idx, "utm_source"],
                    "utm_medium": links_to_duplicate.at[idx, "utm_medium"],
                    "utm_content": links_to_duplicate.at[idx, "utm_content"],
                    "utm_term": links_to_duplicate.at[idx, "utm_term"],
                }
                links_to_duplicate.at[idx, "final_url"] = build_utm_url(row["base_url"], params)
            
            insert_utm_links(ss, new_campaign_id, links_to_duplicate)
            
        return new_campaign_id
    except Exception:
        return None


# =============================================================================
# UTM + Formatting Helpers
# =============================================================================

def apply_formatting(value: str, force_lower: bool, space_style: str) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if force_lower:
        s = s.lower()
    if space_style == "-":
        s = s.replace(" ", "-")
    elif space_style == "_":
        s = s.replace(" ", "_")
    return s


def generate_campaign_utm_name(campaign_name: str) -> str:
    """Generate utm_campaign name from campaign name: lowercase with hyphens"""
    if not campaign_name:
        return ""
    return campaign_name.strip().lower().replace(" ", "-")


def build_utm_url(base_url: str, params: Dict[str, str]) -> str:
    if not base_url:
        return ""
    parts = urlsplit(base_url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    for k, v in params.items():
        if v:
            q[k] = v
    new_query = urlencode(q, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


# =============================================================================
# UI Helpers & Session
# =============================================================================

def snack(msg: str, icon: str = "✅"):
    st.toast(msg, icon=icon)


def test_url_status(url: str) -> tuple[bool, str, int]:
    """
    Test URL format and basic validation (no HTTP requests to avoid deployment issues)
    Returns: (is_success, message, status_code)
    """
    if not url.strip():
        return False, "Empty URL", 0
    
    try:
        # Add protocol if missing
        test_url = url
        if not test_url.startswith(('http://', 'https://')):
            test_url = 'https://' + test_url
        
        # Validate URL format
        parsed = urlparse(test_url)
        if not parsed.netloc:
            return False, "Invalid URL format", 0
        
        # Check for common issues
        if len(parsed.netloc) < 3:
            return False, "Domain too short", 0
        
        if not ('.' in parsed.netloc):
            return False, "Invalid domain format", 0
        
        # Basic validation passed
        return True, "URL format is valid", 200
            
    except Exception as e:
        return False, f"URL validation error: {str(e)[:30]}", 0


def ensure_session_state():
    if "selected_campaign_id" not in st.session_state:
        st.session_state.selected_campaign_id = None
    if "bulk_df" not in st.session_state:
        st.session_state.bulk_df = pd.DataFrame(
            [
                {"base_url": "", "utm_campaign": "", "utm_source": "", "utm_medium": "", "utm_content": "", "utm_term": "", "template": ""}
                for _ in range(3)
            ]
        )


# =============================================================================
# UI: Sidebars
# =============================================================================

def sidebar_campaigns(ss_env: SheetsEnv):
    st.sidebar.header("📁 Campaigns/Projects")
    
    # Initialize clear flag in session state
    if "clear_campaign_input" not in st.session_state:
        st.session_state.clear_campaign_input = False
    
    col1, col2 = st.sidebar.columns([3, 1])
    with col1:
        # Clear the input by using a different key when flag is set
        input_key = "new_campaign_name_cleared" if st.session_state.clear_campaign_input else "new_campaign_name"
        new_name = st.text_input("New campaign name", key=input_key)
    with col2:
        if st.button("Add", type="primary"):
            if not new_name.strip():
                st.warning("Please provide a campaign name.")
            else:
                cid = create_campaign(ss_env.spreadsheet, new_name)
                if cid is None:
                    st.error("A campaign with that name already exists.")
                else:
                    st.session_state.selected_campaign_id = cid
                    # Set flag to clear input on next run
                    st.session_state.clear_campaign_input = not st.session_state.clear_campaign_input
                    snack("Campaign created")
                    st.rerun()

    campaigns = list_campaigns(ss_env.spreadsheet)
    options = {c["name"]: int(c["id"]) for c in campaigns}
    if options:
        # figure out current index
        ids = list(options.values())
        names = list(options.keys())
        idx = 0
        if st.session_state.selected_campaign_id in ids:
            idx = ids.index(st.session_state.selected_campaign_id)
        selected_name = st.sidebar.selectbox("Select campaign", options=names, index=idx)
        st.session_state.selected_campaign_id = options[selected_name]
    else:
        st.sidebar.info("No campaigns yet.")

    if st.session_state.selected_campaign_id:
        with st.sidebar.expander("Export / Delete", expanded=False):
            links_df = load_campaign_links(ss_env.spreadsheet, st.session_state.selected_campaign_id)
            if not links_df.empty:
                csv = links_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="⬇️ Download CSV",
                    data=csv,
                    file_name=f"campaign_{st.session_state.selected_campaign_id}_utm_links.csv",
                    mime="text/csv",
                )
            else:
                st.caption("No links in this campaign yet.")

            st.divider()
            
            # Duplicate campaign
            duplicate_name = st.text_input("Duplicate campaign as:", placeholder="New campaign name")
            if duplicate_name.strip() and st.button("📋 Duplicate Campaign"):
                new_id = duplicate_campaign(ss_env.spreadsheet, st.session_state.selected_campaign_id, duplicate_name.strip())
                if new_id:
                    st.session_state.selected_campaign_id = new_id
                    snack(f"Campaign duplicated as '{duplicate_name}'")
                    st.rerun()
                else:
                    st.error("Failed to duplicate campaign or name already exists.")
            
            st.divider()
            danger = st.checkbox("Enable delete", value=False)
            if danger and st.button("🗑️ Delete campaign", type="secondary"):
                delete_campaign(ss_env.spreadsheet, st.session_state.selected_campaign_id)
                st.session_state.selected_campaign_id = None
                snack("Campaign deleted", icon="⚠️")
                st.rerun()


def presets_tab(ss_env: SheetsEnv):
    """Template management moved to a main tab"""
    st.header("📝 Template Presets")
    st.caption("Create and manage reusable UTM templates organized by category.")
    
    # Template categories
    template_categories = [
        "Social Media", "Email Marketing", "Paid Ads", "Content Marketing", 
        "PR/Outreach", "Partnerships", "Events", "Direct Marketing", "Other"
    ]
    
    # Create new template section
    st.subheader("Create New Template")
    with st.form("template_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            t_name = st.text_input("Template name", placeholder="e.g., LinkedIn CEO Social")
            t_category = st.selectbox("Category", options=template_categories, index=0)
        with col2:
            t_source = st.text_input("utm_source", placeholder="e.g., linkedin")
            t_medium = st.text_input("utm_medium", placeholder="e.g., social")
        
        col3, col4 = st.columns(2)
        with col3:
            t_content = st.text_input("utm_content (optional)", placeholder="e.g., ceo-post")
        with col4:
            t_term = st.text_input("utm_term (optional)", placeholder="e.g., brand-awareness")
        
        submitted = st.form_submit_button("Save Template", type="primary")
        if submitted:
            if not t_name.strip():
                st.error("Template needs a name.")
            elif not t_source.strip() or not t_medium.strip():
                st.error("Source and medium are required for a template.")
            else:
                ok = save_template(ss_env.spreadsheet, t_name, t_category, t_source, t_medium, t_content, t_term)
                if ok:
                    snack("Template saved")
                    st.rerun()
                else:
                    st.error("A template with that name already exists.")

    st.divider()

    # Display existing templates
    df = list_templates(ss_env.spreadsheet)
    if not df.empty:
        st.subheader("Saved Templates")
        
        # Filter controls
        col1, col2 = st.columns([3, 1])
        with col1:
            categories = ["All"] + sorted(df["category"].unique().tolist())
            selected_category = st.selectbox("Filter by category", options=categories, index=0)
        with col2:
            st.write("")  # spacer
            
        if selected_category != "All":
            filtered_df = df[df["category"] == selected_category]
        else:
            filtered_df = df
            
        if not filtered_df.empty:
            display_cols = ["id", "name", "category", "source", "medium", "content", "term"]
            st.dataframe(filtered_df[display_cols], use_container_width=True, hide_index=True)
            
            # Delete template section
            st.subheader("Delete Template")
            col1, col2 = st.columns([3, 1])
            with col1:
                to_delete = st.number_input("Template ID to delete", min_value=0, step=1, value=0)
            with col2:
                st.write("")  # spacer
                if to_delete and st.button("Delete Template", type="secondary"):
                    delete_template(ss_env.spreadsheet, int(to_delete))
                    snack("Template deleted", icon="🧹")
                    st.rerun()
        else:
            st.info(f"No templates in {selected_category} category.")
    else:
        st.info("No templates saved yet. Create your first template above.")


# =============================================================================
# UI: Main builders
# =============================================================================

def formatting_controls():
    st.subheader("⚙️ Formatting Controls")
    c1, c2 = st.columns(2)
    with c1:
        force_lower = st.checkbox("Force lowercase", value=True)
    with c2:
        space_style = st.radio("Replace spaces with", options=["(none)", "-", "_"], index=1, horizontal=True)
        if space_style == "(none)":
            space_style = ""
    st.caption("Formatting applies to UTM fields (campaign, source, medium, content, term) when generating URLs.")
    return force_lower, space_style


def single_builder(ss_env: SheetsEnv, force_lower: bool, space_style: str, templates_df: pd.DataFrame):
    st.subheader("🔗 Single UTM Builder")
    base_url = st.text_input("Base URL", placeholder="https://example.com/page")

    # Get suggested utm_campaign from selected campaign
    suggested_utm_campaign = ""
    if st.session_state.selected_campaign_id:
        campaigns = list_campaigns(ss_env.spreadsheet)
        selected_campaign = next((c for c in campaigns if c["id"] == st.session_state.selected_campaign_id), None)
        if selected_campaign:
            suggested_utm_campaign = generate_campaign_utm_name(selected_campaign["name"])

    template_names = ["(none)"] + templates_df["name"].tolist() if not templates_df.empty else ["(none)"]
    t_choice = st.selectbox("Apply template", options=template_names)
    t_row = templates_df[templates_df["name"] == t_choice].head(1) if t_choice != "(none)" and not templates_df.empty else pd.DataFrame()

    c1, c2 = st.columns(2)
    with c1:
        utm_campaign = st.text_input(
            "utm_campaign", 
            value=suggested_utm_campaign,
            help="Auto-suggested from selected campaign name"
        )
        utm_source = st.text_input("utm_source", value=apply_formatting(t_row["source"].iloc[0] if not t_row.empty else "", force_lower, space_style))
        utm_medium = st.text_input("utm_medium", value=apply_formatting(t_row["medium"].iloc[0] if not t_row.empty else "", force_lower, space_style))
    with c2:
        utm_content = st.text_input("utm_content", value=apply_formatting(t_row["content"].iloc[0] if not t_row.empty else "", force_lower, space_style))
        utm_term = st.text_input("utm_term", value=apply_formatting(t_row["term"].iloc[0] if not t_row.empty else "", force_lower, space_style))

    f_campaign = apply_formatting(utm_campaign, force_lower, space_style)
    f_source = apply_formatting(utm_source, force_lower, space_style)
    f_medium = apply_formatting(utm_medium, force_lower, space_style)
    f_content = apply_formatting(utm_content, force_lower, space_style)
    f_term = apply_formatting(utm_term, force_lower, space_style)

    params = {
        "utm_campaign": f_campaign,
        "utm_source": f_source,
        "utm_medium": f_medium,
        "utm_content": f_content,
        "utm_term": f_term,
    }

    final_url = build_utm_url(base_url, params)
    st.code(final_url or "(URL preview will appear here)")

    # URL Testing Section
    if final_url:
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("Test URL", help="Validate URL format and structure"):
                with st.spinner("Validating URL..."):
                    is_success, message, status_code = test_url_status(final_url)
                    if is_success:
                        st.success(f"✅ {message}")
                    else:
                        st.error(f"❌ {message}")
        with col2:
            st.caption("Validates URL format and basic structure (no live HTTP test to avoid deployment issues).")

    if st.session_state.selected_campaign_id and final_url:
        if st.button("Save to selected campaign"):
            df = pd.DataFrame([
                {
                    "base_url": base_url,
                    "utm_campaign": f_campaign,
                    "utm_source": f_source,
                    "utm_medium": f_medium,
                    "utm_content": f_content,
                    "utm_term": f_term,
                    "final_url": final_url,
                }
            ])
            insert_utm_links(ss_env.spreadsheet, st.session_state.selected_campaign_id, df)
            snack("Link saved to campaign")
    elif not st.session_state.selected_campaign_id:
        st.info("Select or create a campaign in the sidebar to save links.")


def bulk_builder(ss_env: SheetsEnv, force_lower: bool, space_style: str, templates_df: pd.DataFrame):
    st.subheader("📦 Bulk UTM Builder")
    st.caption("Use the table to add multiple rows. You can apply a template to any row.")

    # Get suggested utm_campaign from selected campaign
    suggested_utm_campaign = ""
    if st.session_state.selected_campaign_id:
        campaigns = list_campaigns(ss_env.spreadsheet)
        selected_campaign = next((c for c in campaigns if c["id"] == st.session_state.selected_campaign_id), None)
        if selected_campaign:
            suggested_utm_campaign = generate_campaign_utm_name(selected_campaign["name"])

    df = st.session_state.bulk_df.copy()

    # Pre-populate utm_campaign with suggested value for new rows
    if suggested_utm_campaign:
        for i in df.index:
            if not df.at[i, "utm_campaign"]:
                df.at[i, "utm_campaign"] = suggested_utm_campaign

    for col in ["base_url", "utm_campaign", "utm_source", "utm_medium", "utm_content", "utm_term", "template"]:
        if col not in df.columns:
            df[col] = ""

    if templates_df.empty:
        t_help = "(no templates yet)"
    else:
        t_help = f"Available: {', '.join(templates_df['name'].tolist())}"

    edited = st.data_editor(
        df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "base_url": st.column_config.TextColumn("base_url", help="The page URL you want to tag."),
            "utm_campaign": st.column_config.TextColumn("utm_campaign", help="Auto-suggested from campaign name"),
            "utm_source": st.column_config.TextColumn("utm_source"),
            "utm_medium": st.column_config.TextColumn("utm_medium"),
            "utm_content": st.column_config.TextColumn("utm_content"),
            "utm_term": st.column_config.TextColumn("utm_term"),
            "template": st.column_config.TextColumn("template", help=t_help),
        },
        hide_index=True,
        key="bulk_editor",
    )

    if not templates_df.empty and not edited.empty:
        merged = edited.copy()
        tmap = templates_df.set_index("name")[
            ["source", "medium", "content", "term"]
        ].to_dict(orient="index")
        for i, row in merged.iterrows():
            tname = str(row.get("template", "")).strip()
            if tname and tname in tmap:
                row_source = row.get("utm_source", "") or tmap[tname]["source"]
                row_medium = row.get("utm_medium", "") or tmap[tname]["medium"]
                row_content = row.get("utm_content", "") or tmap[tname]["content"]
                row_term = row.get("utm_term", "") or tmap[tname]["term"]
                merged.at[i, "utm_source"] = row_source
                merged.at[i, "utm_medium"] = row_medium
                merged.at[i, "utm_content"] = row_content
                merged.at[i, "utm_term"] = row_term
        edited = merged

    def format_row(row):
        row["utm_campaign"] = apply_formatting(row.get("utm_campaign", ""), force_lower, space_style)
        row["utm_source"] = apply_formatting(row.get("utm_source", ""), force_lower, space_style)
        row["utm_medium"] = apply_formatting(row.get("utm_medium", ""), force_lower, space_style)
        row["utm_content"] = apply_formatting(row.get("utm_content", ""), force_lower, space_style)
        row["utm_term"] = apply_formatting(row.get("utm_term", ""), force_lower, space_style)
        row["final_url"] = build_utm_url(
            row.get("base_url", ""),
            {
                "utm_campaign": row.get("utm_campaign", ""),
                "utm_source": row.get("utm_source", ""),
                "utm_medium": row.get("utm_medium", ""),
                "utm_content": row.get("utm_content", ""),
                "utm_term": row.get("utm_term", ""),
            },
        )
        return row

    if not edited.empty:
        preview_df = edited.apply(format_row, axis=1)
        st.markdown("**Preview (generated URLs):**")
        st.dataframe(preview_df[[
            "base_url", "utm_campaign", "utm_source", "utm_medium", "utm_content", "utm_term", "final_url"
        ]], use_container_width=True, hide_index=True)

        st.session_state.bulk_df = edited

        if st.session_state.selected_campaign_id and st.button("Save all to selected campaign", type="primary"):
            to_save = preview_df[[
                "base_url", "utm_campaign", "utm_source", "utm_medium", "utm_content", "utm_term", "final_url"
            ]].copy()
            to_save = to_save[to_save["final_url"].astype(bool) & to_save["base_url"].astype(bool)]
            if to_save.empty:
                st.warning("Nothing to save — please complete at least one row.")
            else:
                insert_utm_links(ss_env.spreadsheet, st.session_state.selected_campaign_id, to_save)
                snack(f"Saved {len(to_save)} link(s) to campaign")
        elif not st.session_state.selected_campaign_id:
            st.info("Select or create a campaign in the sidebar to save links.")
    else:
        st.caption("Add some rows above to generate URLs.")


# =============================================================================
# Future Integrations (placeholders)
# =============================================================================

def push_to_google_sheets_placeholder(df: pd.DataFrame, spreadsheet_url: str):
    """Not needed now: app already uses Sheets for persistence."""
    pass


def chatgpt_insights_placeholder(campaign_id: int):
    """Placeholder for future ChatGPT-powered summaries/insights."""
    pass


# =============================================================================
# Main
# =============================================================================

def main():
    st.set_page_config(page_title="UTM Builder", page_icon="📖", layout="wide")
    ensure_session_state()

    # Connect to Google Sheets
    try:
        ss_env = connect_sheets()
    except Exception as e:
        st.error(
            "Google Sheets connection failed. Check Streamlit secrets (gcp_service_account and gsheets.*). "
            + str(e)
        )
        return

    # Sidebar (campaigns only now)
    sidebar_campaigns(ss_env)

    st.title("📖 UTM Builder — Cloud Edition")
    st.write("Create, format, and store UTM-tagged links under campaigns. Save and reuse templates. Export anytime.")

    # Formatting controls
    force_lower, space_style = formatting_controls()

    # Load templates for use in builders
    templates_df = list_templates(ss_env.spreadsheet)

    # Main tabs: Single / Bulk / Campaign Links / Presets
    t1, t2, t3, t4 = st.tabs(["Single", "Bulk", "Campaign Links", "Presets"])

    with t1:
        single_builder(ss_env, force_lower, space_style, templates_df)

    with t2:
        bulk_builder(ss_env, force_lower, space_style, templates_df)

def campaign_links_tab(ss_env: SheetsEnv):
    """Enhanced Campaign Links tab with editing and search features"""
    if not st.session_state.selected_campaign_id:
        st.info("Select a campaign in the sidebar to view its links.")
        return
        
    # Get current campaign info
    campaigns = list_campaigns(ss_env.spreadsheet)
    selected_campaign = next((c for c in campaigns if c["id"] == st.session_state.selected_campaign_id), None)
    if not selected_campaign:
        st.error("Selected campaign not found.")
        return
        
    st.subheader(f"Links for: {selected_campaign['name']}")
    
    # Load links
    try:
        links_df = load_campaign_links(ss_env.spreadsheet, st.session_state.selected_campaign_id)
    except Exception as e:
        st.error(f"Error loading campaign links: {str(e)}")
        return
    
    if links_df.empty:
        st.info("No links yet in this campaign. Create some in the Single or Bulk tabs!")
        return
    
    # Search and filter controls
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        search_term = st.text_input("Search links", placeholder="Search in URLs, source, medium...")
    with col2:
        source_filter = st.selectbox("Filter by source", options=["All"] + sorted(links_df["utm_source"].unique().tolist()))
    with col3:
        medium_filter = st.selectbox("Filter by medium", options=["All"] + sorted(links_df["utm_medium"].unique().tolist()))
    
    # Apply filters
    filtered_df = links_df.copy()
    
    if search_term:
        mask = (
            filtered_df["final_url"].str.contains(search_term, case=False, na=False) |
            filtered_df["base_url"].str.contains(search_term, case=False, na=False) |
            filtered_df["utm_source"].str.contains(search_term, case=False, na=False) |
            filtered_df["utm_medium"].str.contains(search_term, case=False, na=False) |
            filtered_df["utm_content"].str.contains(search_term, case=False, na=False)
        )
        filtered_df = filtered_df[mask]
    
    if source_filter != "All":
        filtered_df = filtered_df[filtered_df["utm_source"] == source_filter]
    
    if medium_filter != "All":
        filtered_df = filtered_df[filtered_df["utm_medium"] == medium_filter]
    
    # Display results count
    st.caption(f"Showing {len(filtered_df)} of {len(links_df)} links")
    
    if filtered_df.empty:
        st.info("No links match your search criteria.")
        return
    
    # Display links in a simple table format for now
    st.subheader("Campaign Links")
    display_columns = ["id", "base_url", "utm_source", "utm_medium", "utm_content", "utm_term", "final_url", "created_at"]
    st.dataframe(filtered_df[display_columns], use_container_width=True, hide_index=True)
    
    # Simple link management
    st.subheader("Link Actions")
    link_id_to_delete = st.number_input("Enter Link ID to delete", min_value=0, step=1, value=0)
    if link_id_to_delete and st.button("Delete Link", type="secondary"):
        if delete_utm_link(ss_env.spreadsheet, int(link_id_to_delete)):
            snack("Link deleted")
            st.rerun()
        else:
            st.error("Failed to delete link or link not found")
    
    # Click tracking info
    with st.expander("Click Tracking Setup", expanded=False):
        st.write("**Google Analytics:**")
        st.code("gtag('event', 'utm_click', {'campaign_name': 'your-campaign'});")
        
        st.write("**URL Shortening Services:**")
        st.write("- Bit.ly: Built-in analytics")  
        st.write("- TinyURL: Basic click counting")
        st.write("- Custom: Use your own domain with tracking")
            
    with t4:
        presets_tab(ss_env)

    st.divider()
    with st.expander("Integrations (coming soon)"):
        st.caption("ChatGPT insights placeholder is ready to be implemented when needed.")


if __name__ == "__main__":
    main()
