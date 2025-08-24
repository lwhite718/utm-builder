"""
Streamlit UTM Builder (MVP, Streamlit Cloud edition)
Author: ED: Dev Wizard

Persistence: Google Sheets via Service Account (Streamlit Cloud friendly)
- Tabs: campaigns, templates, utm_links
- Secrets: gcp_service_account (JSON), gsheets.spreadsheet_url OR gsheets.spreadsheet_name

Run on Streamlit Cloud or locally (if you set secrets).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit, urlencode, parse_qsl

import pandas as pd
import streamlit as st

# --- Google Sheets (gspread) ---
import gspread
from google.oauth2.service_account import Credentials

# =============================================================================
# Google Sheets Persistence Layer
# =============================================================================

REQ_TABS = {
    "campaigns": ["id", "name", "created_at"],
    "templates": ["id", "name", "source", "medium", "content", "term", "created_at"],
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
            current = ws.row_values(1)
            if current != headers:
                ws.resize(rows=1)
                ws.update([headers])


def _ws(ss: gspread.Spreadsheet, tab: str) -> gspread.Worksheet:
    return ss.worksheet(tab)


def _read_df(ss: gspread.Spreadsheet, tab: str) -> pd.DataFrame:
    ws = _ws(ss, tab)
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=REQ_TABS[tab])
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


# CRUD functions (campaigns / templates / utm_links) ... [unchanged for brevity]
# ...

# =============================================================================
# Main
# =============================================================================

def main():
    st.set_page_config(page_title="UTM Builder", page_icon="🔖", layout="wide")
    ensure_session_state()

    try:
        ss_env = connect_sheets()
    except Exception as e:
        st.error(
            "Google Sheets connection failed. Check Streamlit secrets (gcp_service_account and gsheets.*).\n"
            + str(e)
        )
        return

    sidebar_campaigns(ss_env)
    sidebar_templates(ss_env)

    st.title("🔖 UTM Builder — Cloud Edition")
    st.write("Create, format, and store UTM-tagged links under campaigns. Save and reuse templates. Export anytime.")

    force_lower, space_style = formatting_controls()
    templates_df = list_templates(ss_env.spreadsheet)

    t1, t2, t3 = st.tabs(["Single", "Bulk", "Campaign Links"])

    with t1:
        single_builder(ss_env, force_lower, space_style, templates_df)

    with t2:
        bulk_builder(ss_env, force_lower, space_style, templates_df)

    with t3:
        if st.session_state.selected_campaign_id:
            links_df = load_campaign_links(ss_env.spreadsheet, st.session_state.selected_campaign_id)
            if links_df.empty:
                st.info("No links yet in this campaign.")
            else:
                st.dataframe(links_df, use_container_width=True, hide_index=True)
        else:
            st.info("Select a campaign in the sidebar to view its links.")

    st.divider()
    with st.expander("Integrations (coming soon)"):
        st.caption("ChatGPT insights placeholder is ready to be implemented when needed.")


if __name__ == "__main__":
    main()
