# --- Streamlit Cloud bootstrap for Playwright ---
import os, subprocess, sys

def _ensure_playwright_browser():
    # On Streamlit Cloud, apt libs come from packages.txt; just install Chromium binary.
    # Keep it NO-OP locally.
    if os.environ.get("STREAMLIT_RUNTIME", "0") == "1" or os.environ.get("STREAMLIT_SERVER_ENABLED"):
        try:
            subprocess.run(
                [sys.executable, "-m", "playwright", "install", "chromium"],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except Exception:
            pass

_ensure_playwright_browser()

import asyncio
import json
import time
import hashlib
from typing import Dict, Optional, List

import pandas as pd
import streamlit as st

from utils.cache import (
    load_csv_if_exists, append_to_csv, split_cached_and_missing,
    CSV_FIELDS, providers_from_urls, provider_of
)
from utils.theme import load_theme_tokens, apply_theme_css, pick_palette
from scrapers.scrapers import scrape_many
from ui.ui import render_charts, render_comparison, render_status_panel
# ------------------------------------------------


# ---------- Streamlit setup ----------
st.set_page_config(page_title="Switch Smarter: UK Broadband Price & Speed Tool", page_icon="📶", layout="wide")
st.title("📶 Switch Smarter: UK Broadband Price & Speed Tool")
st.caption("Compare the best deals from major UK broadband providers in your postcode.")

# ---------- Session state ----------
if "results_df" not in st.session_state:
    st.session_state.results_df = pd.DataFrame(columns=CSV_FIELDS)
if "compare_ids" not in st.session_state:
    st.session_state.compare_ids = set()
if "status_rows" not in st.session_state:
    st.session_state.status_rows = []

# ---------- Sidebar inputs ----------
with st.sidebar:
    st.header("Inputs")
    postcode = st.text_input("Postcode", value="TW8 0FD")

    DEFAULT_ISPS = {
        "BT": "https://www.bt.com/broadband",
        "Virgin Media": "https://www.virginmedia.com/broadband",
        "Sky": "https://www.sky.com/broadband/buy",
        "TalkTalk": "https://www.talktalk.co.uk/",
        "Vodafone": "https://www.vodafone.co.uk/broadband",
        "EE": "https://ee.co.uk/broadband",
        "Plusnet": "https://www.plus.net/broadband/",
        "NOW": "https://www.nowtv.com/broadband",
    }

    providers_sel = st.multiselect(
        "Choose providers",
        options=list(DEFAULT_ISPS.keys()),
        default=list(DEFAULT_ISPS.keys())
    )

    custom_urls_text = st.text_area("Extra ISP URLs (one per line)", placeholder="https://example.com/broadband")

    st.markdown("### Address Selection")
    addr_hint = st.text_input("Address contains (optional)", help="e.g., house/flat number or street substring")
    addr_index = st.number_input("Or pick address by index (1-based)", min_value=1, value=1, step=1)

    moving_choice = st.selectbox("Are you moving to this address?", ["Auto (detect)", "Yes", "No"])
    moving_bool: Optional[bool] = None if moving_choice.startswith("Auto") else (moving_choice == "Yes")

    with st.expander("Advanced"):
        max_steps = st.slider("Max wizard steps", min_value=3, max_value=12, value=6,
                              help="How many times to try address/moving/continue before giving up")
        extra_fields_raw = st.text_area(
            "Extra form fields (JSON)", 
            placeholder='{"House number":"3", "Address line 2":"Flat B"}',
            help="Optional mapping of label/placeholder/name/id substrings to values"
        )
        respect_robots = st.toggle("Respect robots.txt (recommended)", value=True,
                                   help="Skip disallowed paths per each site's robots.txt")

        st.markdown("### CSV Cache")
        csv_path = st.text_input("CSV path", value="offers.csv")
        data_mode = st.radio(
            "Source of data",
            ["Auto (reuse if < 1 day old)", "Use CSV only (even if older)", "Always refresh (ignore CSV)"],
            index=0
        )
        dedupe_csv = st.toggle("Dedupe on append (keep latest per plan/price)", value=True)

        st.markdown("### Theme")
        theme_csv_path = st.text_input("Theme tokens CSV", value="data/EXL-Orange_PriceCompare_Theme_Tokens.csv")
        theme = load_theme_tokens(theme_csv_path)   # loads + falls back safely
        apply_theme_css(theme)                      # light CSS for headings/buttons
        palette = pick_palette(theme, need=12)      # colors for charts

    # ---------- Debug browser controls ----------
    with st.expander("Debug browser"):
        headed = st.toggle("Show browser window", value=False)
        slow_mo_ms = st.slider("Slow motion (ms per action)", 0, 1000, 0, step=50)
        devtools = st.toggle("Open DevTools", value=False)
        record_video_dir = st.text_input("Record videos to folder (optional)", value="")
        record_video_dir = record_video_dir or None
        record_har_path = st.text_input("Record HAR to file (optional)", value="")
        record_har_path = record_har_path or None
        trace_path = st.text_input("Record Playwright trace to (optional .zip)", value="")
        trace_path = trace_path or None
        pause_on_start = st.toggle("Pause on start (Playwright Inspector)", value=False)
        console_log_path = st.text_input(
            "Console log file (optional)",
            value="",
            help="If blank, a timestamped log will be created under logs/ (e.g., logs/console-YYYYMMDD-HHMMSS.log)"
        )
        console_log_path = console_log_path or None

    run_btn = st.button("Fetch & Compare", type="primary")
    clear_btn = st.button("Clear results/status")

# ---------- Build URL list ----------
chosen_urls = [DEFAULT_ISPS[name] for name in providers_sel]
if custom_urls_text.strip():
    for line in custom_urls_text.splitlines():
        s = line.strip()
        if s:
            if not s.startswith("http"):
                s = "https://" + s
            chosen_urls.append(s)

# ---------- Parse extra form fields ----------
extra_fields: Optional[Dict[str, str]] = None
if extra_fields_raw and extra_fields_raw.strip():
    try:
        parsed = json.loads(extra_fields_raw)
        if isinstance(parsed, dict):
            extra_fields = {str(k): str(v) for k, v in parsed.items()}
        else:
            st.warning("Extra form fields must be a JSON object (key/value). Ignoring.")
    except Exception as e:
        st.warning(f"Invalid JSON for extra form fields: {e}")

# ---------- Clear ----------
if clear_btn:
    st.session_state.results_df = pd.DataFrame(columns=CSV_FIELDS)
    st.session_state.status_rows = []
    st.session_state.compare_ids = set()

# ---------- Run ----------
if run_btn:
    if not postcode.strip():
        st.error("Please enter a postcode.")
    elif not chosen_urls:
        st.error("Choose at least one provider or add a URL.")
    else:
        with st.spinner("Loading cached data & scraping as needed…"):
            try:
                providers_hosts = providers_from_urls(chosen_urls)

                # CSV split
                csv_df = load_csv_if_exists(csv_path)
                mode = data_mode.split(" ")[0].lower()
                force_csv_only = (mode == "use")
                force_refresh = (mode == "always")

                cached_df = pd.DataFrame(columns=CSV_FIELDS)
                to_scrape_urls = chosen_urls[:]  # default
                status_rows: List[Dict] = []

                if force_refresh:
                    status_rows.append({"provider": "all", "url": "", "step": "cache_ignored_refresh", "detail": "", "allowed": None})
                    to_scrape_urls = chosen_urls
                else:
                    cached_subset, missing_providers, strows = split_cached_and_missing(
                        csv_df, postcode.strip(), providers_hosts,
                        max_age_hours=24, force_csv_only=force_csv_only
                    )
                    status_rows.extend(strows)
                    cached_df = cached_subset.copy()
                    if force_csv_only:
                        to_scrape_urls = []
                        if cached_df.empty:
                            status_rows.append({"provider": "all", "url": "", "step": "cache_empty_fallback_scrape", "detail": "", "allowed": None})
                            to_scrape_urls = chosen_urls
                    else:
                        to_scrape_urls = [u for u in chosen_urls if provider_of(u) in set(missing_providers)]

                # scrape (if needed)
                scraped_df = pd.DataFrame(columns=CSV_FIELDS)
                if to_scrape_urls:
                    t0 = time.perf_counter()
                    fresh_df, scrape_status = asyncio.run(
                        scrape_many(
                            postcode.strip().upper(),
                            to_scrape_urls,
                            address_hint=addr_hint.strip() or None,
                            address_index=int(addr_index),
                            moving=moving_bool,
                            extra_fields=extra_fields,
                            max_steps=int(max_steps),
                            respect_robots=respect_robots,
                            # ---- debug flags ----
                            headed=headed,
                            slow_mo_ms=slow_mo_ms,
                            devtools=devtools,
                            record_video_dir=record_video_dir,
                            record_har_path=record_har_path,
                            trace_path=trace_path,
                            pause_on_start=pause_on_start,
                            console_log_path=console_log_path,   # <-- NEW
                        )
                    )
                    elapsed = time.perf_counter() - t0
                    status_rows.extend(scrape_status)
                    scraped_df = fresh_df
                    st.info(f"Scraped {len(scraped_df)} rows from {len(to_scrape_urls)} site(s) in {elapsed:.1f}s.")
                    append_to_csv(csv_path, scraped_df, dedupe=dedupe_csv)
                else:
                    st.info("No scraping needed — using cached CSV rows.")

                combined = pd.concat([cached_df, scraped_df], ignore_index=True)
                if not combined.empty and "row_id" not in combined.columns:
                    def mk_id(row):
                        raw = f"{row.get('provider')}|{row.get('plan_name')}|{row.get('speed_mbps')}|{row.get('monthly_price_gbp')}|{row.get('url')}"
                        return hashlib.sha1(str(raw).encode("utf-8")).hexdigest()[:12]
                    combined["row_id"] = combined.apply(mk_id, axis=1)

                st.session_state.results_df = combined
                st.session_state.status_rows = status_rows
                st.session_state.compare_ids = set()

                st.success(f"Ready • {len(combined)} offer rows")
            except Exception as e:
                st.error(f"Run error: {e}")

# ---------- Main body ----------
df = st.session_state.results_df
status_rows = st.session_state.status_rows

st.markdown("## Results")
if df.empty:
    st.info("Pick providers, enter a postcode, and click **Fetch & Compare**.")
else:
    # CHARTS FIRST with clear titles
    st.markdown("### Entry Price by Provider")
    st.caption("Lowest monthly price found for each provider (at any speed).")
    render_charts(df, palette=palette, section="entry_price")

    st.markdown("### Price vs. Speed")
    st.caption("All offers shown. Prices are monthly; speeds in Mb/s.")
    render_charts(df, palette=palette, section="scatter")

    # COMPARISON TABLE AFTER CHARTS
    st.markdown("### Compare Selected Plans")
    st.caption("Filter, sort, and tick rows to compare cards side-by-side.")
    render_comparison(df)

# STATUS AT THE BOTTOM
st.markdown("## Status & Debug")
st.caption("Robots decisions, cache hits, navigation steps/pages, and offer counts.")
render_status_panel(status_rows)
