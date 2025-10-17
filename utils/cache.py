import hashlib
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import pandas as pd
from urllib.parse import urlparse

CSV_FIELDS = ["provider","url","postcode","plan_name","speed_mbps","monthly_price_gbp",
              "upfront_fee_gbp","contract_months","scraped_at","card_text_sample","row_id"]

def provider_of(url: str) -> str:
    return (urlparse(url).hostname or "").replace("www.", "")

def providers_from_urls(urls: List[str]) -> List[str]:
    return [provider_of(u) for u in urls]

def load_csv_if_exists(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
        for c in CSV_FIELDS:
            if c not in df.columns:
                df[c] = pd.NA
        if "scraped_at" in df.columns:
            df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True, errors="coerce")
        else:
            df["scraped_at"] = pd.NaT
        if "row_id" not in df.columns or df["row_id"].isna().all():
            def mk_id(row):
                raw = f"{row.get('provider')}|{row.get('plan_name')}|{row.get('speed_mbps')}|{row.get('monthly_price_gbp')}|{row.get('url')}"
                return hashlib.sha1(str(raw).encode("utf-8")).hexdigest()[:12]
            df["row_id"] = df.apply(mk_id, axis=1)
        return df[CSV_FIELDS]
    except FileNotFoundError:
        return pd.DataFrame(columns=CSV_FIELDS)

def append_to_csv(path: str, new_df: pd.DataFrame, dedupe: bool = True):
    if new_df is None or new_df.empty:
        return
    existing = load_csv_if_exists(path)
    all_df = pd.concat([existing, new_df[CSV_FIELDS]], ignore_index=True)
    if dedupe:
        keys = ["provider","url","postcode","plan_name","speed_mbps","monthly_price_gbp"]
        all_df["scraped_at"] = pd.to_datetime(all_df["scraped_at"], utc=True, errors="coerce")
        all_df.sort_values("scraped_at", ascending=False, inplace=True)
        all_df = all_df.drop_duplicates(subset=keys, keep="first")
    all_df.to_csv(path, index=False)

def split_cached_and_missing(csv_df: pd.DataFrame, postcode: str, providers: List[str],
                             max_age_hours: int, force_csv_only: bool) -> Tuple[pd.DataFrame, List[str], List[dict]]:
    status = []
    cached_rows = pd.DataFrame(columns=CSV_FIELDS)
    providers_set = set(providers)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=max_age_hours)

    subset = csv_df[(csv_df["postcode"].str.upper() == postcode.upper()) & (csv_df["provider"].isin(providers_set))].copy()
    if subset.empty:
        status.append({"provider": "all", "url": "", "step": "cache_miss", "detail": "no matching rows in CSV", "allowed": None})
        return cached_rows, providers, status

    if force_csv_only:
        cached_rows = subset.copy()
        for pr in providers:
            rows = subset[subset["provider"] == pr]
            status.append({"provider": pr, "url": "", "step": "cache_used_forced", "detail": f"rows={len(rows)} (age ignored)", "allowed": None})
        return cached_rows, [], status

    stale_to_scrape, fresh_used = [], []
    for pr in providers:
        rows = subset[subset["provider"] == pr]
        if rows.empty:
            stale_to_scrape.append(pr)
            status.append({"provider": pr, "url": "", "step": "cache_miss_provider", "detail": "no rows for provider", "allowed": None})
            continue
        if "scraped_at" in rows.columns and rows["scraped_at"].notna().any():
            latest = rows["scraped_at"].max()
            if latest >= cutoff:
                fresh_rows = rows[rows["scraped_at"] >= cutoff].copy()
                cached_rows = pd.concat([cached_rows, fresh_rows], ignore_index=True)
                fresh_used.append(pr)
                status.append({"provider": pr, "url": "", "step": "cache_used", "detail": f"fresh rows={len(fresh_rows)}", "allowed": None})
            else:
                stale_to_scrape.append(pr)
                status.append({"provider": pr, "url": "", "step": "cache_stale", "detail": f"latest={latest.isoformat()}", "allowed": None})
        else:
            stale_to_scrape.append(pr)
            status.append({"provider": pr, "url": "", "step": "cache_no_timestamp", "detail": "", "allowed": None})

    return cached_rows, stale_to_scrape, status
