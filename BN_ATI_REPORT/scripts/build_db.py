#!/usr/bin/env python3
from __future__ import annotations

import io
import re
import time
import sqlite3
from pathlib import Path
from typing import Iterable, List, Dict

import pandas as pd
import requests
from documentcloud import DocumentCloud
from datetime import date

# --------------------
# Config
# --------------------
# CKAN resource dumps (CSV)
A_URL = "https://open.canada.ca/data/en/datastore/dump/299a2e26-5103-4a49-ac3a-53db9fcc06c7?format=csv"
B_URL = "https://open.canada.ca/data/en/datastore/dump/e664cf3d-6cb7-4aaa-adfa-e459c2552e3e?format=csv"
C_URL = "https://open.canada.ca/data/en/datastore/dump/19383ca2-b01a-487d-88f7-e1ffbc7d39c2?format=csv"

# DocumentCloud query (as requested)
DOCCLOUD_QUERY = 'organization:38956 created_at:[NOW-10DAY TO NOW]'
DOCCLOUD_PER_PAGE = 25

# Outputs
ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs"
OUT_SQLITE = OUT_DIR / "data.sqlite"

# Regex chunking for fuzzy search
TN_REGEX_CHUNK = 400

# Weak IDs
WEAK_BN_VALUES = {s.lower() for s in ["c", "1", "0", "NA", "na", "-", "REDACTED", "[REDACTED]", "TBD-PM-00"]}


# --------------------
# Helpers
# --------------------
def download_csv_df(url: str, retries: int = 4, chunk_size: int = 1024 * 1024) -> pd.DataFrame:
    last_err = None
    for i in range(retries):
        try:
            with requests.get(url, stream=True, timeout=90) as r:
                r.raise_for_status()
                buf = io.BytesIO()
                for part in r.iter_content(chunk_size=chunk_size):
                    if part:
                        buf.write(part)
                buf.seek(0)
            return pd.read_csv(buf, dtype=str, keep_default_na=False).fillna("")
        except Exception as e:
            last_err = e
            print(f"[download_csv_df] attempt {i+1}/{retries} failed: {e}")
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"Failed to download {url}: {last_err}")


def agg_unique_identifiers(series: pd.Series) -> str:
    vals = [str(x).strip() for x in series if str(x).strip()]
    return "; ".join(sorted(set(vals))) if vals else ""


def iter_chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def is_weak(v: str) -> bool:
    s = (str(v) or "").strip().lower()
    return s in WEAK_BN_VALUES


# --------------------
# DocumentCloud → DataFrame
# --------------------
def fetch_doccloud_table() -> pd.DataFrame:
    """
    Returns DataFrame with:
      owner_org, request_number, tracking_number, open_by_default_url, open_by_default_flag
    We try to read owner_org / request_number / tracking_number from doc.data (metadata).
    If not present, we leave them empty (no join for those rows).
    """
    try:
        print("🔎 DocumentCloud: querying...")
        client = DocumentCloud()  # uses env auth (username/password or token)
        docs = client.documents.search(query=DOCCLOUD_QUERY, per_page=DOCCLOUD_PER_PAGE)

        recs = []
        for d in docs:
            data = getattr(d, "data", None) or {}
            owner_org = str(data.get("owner_org", "")).strip()
            request_number = str(data.get("request_number", "")).strip()
            tracking_number = str(data.get("tracking_number", "")).strip()
            open_url = str(data.get("open_by_default_url", "")).strip()

            recs.append({
                "owner_org": owner_org,
                "request_number": request_number,
                "tracking_number": tracking_number,
                "open_by_default_url": open_url,
                "open_by_default_flag": 1 if open_url else 0,
            })

        df = pd.DataFrame(recs, dtype=str)
        if df.empty:
            df = pd.DataFrame(
                columns=[
                    "owner_org",
                    "request_number",
                    "tracking_number",
                    "open_by_default_url",
                    "open_by_default_flag",
                ]
            )
        else:
            df["open_by_default_flag"] = df["open_by_default_flag"].astype(int)
        print(f"DocumentCloud rows: {len(df):,}")
        return df.fillna("")
    except Exception as err:  # network/auth failures shouldn't break build
        print(f"⚠️  DocumentCloud query failed: {err}")
        return pd.DataFrame(
            columns=[
                "owner_org",
                "request_number",
                "tracking_number",
                "open_by_default_url",
                "open_by_default_flag",
            ]
        )


# --------------------
# Main build
# --------------------
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) B (aggregate)
    print("⬇️ Downloading B …")
    dfB = download_csv_df(B_URL)
    needB = ["owner_org", "Request Number", "Number of Informal Requests", "Unique Identifier"]
    missB = [c for c in needB if c not in dfB.columns]
    if missB:
        raise ValueError(f"B missing columns: {missB}")

    metric_col = "Number of Informal Requests"
    dfB[metric_col] = pd.to_numeric(dfB[metric_col], errors="coerce").fillna(0.0)
    dfB_agg = (
        dfB.groupby(["owner_org", "Request Number"], as_index=False)
           .agg({metric_col: "sum", "Unique Identifier": agg_unique_identifiers})
           .rename(columns={metric_col: "informal_requests_sum",
                            "Unique Identifier": "unique_identifiers"})
    )
    dfB_agg["request_number_lc"] = dfB_agg["Request Number"].str.lower()
    print(f"B rows: {len(dfB):,}  (agg: {len(dfB_agg):,})")

    # 2) C (merge with B)
    print("⬇️ Downloading C …")
    dfC = download_csv_df(C_URL)
    for c in ("owner_org", "request_number", "summary_en", "summary_fr"):
        if c not in dfC.columns:
            dfC[c] = ""
    dfC["request_number_lc"] = dfC["request_number"].str.lower()

    dfBC = dfC.merge(
        dfB_agg.drop(columns=["Request Number"]),
        on=["owner_org", "request_number_lc"],
        how="left",
    )
    if "unique_identifiers" not in dfBC.columns:
        dfBC["unique_identifiers"] = ""
    dfBC["unique_identifiers"] = dfBC["unique_identifiers"].fillna("")
    dfBC["informal_requests_sum"] = pd.to_numeric(dfBC.get("informal_requests_sum", 0.0), errors="coerce").fillna(0.0)
    dfBC["__haystack"] = (dfBC["summary_en"] + " " + dfBC["summary_fr"]).str.lower()
    print(f"C rows: {len(dfC):,}  Merged BC rows: {len(dfBC):,}")

    # 3) A
    print("⬇️ Downloading A …")
    dfA = download_csv_df(A_URL)
    needA = ["owner_org", "tracking_number"]
    missA = [c for c in needA if c not in dfA.columns]
    if missA:
        raise ValueError(f"A missing columns: {missA}")
    dfA["tn_lc"] = dfA["tracking_number"].str.lower()
    print(f"A rows: {len(dfA):,}")

    # 4) Fuzzy match A.tracking_number in C summaries by owner_org
    results = []
    orgs = sorted(set(dfA["owner_org"]).intersection(set(dfBC["owner_org"])))
    print(f"Matching across {len(orgs)} owner_org groups …")
    for org in orgs:
        a_org = (
            dfA.loc[dfA["owner_org"] == org, ["tn_lc", "tracking_number"]]
               .drop_duplicates()
               .reset_index(drop=True)
        )
        if a_org.empty:
            continue

        lut = dict(zip(a_org["tn_lc"], a_org["tracking_number"]))
        bc_org = dfBC.loc[
            dfBC["owner_org"] == org,
            ["owner_org", "request_number", "informal_requests_sum",
             "unique_identifiers", "summary_en", "summary_fr", "__haystack"],
        ].copy()
        if bc_org.empty:
            continue

        tn_list = [t for t in a_org["tn_lc"].tolist() if t]
        if not tn_list:
            continue

        matched_blocks = []
        for chunk in iter_chunks(tn_list, TN_REGEX_CHUNK):
            parts = [re.escape(t) for t in chunk]
            pattern = "(?:" + "|".join(parts) + ")"
            mask = bc_org["__haystack"].str.contains(pattern, regex=True)
            if not mask.any():
                continue
            sub = bc_org.loc[mask].copy()
            sub.loc[:, "_match_lc"] = sub["__haystack"].str.extract("(" + pattern + ")", expand=False)
            sub.loc[:, "tracking_number"] = sub["_match_lc"].map(lut).fillna(sub["_match_lc"])
            matched_blocks.append(
                sub[["owner_org", "tracking_number", "request_number",
                     "informal_requests_sum", "unique_identifiers", "summary_en", "summary_fr"]]
            )
        if matched_blocks:
            results.append(pd.concat(matched_blocks, ignore_index=True))

    if results:
        df_out = pd.concat(results, ignore_index=True).drop_duplicates()
    else:
        df_out = pd.DataFrame(columns=[
            "owner_org", "tracking_number", "request_number",
            "informal_requests_sum", "unique_identifiers", "summary_en", "summary_fr"
        ])

    print(f"Matches (pre-filter): {len(df_out):,}")

    # 5) Split weak/strong
    df_weak = df_out[df_out["tracking_number"].map(is_weak)].copy()
    df_strong = df_out[~df_out["tracking_number"].map(is_weak)].copy()

    # 6) Fetch DocCloud table and merge onto strong
    df_dc = fetch_doccloud_table()
    if not df_dc.empty:
        # Ensure schemas
        for col in ("owner_org", "request_number", "tracking_number", "open_by_default_url", "open_by_default_flag"):
            if col not in df_dc.columns:
                df_dc[col] = "" if col != "open_by_default_flag" else 0

        # Preferred join: owner_org + request_number
        df_strong = df_strong.merge(
            df_dc[["owner_org","request_number","open_by_default_url","open_by_default_flag"]],
            on=["owner_org","request_number"],
            how="left"
        )

        # Fallback: owner_org + tracking_number (fill only where missing)
        mask_missing = df_strong["open_by_default_url"].isna() | (df_strong["open_by_default_url"].astype(str).str.strip() == "")
        if mask_missing.any():
            d2 = df_dc[["owner_org","tracking_number","open_by_default_url","open_by_default_flag"]].copy()
            df_strong = df_strong.merge(
                d2,
                on=["owner_org","tracking_number"],
                how="left",
                suffixes=("","_by_tn")
            )
            df_strong["open_by_default_url"] = df_strong["open_by_default_url"].where(
                ~mask_missing, df_strong["open_by_default_url_by_tn"]
            )
            df_strong["open_by_default_flag"] = df_strong["open_by_default_flag"].where(
                ~mask_missing, df_strong["open_by_default_flag_by_tn"]
            )
            df_strong.drop(columns=[c for c in df_strong.columns if c.endswith("_by_tn")], inplace=True)

        df_strong["open_by_default_url"] = df_strong["open_by_default_url"].fillna("")
        df_strong["open_by_default_flag"] = df_strong["open_by_default_flag"].fillna(0).astype(int)
    else:
        df_strong["open_by_default_url"] = ""
        df_strong["open_by_default_flag"] = 0

    print(f"Weak BN IDs: {len(df_weak):,}  |  Strong BN IDs: {len(df_strong):,}")

    # 7) Write SQLite (paged consumption by the site)
    if OUT_SQLITE.exists():
        OUT_SQLITE.unlink()

    con = sqlite3.connect(OUT_SQLITE)
    cur = con.cursor()
    cur.executescript("""
    PRAGMA journal_mode=OFF;
    PRAGMA synchronous=OFF;
    PRAGMA temp_store=MEMORY;

    DROP TABLE IF EXISTS strong_matches;
    CREATE TABLE strong_matches (
      owner_org TEXT,
      tracking_number TEXT,
      request_number TEXT,
      informal_requests_sum REAL,
      unique_identifiers TEXT,
      open_by_default_url TEXT,
      open_by_default_flag INTEGER,
      summary_en TEXT,
      summary_fr TEXT
    );

    DROP TABLE IF EXISTS weak_matches;
    CREATE TABLE weak_matches (
      owner_org TEXT,
      tracking_number TEXT
    );

    DROP TABLE IF EXISTS meta_counts;
    CREATE TABLE meta_counts (
      key TEXT PRIMARY KEY,
      value TEXT
    );

    CREATE INDEX idx_strong_owner_org ON strong_matches(owner_org);
    CREATE INDEX idx_strong_req ON strong_matches(request_number);
    CREATE INDEX idx_strong_track ON strong_matches(tracking_number);
    """)

    if not df_strong.empty:
        df_strong.to_sql("strong_matches", con, if_exists="append", index=False)
    if not df_weak.empty:
        df_weak[["owner_org","tracking_number"]].to_sql("weak_matches", con, if_exists="append", index=False)

    # Optional: FTS (kept for future search UI)
    cur.executescript("""
    DROP TABLE IF EXISTS strong_fts;
    CREATE VIRTUAL TABLE strong_fts USING fts5(
      owner_org, request_number, tracking_number, summary_en, summary_fr,
      content='strong_matches', content_rowid='rowid'
    );
    INSERT INTO strong_fts(rowid, owner_org, request_number, tracking_number, summary_en, summary_fr)
      SELECT rowid, owner_org, request_number, tracking_number, summary_en, summary_fr
      FROM strong_matches;
    """)

    counts_common: Dict[str, int] = {
        "A_rows": int(len(dfA)),
        "B_rows": int(len(dfB)),
        "C_rows": int(len(dfC)),
        "BC_rows": int(len(dfBC)),
        "matches": int(len(df_out)),
        "weak_matches": int(len(df_weak)),
        "strong_matches": int(len(df_strong)),
    }
    cur.executemany(
        "INSERT INTO meta_counts(key,value) VALUES (?,?)",
        [(k, str(v)) for k, v in counts_common.items()] + [("build_date", date.today().isoformat())],
    )

    con.commit()
    con.close()
    print(f"✅ Wrote {OUT_SQLITE}")


if __name__ == "__main__":
    main()
