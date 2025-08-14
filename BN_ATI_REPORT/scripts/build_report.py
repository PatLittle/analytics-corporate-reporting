# BN_ATI_REPORT/scripts/build_report.py
"""
Build BN_ATI report:
- Download 3 CSV datasets from the Government of Canada Open Data portal
- B: aggregate "Number of Informal Requests" by (owner_org, Request Number) and collect "Unique Identifier"
- C: merge the B aggregate on (owner_org, request_number)
- A: for each owner_org, find rows in C whose summary_en/summary_fr contain any A.tracking_number
- Emit:
  - docs/report.json (strict JSON, no NaN)
  - docs/index.html   (GCWeb + DataTables table)
"""

from __future__ import annotations

import io
import json
import re
import time
from pathlib import Path
from typing import Iterable, List

import pandas as pd
import requests

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

A_URL = "https://open.canada.ca/data/en/datastore/dump/299a2e26-5103-4a49-ac3a-53db9fcc06c7?format=csv"
B_URL = "https://open.canada.ca/data/en/datastore/dump/e664cf3d-6cb7-4aaa-adfa-e459c2552e3e?format=csv"
C_URL = "https://open.canada.ca/data/en/datastore/dump/19383ca2-b01a-487d-88f7-e1ffbc7d39c2?format=csv"

# Outputs (workflow's working directory is BN_ATI_REPORT)
OUT_DIR = Path("docs")
OUT_JSON = OUT_DIR / "report.json"
OUT_HTML = OUT_DIR / "index.html"

# Regex chunk size per owner_org to avoid extremely long patterns
TN_REGEX_CHUNK = 400

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------


def download_csv_df(url: str, retries: int = 4, chunk_size: int = 1024 * 1024) -> pd.DataFrame:
    """
    Stream a CSV with retries and return a DataFrame with dtype=str (no NaN).
    """
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
            # dtype=str + keep_default_na=False => missing become empty string ""
            return pd.read_csv(buf, dtype=str, keep_default_na=False).fillna("")
        except Exception as e:  # pragma: no cover (network)
            last_err = e
            print(f"[download_csv_df] attempt {i+1}/{retries} failed: {e}")
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"Failed to download {url}: {last_err}")


def agg_unique_identifiers(series: pd.Series) -> str:
    """
    Aggregate Unique Identifier values: dedupe + sort + join with '; '.
    """
    vals = [str(x).strip() for x in series if str(x).strip()]
    if not vals:
        return ""
    return "; ".join(sorted(set(vals)))


def iter_chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


# ---------------------------------------------------------------------
# Main build logic
# ---------------------------------------------------------------------


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ---------------- B: download & aggregate ----------------
    print("Downloading B …")
    dfB = download_csv_df(B_URL)
    needB = ["owner_org", "Request Number", "Number of Informal Requests", "Unique Identifier"]
    missingB = [c for c in needB if c not in dfB.columns]
    if missingB:
        raise ValueError(f"B is missing expected columns: {missingB}")

    metric_col = "Number of Informal Requests"
    # numeric with NaN -> 0.0
    dfB[metric_col] = pd.to_numeric(dfB[metric_col], errors="coerce").fillna(0.0)

    dfB_agg = (
        dfB.groupby(["owner_org", "Request Number"], as_index=False)
        .agg({metric_col: "sum", "Unique Identifier": agg_unique_identifiers})
        .rename(
            columns={
                metric_col: "informal_requests_sum",
                "Unique Identifier": "unique_identifiers",
            }
        )
    )
    # Lowercased join key for robustness
    dfB_agg["request_number_lc"] = dfB_agg["Request Number"].str.lower()
    print(f"B rows: {len(dfB):,}  (agg: {len(dfB_agg):,})")

    # ---------------- C: download & merge with B ----------------
    print("Downloading C …")
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

    # Ensure presence + sanitize NaN for string col
    if "unique_identifiers" not in dfBC.columns:
        dfBC["unique_identifiers"] = ""
    dfBC["unique_identifiers"] = dfBC["unique_identifiers"].fillna("")

    dfBC["informal_requests_sum"] = (
        pd.to_numeric(dfBC.get("informal_requests_sum", 0.0), errors="coerce").fillna(0.0)
    )

    # Haystack for summary matching
    dfBC["__haystack"] = (dfBC["summary_en"] + " " + dfBC["summary_fr"]).str.lower()
    print(f"C rows: {len(dfC):,}  Merged BC rows: {len(dfBC):,}")

    # ---------------- A: download ----------------
    print("Downloading A …")
    dfA = download_csv_df(A_URL)
    needA = ["owner_org", "tracking_number"]
    missingA = [c for c in needA if c not in dfA.columns]
    if missingA:
        raise ValueError(f"A is missing expected columns: {missingA}")

    dfA["tn_lc"] = dfA["tracking_number"].str.lower()
    print(f"A rows: {len(dfA):,}")

    # ---------------- Match per owner_org with regex chunks ----------------
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

        # Lookup to restore original casing
        lut = dict(zip(a_org["tn_lc"], a_org["tracking_number"]))
        # Per-org subset of BC (copy for safe assignment)
        bc_org = dfBC.loc[
            dfBC["owner_org"] == org,
            [
                "owner_org",
                "request_number",
                "informal_requests_sum",
                "unique_identifiers",
                "summary_en",
                "summary_fr",
                "__haystack",
            ],
        ].copy()
        if bc_org.empty:
            continue

        # Build alternation in chunks to avoid extremely long regexes
        tn_list = [t for t in a_org["tn_lc"].tolist() if t]
        if not tn_list:
            continue

        matched_blocks = []
        for chunk in iter_chunks(tn_list, TN_REGEX_CHUNK):
            # escape special chars
            parts = [re.escape(t) for t in chunk]
            pattern = "(?:" + "|".join(parts) + ")"
            # contains (regex)
            mask = bc_org["__haystack"].str.contains(pattern, regex=True)
            if not mask.any():
                continue
            sub_hits = bc_org.loc[mask].copy()
            # extract concrete match and map to canonical casing
            sub_hits.loc[:, "_match_lc"] = sub_hits["__haystack"].str.extract("(" + pattern + ")", expand=False)
            sub_hits.loc[:, "tracking_number"] = sub_hits["_match_lc"].map(lut).fillna(sub_hits["_match_lc"])
            matched_blocks.append(
                sub_hits[
                    [
                        "owner_org",
                        "tracking_number",
                        "request_number",
                        "informal_requests_sum",
                        "unique_identifiers",
                        "summary_en",
                        "summary_fr",
                    ]
                ]
            )

        if matched_blocks:
            results.append(pd.concat(matched_blocks, ignore_index=True))

    if results:
        df_out = pd.concat(results, ignore_index=True).drop_duplicates()
    else:
        df_out = pd.DataFrame(
            columns=[
                "owner_org",
                "tracking_number",
                "request_number",
                "informal_requests_sum",
                "unique_identifiers",
                "summary_en",
                "summary_fr",
            ]
        )

    print(f"Matches: {len(df_out):,}")

    # ---------------- Serialize (strict JSON) + HTML ----------------
    # Ensure no NaN in string columns
    for col in [
        "owner_org",
        "tracking_number",
        "request_number",
        "unique_identifiers",
        "summary_en",
        "summary_fr",
    ]:
        if col in df_out.columns:
            df_out[col] = df_out[col].fillna("")

    payload = {
        "meta": {
            "counts": {
                "A_rows": int(len(dfA)),
                "B_rows": int(len(dfB)),
                "C_rows": int(len(dfC)),
                "BC_rows": int(len(dfBC)),
                "matches": int(len(df_out)),
            }
        },
        "rows": [
            {
                "owner_org": r["owner_org"],
                "tracking_number": r["tracking_number"],
                "c_request_number": r["request_number"],
                "informal_requests_sum": float(r.get("informal_requests_sum", 0) or 0),
                "unique_identifiers": r.get("unique_identifiers", ""),
                "summary_en": r.get("summary_en", ""),
                "summary_fr": r.get("summary_fr", ""),
            }
            for _, r in df_out.iterrows()
        ],
    }

    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, allow_nan=False, indent=2)
    print(f"Wrote {OUT_JSON}")

    INDEX_HTML = """<!DOCTYPE html>
<html lang="en" class="no-js">
<head>
  <meta charset="utf-8" />
  <title>BN ATI Report</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <!-- GCWeb (CDTS) + GC theme -->
  <link rel="stylesheet" href="https://www.canada.ca/etc/designs/canada/cdts/gcweb/v5_0_2/cdts/cdts-styles.css">
  <link rel="stylesheet" href="https://wet-boew.github.io/wet-boew/themes-dist/GCWeb/wet-boew/css/theme.min.css">
  <!-- DataTables -->
  <link rel="stylesheet" href="https://cdn.datatables.net/2.0.8/css/dataTables.dataTables.min.css">
  <style>
    .pill{display:inline-block;padding:.15rem .4rem;border-radius:999px;background:#eee;font-size:.85em}
    .muted{color:#555}.w-clip{max-width:520px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .mono{font-family:ui-monospace, Menlo, Consolas, monospace}
    td.small{max-width:560px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  </style>
</head>
<body vocab="http://schema.org/" typeof="WebPage">
  <main role="main" class="container">
    <h1 class="mrgn-tp-md">BN ATI Report</h1>
    <p class="lead">
      Sum of <span class="pill">Number of Informal Requests</span> from B by (<span class="mono">owner_org</span>, <span class="mono">request_number</span>),
      merged into C, then filtered where an A <span class="pill">tracking_number</span> appears in <span class="pill">summary_en/summary_fr</span> for the same <span class="pill">owner_org</span>.
    </p>

    <section id="stats" class="mrgn-bttm-lg well well-sm">
      <strong>Summary:</strong>
      <span id="count-a" class="pill">A: …</span>
      <span id="count-b" class="pill">B: …</span>
      <span id="count-c" class="pill">C: …</span>
      <span id="count-bc" class="pill">BC: …</span>
      <span id="count-m" class="pill">Matches: …</span>
    </section>

    <table id="report" class="wb-tables table table-striped table-hover"
           data-wb-tables='{"ordering": true, "searching": true, "pageLength": 25}'>
      <thead>
        <tr>
          <th>owner_org</th>
          <th>tracking_number</th>
          <th>request_number</th>
          <th>Informal Requests (sum)</th>
          <th>Unique Identifier(s)</th>
          <th>summary_en</th>
          <th>summary_fr</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </main>

  <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.7.1/jquery.min.js" defer></script>
  <script src="https://wet-boew.github.io/wet-boew/wet-boew/js/wet-boew.min.js" defer></script>
  <script src="https://cdn.datatables.net/2.0.8/js/dataTables.min.js" defer></script>
  <script>
    (async function () {
      const res = await fetch('./report.json', {cache: 'no-store'});
      const data = await res.json();

      // Stats
      document.getElementById('count-a').textContent  = `A: ${data.meta.counts.A_rows.toLocaleString()}`;
      document.getElementById('count-b').textContent  = `B: ${data.meta.counts.B_rows.toLocaleString()}`;
      document.getElementById('count-c').textContent  = `C: ${data.meta.counts.C_rows.toLocaleString()}`;
      document.getElementById('count-bc').textContent = `BC: ${data.meta.counts.BC_rows.toLocaleString()}`;
      document.getElementById('count-m').textContent  = `Matches: ${data.meta.counts.matches.toLocaleString()}`;

      // Rows
      const tbody = document.querySelector('#report tbody');
      for (const r of data.rows) {
        const tr = document.createElement('tr');

        const tdOwner = document.createElement('td'); tdOwner.textContent = r.owner_org || ''; tr.appendChild(tdOwner);
        const tdTN    = document.createElement('td'); tdTN.textContent    = r.tracking_number || ''; tr.appendChild(tdTN);
        const tdReq   = document.createElement('td'); tdReq.textContent   = r.c_request_number || ''; tr.appendChild(tdReq);
        const tdSum   = document.createElement('td'); tdSum.textContent   = (r.informal_requests_sum || 0).toLocaleString(); tr.appendChild(tdSum);
        const tdUID   = document.createElement('td'); tdUID.textContent   = r.unique_identifiers || ''; tr.appendChild(tdUID);
        const tdEN    = document.createElement('td'); tdEN.className='small'; tdEN.textContent = r.summary_en || ''; tr.appendChild(tdEN);
        const tdFR    = document.createElement('td'); tdFR.className='small'; tdFR.textContent = r.summary_fr || ''; tr.appendChild(tdFR);

        tbody.appendChild(tr);
      }

      // Init WET table
      window.wb && window.wb.shell && window.wb.shell.init($(document));
    })();
  </script>
</body>
</html>
"""
    OUT_HTML.write_text(INDEX_HTML, encoding="utf-8")
    print(f"Wrote {OUT_HTML}")


if __name__ == "__main__":
    main()
