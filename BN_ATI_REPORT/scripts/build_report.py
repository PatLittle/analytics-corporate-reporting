# BN_ATI_REPORT/scripts/build_report.py
"""
Build BN_ATI report:
- Download 3 CSV datasets from the Government of Canada Open Data portal
- B: aggregate "Number of Informal Requests" by (owner_org, Request Number) and collect "Unique Identifier"
- C: merge the B aggregate on (owner_org, request_number)
- A: per owner_org, find rows in C whose summary_en/summary_fr contain any A.tracking_number
- Emit:
  - docs/report.json (strict JSON, no NaN)
  - docs/index.html   (from your template if provided, else GCWeb fallback)
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

# Optional template locations (first one found wins)
TEMPLATE_PATHS = [
    Path("page_template.html"),
    Path("templates/page_template.html"),
    Path("docs/page_template.html"),
]

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


def load_template_html() -> str | None:
    for p in TEMPLATE_PATHS:
        if p.exists():
            print(f"Using page template: {p}")
            return p.read_text(encoding="utf-8")
    return None


def inject_script(html: str, script_tag: str) -> str:
    """Insert script into html. If <!--REPORT_SCRIPT--> present, replace it; else insert before </body>."""
    marker = "<!--REPORT_SCRIPT-->"
    if marker in html:
        return html.replace(marker, script_tag)
    # insert before </body>, or append if not found
    lower = html.lower()
    idx = lower.rfind("</body>")
    if idx != -1:
        return html[:idx] + script_tag + html[idx:]
    return html + script_tag


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

    # ---------------- Serialize (strict JSON) ----------------
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

    # ---------------- Build HTML from template or fallback ----------------
    template_html = load_template_html()
    if template_html is None:
        # Fallback GCWeb page
        template_html = """<!DOCTYPE html>
<html lang="en" class="no-js">
<head>
  <meta charset="utf-8" />
  <title>BN ATI Report</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link rel="stylesheet" href="https://www.canada.ca/etc/designs/canada/cdts/gcweb/v5_0_2/cdts/cdts-styles.css">
  <link rel="stylesheet" href="https://wet-boew.github.io/wet-boew/themes-dist/GCWeb/wet-boew/css/theme.min.css">
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
    <section id="stats" class="mrgn-bttm-lg well well-sm">
      <strong>Summary:</strong>
      <span id="count-a" class="pill">A: …</span>
      <span id="count-b" class="pill">B: …</span>
      <span id="count-c" class="pill">C: …</span>
      <span id="count-bc" class="pill">BC: …</span>
      <span id="count-m" class="pill">Matches: …</span>
    </section>
    <table id="report" class="wb-tables table table-striped table-hover" data-wb-tables='{"ordering": true, "searching": true, "pageLength": 25}'>
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
  <!--REPORT_SCRIPT-->
</body>
</html>"""

    # The data-loading script (works with/without WET/DataTables present)
    loader_js = r"""
<script>
(function(){
  async function load() {
    const res = await fetch('./report.json', {cache: 'no-store'});
    const data = await res.json();

    // Stats (if present)
    function setText(id, txt){
      var el = document.getElementById(id);
      if (el) el.textContent = txt;
    }
    if (data.meta && data.meta.counts){
      const c = data.meta.counts;
      setText('count-a',  `A: ${(+c.A_rows||0).toLocaleString()}`);
      setText('count-b',  `B: ${(+c.B_rows||0).toLocaleString()}`);
      setText('count-c',  `C: ${(+c.C_rows||0).toLocaleString()}`);
      setText('count-bc', `BC: ${(+c.BC_rows||0).toLocaleString()}`);
      setText('count-m',  `Matches: ${(+c.matches||0).toLocaleString()}`);
    }

    // Ensure a table exists
    var table = document.getElementById('report');
    if (!table) {
      table = document.createElement('table');
      table.id = 'report';
      table.className = 'wb-tables table table-striped table-hover';
      table.setAttribute('data-wb-tables','{"ordering": true, "searching": true, "pageLength": 25}');
      var thead = document.createElement('thead');
      thead.innerHTML = '<tr>' +
        '<th>owner_org</th><th>tracking_number</th><th>request_number</th>' +
        '<th>Informal Requests (sum)</th><th>Unique Identifier(s)</th>' +
        '<th>summary_en</th><th>summary_fr</th></tr>';
      var tbody = document.createElement('tbody');
      table.appendChild(thead); table.appendChild(tbody);
      (document.querySelector('main') || document.body).appendChild(table);
    }
    var tbody = table.querySelector('tbody') || document.createElement('tbody');
    if (!table.querySelector('tbody')) table.appendChild(tbody);

    // Populate rows
    (data.rows || []).forEach(function(r){
      var tr = document.createElement('tr');
      function td(v, cls){ var x=document.createElement('td'); if(cls) x.className=cls; x.textContent = v==null?'':String(v); return x; }
      tr.appendChild(td(r.owner_org));
      tr.appendChild(td(r.tracking_number));
      tr.appendChild(td(r.c_request_number || ''));
      tr.appendChild(td((r.informal_requests_sum||0).toLocaleString()));
      tr.appendChild(td(r.unique_identifiers || ''));
      tr.appendChild(td(r.summary_en || '', 'small'));
      tr.appendChild(td(r.summary_fr || '', 'small'));
      tbody.appendChild(tr);
    });

    // Initialize WET, if present
    if (window.wb && window.wb.shell) { window.wb.shell.init($(document)); }
  }
  load().catch(console.error);
})();
</script>
"""
    final_html = inject_script(template_html, loader_js)
    OUT_HTML.write_text(final_html, encoding="utf-8")
    print(f"Wrote {OUT_HTML}")


if __name__ == "__main__":
    main()
