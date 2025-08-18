# BN_ATI_REPORT/scripts/build_report.py
from __future__ import annotations

import io, json, re, time
from pathlib import Path
from typing import Iterable, List

import pandas as pd
import requests

# ---------------------------------------------------------------------
# CKAN resource dumps (CSV)
# ---------------------------------------------------------------------
A_URL = "https://open.canada.ca/data/en/datastore/dump/299a2e26-5103-4a49-ac3a-53db9fcc06c7?format=csv"
B_URL = "https://open.canada.ca/data/en/datastore/dump/e664cf3d-6cb7-4aaa-adfa-e459c2552e3e?format=csv"
C_URL = "https://open.canada.ca/data/en/datastore/dump/19383ca2-b01a-487d-88f7-e1ffbc7d39c2?format=csv"

# ---------------------------------------------------------------------
# Outputs (workflow working-directory = BN_ATI_REPORT)
# ---------------------------------------------------------------------
OUT_DIR = Path("docs")
OUT_JSON = OUT_DIR / "report.json"
OUT_HTML = OUT_DIR / "index.html"

# Optional external template (first found wins)
TEMPLATE_PATHS = [
    Path("page_template.html"),
    Path("templates/page_template.html"),
    Path("docs/page_template.html"),
]

# Chunk alternation to keep regex manageable
TN_REGEX_CHUNK = 400

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
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

def load_template_html() -> str | None:
    for p in TEMPLATE_PATHS:
        if p.exists():
            print(f"Using page template: {p}")
            return p.read_text(encoding="utf-8")
    return None

def inject_script(html: str, script_tag: str) -> str:
    marker = "<!--REPORT_SCRIPT-->"
    if marker in html:
        return html.replace(marker, script_tag)
    # insert before </body>, or append
    lower = html.lower()
    idx = lower.rfind("</body>")
    if idx != -1:
        return html[:idx] + script_tag + html[idx:]
    return html + script_tag

# ---------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ---------------- B: download & aggregate ----------------
    print("Downloading B …")
    dfB = download_csv_df(B_URL)
    needB = ["owner_org", "Request Number", "Number of Informal Requests", "Unique Identifier"]
    missB = [c for c in needB if c not in dfB.columns]
    if missB:
        raise ValueError(f"B is missing expected columns: {missB}")

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
    if "unique_identifiers" not in dfBC.columns:
        dfBC["unique_identifiers"] = ""
    dfBC["unique_identifiers"] = dfBC["unique_identifiers"].fillna("")
    dfBC["informal_requests_sum"] = pd.to_numeric(dfBC.get("informal_requests_sum", 0.0),
                                                  errors="coerce").fillna(0.0)
    dfBC["__haystack"] = (dfBC["summary_en"] + " " + dfBC["summary_fr"]).str.lower()
    print(f"C rows: {len(dfC):,}  Merged BC rows: {len(dfBC):,}")

    # ---------------- A: download ----------------
    print("Downloading A …")
    dfA = download_csv_df(A_URL)
    needA = ["owner_org", "tracking_number"]
    missA = [c for c in needA if c not in dfA.columns]
    if missA:
        raise ValueError(f"A is missing expected columns: {missA}")
    dfA["tn_lc"] = dfA["tracking_number"].str.lower()
    print(f"A rows: {len(dfA):,}")

    # ---------------- Match per owner_org (regex chunks) ----------------
    results = []
    orgs = sorted(set(dfA["owner_org"]).intersection(set(dfBC["owner_org"])))
    print(f"Matching across {len(orgs)} owner_org groups …")

    for org in orgs:
        a_org = (dfA.loc[dfA["owner_org"] == org, ["tn_lc", "tracking_number"]]
                   .drop_duplicates().reset_index(drop=True))
        if a_org.empty:
            continue

        lut = dict(zip(a_org["tn_lc"], a_org["tracking_number"]))
        bc_org = dfBC.loc[dfBC["owner_org"] == org,
                          ["owner_org","request_number","informal_requests_sum",
                           "unique_identifiers","summary_en","summary_fr","__haystack"]].copy()
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
            matched_blocks.append(sub[["owner_org","tracking_number","request_number",
                                       "informal_requests_sum","unique_identifiers",
                                       "summary_en","summary_fr"]])
        if matched_blocks:
            results.append(pd.concat(matched_blocks, ignore_index=True))

    if results:
        df_out = pd.concat(results, ignore_index=True).drop_duplicates()
    else:
        df_out = pd.DataFrame(columns=["owner_org","tracking_number","request_number",
                                       "informal_requests_sum","unique_identifiers",
                                       "summary_en","summary_fr"])

    print(f"Matches: {len(df_out):,}")

    # ---------------- Serialize JSON (strict) ----------------
    for col in ["owner_org","tracking_number","request_number","unique_identifiers","summary_en","summary_fr"]:
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

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_JSON}")

    # ---------------- Build HTML (GCDS template + DataTables) ----------------
    template_html = load_template_html()
    if template_html is None:
        # Your provided GCDS template as the default (kept verbatim), just with <!--REPORT_SCRIPT--> marker.
        template_html = """<!-- TODO: Remove all comments before deploying your code to production. -->
<!DOCTYPE html>
<html dir="ltr" lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta name="description" content="Add a description to provide a brief summary of the content." />
    <title>Basic Page Template (EN)</title>

    <!---------- GC Design System Utility ---------->
    <link rel="stylesheet" href="https://cdn.design-system.alpha.canada.ca/@cdssnc/gcds-utility@1.9.2/dist/gcds-utility.min.css" />

    <!---------- GC Design System Components ---------->
    <link rel="stylesheet" href="https://cdn.design-system.alpha.canada.ca/@cdssnc/gcds-components@0.40.0/dist/gcds/gcds.css" />
    <script type="module" src="https://cdn.design-system.alpha.canada.ca/@cdssnc/gcds-components@0.40.0/dist/gcds/gcds.esm.js"></script>

    <!-- DataTables (CSS only here; JS injected by loader if missing) -->
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css" />

    <!-- Custom styles -->
    <style>
      td.small{max-width:56ch;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
      .pill{display:inline-block;padding:.15rem .4rem;border-radius:999px;background:#eef;font-size:.85em;margin-right:.25rem}
      .table-wrap{margin-block: 1.5rem;}
      table.dataTable {width:100% !important}
    </style>
  </head>

  <body>
    <!---------- Header ---------->
    <gcds-header lang-href="#" skip-to-href="#main-content">
      <gcds-search slot="search"></gcds-search>
      <gcds-breadcrumbs slot="breadcrumb">
        <gcds-breadcrumbs-item href="#">Link</gcds-breadcrumbs-item>
        <gcds-breadcrumbs-item href="#">Link</gcds-breadcrumbs-item>
      </gcds-breadcrumbs>
    </gcds-header>

    <!---------- Main content ---------->
    <gcds-container id="main-content" main-container size="xl" centered tag="main">
      <section>
        <gcds-heading tag="h1">Basic page</gcds-heading>
        <gcds-text>
          Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
        </gcds-text>
      </section>

      <section class="table-wrap">
        <gcds-heading tag="h2">BN ATI report</gcds-heading>
        <!-- The table will be created and enhanced by DataTables -->
        <table id="report" class="display">
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
      </section>

      <gcds-date-modified>2024-08-22</gcds-date-modified>
    </gcds-container>

    <!---------- Footer ---------->
    <gcds-footer display="full" contextual-heading="Canadian Digital Service"
      contextual-links='{ "Why GC Notify": "#","Features": "#", "Activity on GC Notify": "#"}'>
    </gcds-footer>

    <!--REPORT_SCRIPT-->
  </body>
</html>"""

    # Loader: fetch report.json, populate table, ensure DataTables+jQuery are present, then initialize.
    loader_js = r"""
<script>
(function(){
  function addCSS(href){
    return new Promise(function(resolve,reject){
      if ([...document.styleSheets].some(s => s.href && s.href.includes('datatables'))) return resolve();
      var l=document.createElement('link'); l.rel='stylesheet'; l.href=href;
      l.onload=resolve; l.onerror=reject; document.head.appendChild(l);
    });
  }
  function addScript(src){
    return new Promise(function(resolve,reject){
      if ([...document.scripts].some(s => s.src && s.src.includes(src.split('/').pop()))) return resolve();
      var s=document.createElement('script'); s.src=src; s.defer=true;
      s.onload=resolve; s.onerror=reject; document.head.appendChild(s);
    });
  }
  async function ensureDataTables(){
    // jQuery + DataTables (1.13.x)
    if (!window.jQuery) await addScript("https://ajax.googleapis.com/ajax/libs/jquery/3.7.1/jquery.min.js");
    if (!jQuery.fn || !jQuery.fn.dataTable) {
      await addCSS("https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css");
      await addScript("https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js");
    }
  }

  async function main(){
    await ensureDataTables();

    const res = await fetch('./report.json', {cache: 'no-store'});
    const data = await res.json();

    const rows = (data.rows || []).map(r => ([
      r.owner_org || '',
      r.tracking_number || '',
      r.c_request_number || '',
      (r.informal_requests_sum || 0),
      r.unique_identifiers || '',
      r.summary_en || '',
      r.summary_fr || ''
    ]));

    const table = document.getElementById('report');
    const tbody = table.querySelector('tbody');
    // Fill tbody fast via DocumentFragment
    const frag = document.createDocumentFragment();
    for (const row of rows) {
      const tr = document.createElement('tr');
      for (let i=0;i<row.length;i++){
        const td = document.createElement('td');
        if (i>=5) td.className = 'small'; // summaries
        td.textContent = (row[i] == null) ? '' : String(row[i]);
        tr.appendChild(td);
      }
      frag.appendChild(tr);
    }
    tbody.innerHTML = '';
    tbody.appendChild(frag);

    // Initialize DataTables
    const dt = jQuery(table).DataTable({
      pageLength: 25,
      lengthMenu: [[10,25,50,100,-1],[10,25,50,100,"All"]],
      order: [[0, "asc"]],
      deferRender: true,
      autoWidth: false,
      layout: {
        topStart: 'search',
        topEnd: 'pageLength',
        bottomStart: 'info',
        bottomEnd: 'paging'
      },
      columnDefs: [
        { targets: [5,6], searchable: true }, // summaries searchable
        { targets: [3], className: 'dt-right' } // numeric align
      ]
    });

    // Optional stats: show counts above table using GCDS text
    const host = document.querySelector('#main-content') || document.body;
    const statsId = 'bn-ati-stats';
    if (!document.getElementById(statsId)) {
      const s = document.createElement('section');
      s.id = statsId;
      s.innerHTML = `
        <gcds-text>
          <strong>Summary</strong> —
          A: ${Number(data.meta?.counts?.A_rows||0).toLocaleString()} ·
          B: ${Number(data.meta?.counts?.B_rows||0).toLocaleString()} ·
          C: ${Number(data.meta?.counts?.C_rows||0).toLocaleString()} ·
          BC: ${Number(data.meta?.counts?.BC_rows||0).toLocaleString()} ·
          Matches: ${Number(data.meta?.counts?.matches||0).toLocaleString()}
        </gcds-text>`;
      host.insertBefore(s, host.querySelector('.table-wrap') || host.firstChild);
    }
  }

  main().catch(console.error);
})();
</script>
"""
    final_html = inject_script(template_html, loader_js)
    OUT_HTML.write_text(final_html, encoding="utf-8")
    print(f"Wrote {OUT_HTML}")

if __name__ == "__main__":
    main()
