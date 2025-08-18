# BN_ATI_REPORT/scripts/build_report.py
from __future__ import annotations

import io, json, re, time
from pathlib import Path
from typing import Iterable, List

import pandas as pd
import requests

# CKAN resource dumps (CSV)
A_URL = "https://open.canada.ca/data/en/datastore/dump/299a2e26-5103-4a49-ac3a-53db9fcc06c7?format=csv"
B_URL = "https://open.canada.ca/data/en/datastore/dump/e664cf3d-6cb7-4aaa-adfa-e459c2552e3e?format=csv"
C_URL = "https://open.canada.ca/data/en/datastore/dump/19383ca2-b01a-487d-88f7-e1ffbc7d39c2?format=csv"

OUT_DIR = Path("docs")
OUT_JSON = OUT_DIR / "report.json"
OUT_HTML = OUT_DIR / "index.html"

# REQUIRED template path (always load this file)
TEMPLATE_FILE = Path("templates/index.html")

TN_REGEX_CHUNK = 400

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

def inject_script_into_html(html: str, script_tag: str) -> str:
    marker = "<!--REPORT_SCRIPT-->"
    if marker in html:
        return html.replace(marker, script_tag)
    lower = html.lower()
    idx = lower.rfind("</body>")
    if idx != -1:
        return html[:idx] + script_tag + html[idx:]
    return html + script_tag

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not TEMPLATE_FILE.exists():
        raise FileNotFoundError(
            f"Required template not found: {TEMPLATE_FILE}. "
            "Please add BN_ATI_REPORT/templates/index.html to the repo."
        )
    template_html = TEMPLATE_FILE.read_text(encoding="utf-8")

    # --- B
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

    # --- C
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

    # --- A
    print("Downloading A …")
    dfA = download_csv_df(A_URL)
    needA = ["owner_org", "tracking_number"]
    missA = [c for c in needA if c not in dfA.columns]
    if missA:
        raise ValueError(f"A is missing expected columns: {missA}")
    dfA["tn_lc"] = dfA["tracking_number"].str.lower()
    print(f"A rows: {len(dfA):,}")

    # --- match by owner_org with regex chunks
    results = []
    orgs = sorted(set(dfA["owner_org"]).intersection(set(dfBC["owner_org"])))
    print(f"Matching across {len(orgs)} owner_org groups …")
    for org in orgs:
        a_org = (dfA.loc[dfA["owner_org"] == org, ["tn_lc", "tracking_number"]]
                   .drop_duplicates().reset_index(drop=True))
        if a_org.empty: continue
        lut = dict(zip(a_org["tn_lc"], a_org["tracking_number"]))
        bc_org = dfBC.loc[dfBC["owner_org"] == org,
                          ["owner_org","request_number","informal_requests_sum",
                           "unique_identifiers","summary_en","summary_fr","__haystack"]].copy()
        if bc_org.empty: continue

        tn_list = [t for t in a_org["tn_lc"].tolist() if t]
        if not tn_list: continue

        blocks = []
        for chunk in iter_chunks(tn_list, TN_REGEX_CHUNK):
            parts = [re.escape(t) for t in chunk]
            pattern = "(?:" + "|".join(parts) + ")"
            mask = bc_org["__haystack"].str.contains(pattern, regex=True)
            if not mask.any(): continue
            sub = bc_org.loc[mask].copy()
            sub.loc[:, "_match_lc"] = sub["__haystack"].str.extract("(" + pattern + ")", expand=False)
            sub.loc[:, "tracking_number"] = sub["_match_lc"].map(lut).fillna(sub["_match_lc"])
            blocks.append(sub[["owner_org","tracking_number","request_number",
                               "informal_requests_sum","unique_identifiers",
                               "summary_en","summary_fr"]])
        if blocks:
            results.append(pd.concat(blocks, ignore_index=True))

    df_out = (pd.concat(results, ignore_index=True).drop_duplicates()
              if results else
              pd.DataFrame(columns=["owner_org","tracking_number","request_number",
                                    "informal_requests_sum","unique_identifiers",
                                    "summary_en","summary_fr"]))
    print(f"Matches: {len(df_out):,}")

    # --- JSON
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

    # --- SearchBuilder loader with presets
    loader_js = r"""
<script>
(function(){
  function addCSS(href){
    return new Promise(function(resolve,reject){
      if ([...document.styleSheets].some(s => s.href && s.href.includes(href))) return resolve();
      const l=document.createElement('link'); l.rel='stylesheet'; l.href=href;
      l.onload=resolve; l.onerror=reject; document.head.appendChild(l);
    });
  }
  function addScript(src){
    return new Promise(function(resolve,reject){
      if ([...document.scripts].some(s => s.src && s.src.includes(src))) return resolve();
      const s=document.createElement('script'); s.src=src; s.defer=true;
      s.onload=resolve; s.onerror=reject; document.head.appendChild(s);
    });
  }
  async function ensureDataTables(){
    if (!window.jQuery) await addScript("https://ajax.googleapis.com/ajax/libs/jquery/3.7.1/jquery.min.js");
    if (!jQuery.fn || !jQuery.fn.dataTable) {
      await addCSS("https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css");
      await addScript("https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js");
    }
    // SearchBuilder
    await addCSS("https://cdn.datatables.net/searchbuilder/1.6.1/css/searchBuilder.dataTables.min.css");
    await addScript("https://cdn.datatables.net/searchbuilder/1.6.1/js/dataTables.searchBuilder.min.js");
  }

  function escapeHtml(s){
    return String(s ?? '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',\"'\":'&#39;'}[m]));
  }
  function linkOwnerOrg(val){
    const raw = String(val || '');
    const slug = encodeURIComponent(raw.toLowerCase());
    const url = `https://search.open.canada.ca/briefing_titles/?owner_org=${slug}`;
    return `<a href="${url}" target="_blank" rel="noopener">${escapeHtml(raw)}</a>`;
  }
  function linkUIDs(val){
    const raw = String(val || '');
    if (!raw.trim()) return '';
    const parts = raw.split(';').map(s => s.trim()).filter(Boolean);
    const base = "https://open.canada.ca/en/search/ati/reference/";
    return parts.map(id => `<a href="${base}${encodeURIComponent(id)}" target="_blank" rel="noopener">${escapeHtml(id)}</a>`).join("<br>");
  }
  function buildDetails(row){
    return `
      <div class="dt-details">
        <h4>Full details</h4>
        <p><strong>owner_org:</strong> ${escapeHtml(row[0])}</p>
        <p><strong>tracking_number:</strong> ${escapeHtml(row[1])}</p>
        <p><strong>request_number:</strong> ${escapeHtml(row[2])}</p>
        <p><strong>Informal Requests (sum):</strong> ${Number(row[3] || 0).toLocaleString()}</p>
        <p><strong>Unique Identifier(s):</strong><br>${linkUIDs(row[4])}</p>
        <p><strong>summary_en:</strong><br>${escapeHtml(row[5])}</p>
        <p><strong>summary_fr:</strong><br>${escapeHtml(row[6])}</p>
      </div>`;
  }

  // Preset definitions (SearchBuilder.Criteria)
  const PRESET_WEAK_IDS = {
    criteria: [
      { data: 'tracking_number', condition: '!=', value: ['c'] },
      { data: 'tracking_number', condition: '!=', value: ['1'] },
      { data: 'tracking_number', condition: '!=', value: ['0'] },
      { data: 'tracking_number', condition: '!=', value: ['NA'] },
      { data: 'tracking_number', condition: '!=', value: ['na'] },
      { data: 'tracking_number', condition: '!=', value: ['-'] },
      { data: 'tracking_number', condition: '!=', value: ['REDACTED'] },
      { data: 'tracking_number', condition: '!=', value: ['[REDACTED]'] }
    ],
    logic: 'AND'
  };
  const PRESET_INFORMAL = {
    criteria: [
      { data: 'Informal Requests (sum)', condition: '>=', value: [1] }
    ],
    logic: 'AND'
  };

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

    // Stats
    const statsEl = document.getElementById('bn-ati-stats');
    if (statsEl) {
      statsEl.innerHTML =
        `<strong>Summary</strong> — ` +
        `A: ${Number(data.meta?.counts?.A_rows||0).toLocaleString()} · ` +
        `B: ${Number(data.meta?.counts?.B_rows||0).toLocaleString()} · ` +
        `C: ${Number(data.meta?.counts?.C_rows||0).toLocaleString()} · ` +
        `BC: ${Number(data.meta?.counts?.BC_rows||0).toLocaleString()} · ` +
        `Matches: ${Number(data.meta?.counts?.matches||0).toLocaleString()}`;
    }

    const table = document.getElementById('report');
    const dt = jQuery(table).DataTable({
      data: rows,
      deferRender: true,
      autoWidth: false,
      pageLength: 25,
      lengthMenu: [[10,25,50,100,-1],[10,25,50,100,"All"]],
      order: [[0, "asc"]],
      // SearchBuilder placement
      dom: 'Qlfrtip',
      searchBuilder: {
        columns: [0,1,2,3,4,5,6]
      },
      columns: [
        { // owner_org → hyperlink (display), raw for search/sort
          data: 0,
          render: function(d, type){ return type === 'display' ? linkOwnerOrg(d) : (d ?? ''); }
        },
        { data: 1 }, // tracking_number
        { data: 2 }, // request_number
        { // numeric sum
          data: 3, className: 'dt-right',
          render: function(d, type){ const n = Number(d||0); return type === 'display' ? n.toLocaleString() : n; }
        },
        { // Unique Identifier(s) → links on display
          data: 4,
          render: function(d, type){ return type === 'display' ? linkUIDs(d) : (d ?? ''); }
        },
        { data: 5, className: 'small' }, // summary_en
        { data: 6, className: 'small' }  // summary_fr
      ]
    });

    // Row expansion toggle
    jQuery('#report tbody').on('click', 'tr', function(){
      const row = dt.row(this);
      if (row.child.isShown()) { row.child.hide(); jQuery(this).removeClass('shown'); }
      else { row.child(buildDetails(row.data())).show(); jQuery(this).addClass('shown'); }
    });

    // Preset buttons
    const btnWeak = document.getElementById('preset-weak');
    const btnInformal = document.getElementById('preset-informal');
    const btnClear = document.getElementById('preset-clear');

    if (btnWeak) btnWeak.addEventListener('click', () => dt.searchBuilder.rebuild(PRESET_WEAK_IDS));
    if (btnInformal) btnInformal.addEventListener('click', () => dt.searchBuilder.rebuild(PRESET_INFORMAL));
    if (btnClear) btnClear.addEventListener('click', () => dt.searchBuilder.rebuild());
  }

  main().catch(console.error);
})();
</script>
"""
    final_html = inject_script_into_html(template_html, loader_js)
    OUT_HTML.write_text(final_html, encoding="utf-8")
    print(f"Wrote {OUT_HTML}")

if __name__ == "__main__":
    main()
