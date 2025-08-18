# BN_ATI_REPORT/scripts/build_report.py
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
# CKAN resource dumps (CSV)
# ---------------------------------------------------------------------
A_URL = "https://open.canada.ca/data/en/datastore/dump/299a2e26-5103-4a49-ac3a-53db9fcc06c7?format=csv"
B_URL = "https://open.canada.ca/data/en/datastore/dump/e664cf3d-6cb7-4aaa-adfa-e459c2552e3e?format=csv"
C_URL = "https://open.canada.ca/data/en/datastore/dump/19383ca2-b01a-487d-88f7-e1ffbc7d39c2?format=csv"

# ---------------------------------------------------------------------
# Outputs (workflow working-directory = BN_ATI_REPORT)
# ---------------------------------------------------------------------
OUT_DIR = Path("docs")
OUT_JSON = OUT_DIR / "report.json"          # strong-only rows
OUT_WEAK = OUT_DIR / "weak_bn_id.json"      # weak-only rows
OUT_HTML = OUT_DIR / "index.html"

# REQUIRED template path (always present in repo)
TEMPLATE_FILE = Path("templates/index.html")

# Chunk alternation size to keep regex manageable
TN_REGEX_CHUNK = 400

# Weak BN ID set (normalized)
WEAK_BN_VALUES = {s.lower() for s in ["c", "1", "0", "NA", "na", "-", "REDACTED", "[REDACTED]", "TBD-PM-00"]}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def download_csv_df(url: str, retries: int = 4, chunk_size: int = 1024 * 1024) -> pd.DataFrame:
    """Stream-download a CSV and return a DataFrame with all string dtypes."""
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
            print(f"[download_csv_df] attempt {i + 1}/{retries} failed: {e}")
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


# ---------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 0) Load REQUIRED template
    if not TEMPLATE_FILE.exists():
        raise FileNotFoundError(
            f"Required template not found: {TEMPLATE_FILE}. "
            "Please add BN_ATI_REPORT/templates/index.html to the repo."
        )
    template_html = TEMPLATE_FILE.read_text(encoding="utf-8")

    # 1) B: download & aggregate
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
        .rename(
            columns={
                metric_col: "informal_requests_sum",
                "Unique Identifier": "unique_identifiers",
            }
        )
    )
    dfB_agg["request_number_lc"] = dfB_agg["Request Number"].str.lower()
    print(f"B rows: {len(dfB):,}  (agg: {len(dfB_agg):,})")

    # 2) C: download & merge
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
    dfBC["informal_requests_sum"] = pd.to_numeric(
        dfBC.get("informal_requests_sum", 0.0), errors="coerce"
    ).fillna(0.0)
    dfBC["__haystack"] = (dfBC["summary_en"] + " " + dfBC["summary_fr"]).str.lower()
    print(f"C rows: {len(dfC):,}  Merged BC rows: {len(dfBC):,}")

    # 3) A: download
    print("Downloading A …")
    dfA = download_csv_df(A_URL)
    needA = ["owner_org", "tracking_number"]
    missA = [c for c in needA if c not in dfA.columns]
    if missA:
        raise ValueError(f"A is missing expected columns: {missA}")
    dfA["tn_lc"] = dfA["tracking_number"].str.lower()
    print(f"A rows: {len(dfA):,}")

    # 4) Match per owner_org (regex chunks)
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
            sub.loc[:, "_match_lc"] = sub["__haystack"].str.extract(
                "(" + pattern + ")", expand=False
            )
            sub.loc[:, "tracking_number"] = sub["_match_lc"].map(lut).fillna(sub["_match_lc"])
            matched_blocks.append(
                sub[
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

    print(f"Matches (pre-filter): {len(df_out):,}")

    # 5) Split into weak vs strong BN IDs, then serialize both
    def is_weak(v: str) -> bool:
        s = (str(v) or "").strip().lower()
        return s in WEAK_BN_VALUES

    df_weak = df_out[df_out["tracking_number"].map(is_weak)].copy()
    df_strong = df_out[~df_out["tracking_number"].map(is_weak)].copy()

    print(f"Weak BN IDs: {len(df_weak):,}  |  Strong BN IDs: {len(df_strong):,}")

    def to_payload(df_all: pd.DataFrame, all_counts: dict) -> dict:
        # ensure strings
        for col in [
            "owner_org",
            "tracking_number",
            "request_number",
            "unique_identifiers",
            "summary_en",
            "summary_fr",
        ]:
            if col in df_all.columns:
                df_all[col] = df_all[col].fillna("")
        rows = [
            {
                "owner_org": r["owner_org"],
                "tracking_number": r["tracking_number"],
                "c_request_number": r["request_number"],
                "informal_requests_sum": float(r.get("informal_requests_sum", 0) or 0),
                "unique_identifiers": r.get("unique_identifiers", ""),
                "summary_en": r.get("summary_en", ""),
                "summary_fr": r.get("summary_fr", ""),
            }
            for _, r in df_all.iterrows()
        ]
        return {"meta": {"counts": all_counts}, "rows": rows}

    counts_common = {
        "A_rows": int(len(dfA)),
        "B_rows": int(len(dfB)),
        "C_rows": int(len(dfC)),
        "BC_rows": int(len(dfBC)),
        "matches": int(len(df_out)),
        "weak_matches": int(len(df_weak)),
        "strong_matches": int(len(df_strong)),
    }

    OUT_WEAK.write_text(
        json.dumps(to_payload(df_weak, counts_common), ensure_ascii=False, allow_nan=False, indent=2),
        encoding="utf-8",
    )
    print(f"Wrote {OUT_WEAK}")

    OUT_JSON.write_text(
        json.dumps(to_payload(df_strong, counts_common), ensure_ascii=False, allow_nan=False, indent=2),
        encoding="utf-8",
    )
    print(f"Wrote {OUT_JSON}")

    # 6) Inject ONLY the loader script into the template, then write docs/index.html
    #    - IndexedDB-first; only fetch if cache missing.
    #    - No weak-ID preset (removed).
    loader_js = r"""
<script>
(function(){
  // ---------- tiny helpers for dynamic assets ----------
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

  // ---------- ensure DataTables v2 + SearchBuilder 1.8.3 ----------
  async function ensureDataTables2(){
    if (!window.jQuery) await addScript("https://ajax.googleapis.com/ajax/libs/jquery/3.7.1/jquery.min.js");
    await addCSS("https://cdn.datatables.net/2.0.8/css/dataTables.dataTables.min.css");
    await addScript("https://cdn.datatables.net/2.0.8/js/dataTables.min.js");
    await addCSS("https://cdn.datatables.net/searchbuilder/1.8.3/css/searchBuilder.dataTables.min.css");
    await addScript("https://cdn.datatables.net/searchbuilder/1.8.3/js/dataTables.searchBuilder.min.js");
  }

  // ---------- IndexedDB (simple wrapper) ----------
  const DB_NAME = "bn_ati_cache";
  const STORE = "reports";
  const KEY = "report_v1"; // bump if JSON schema changes

  function idbOpen(){
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, 1);
      req.onupgradeneeded = (e) => {
        const db = e.target.result;
        if (!db.objectStoreNames.contains(STORE)) db.createObjectStore(STORE);
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }
  async function idbGet(key){
    try{
      const db = await idbOpen();
      return await new Promise((resolve, reject) => {
        const tx = db.transaction(STORE, "readonly");
        const store = tx.objectStore(STORE);
        const req = store.get(key);
        req.onsuccess = () => resolve(req.result || null);
        req.onerror = () => reject(req.error);
      });
    }catch(e){ console.warn("idbGet failed", e); return null; }
  }
  async function idbPut(key, value){
    try{
      const db = await idbOpen();
      return await new Promise((resolve, reject) => {
        const tx = db.transaction(STORE, "readwrite");
        const store = tx.objectStore(STORE);
        const req = store.put(value, key);
        req.onsuccess = () => resolve(true);
        req.onerror = () => reject(req.error);
      });
    }catch(e){ console.warn("idbPut failed", e); return false; }
  }

  // ---------- utilities ----------
  function escapeHtml(s){
    return String(s ?? '').replace(/[&<>"']/g, m => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[m]));
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
    // row = [owner_org, tracking_number, request_number, sum, unique_ids, summary_en, summary_fr]
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
  function toRows(json){
    return (json?.rows || []).map(r => ([
      r.owner_org || '',
      r.tracking_number || '',
      r.c_request_number || '',
      (r.informal_requests_sum || 0),
      r.unique_identifiers || '',
      r.summary_en || '',
      r.summary_fr || ''
    ]));
  }
  function updateStats(data){
    const statsEl = document.getElementById('bn-ati-stats');
    if (!statsEl) return;
    statsEl.innerHTML =
      `<strong>Summary</strong> — ` +
      `A: ${Number(data.meta?.counts?.A_rows||0).toLocaleString()} · ` +
      `B: ${Number(data.meta?.counts?.B_rows||0).toLocaleString()} · ` +
      `C: ${Number(data.meta?.counts?.C_rows||0).toLocaleString()} · ` +
      `BC: ${Number(data.meta?.counts?.BC_rows||0).toLocaleString()} · ` +
      `Matches: ${Number(data.meta?.counts?.matches||0).toLocaleString()} · ` +
      `Strong: ${Number(data.meta?.counts?.strong_matches||0).toLocaleString()} · ` +
      `Weak: ${Number(data.meta?.counts?.weak_matches||0).toLocaleString()}`;
  }

  // Only Records Informally Requested preset
  const PRESET_INFORMAL = {
    criteria: [{ data: 'Informal Requests (sum)', condition: '>=', value: [1] }],
    logic: 'AND'
  };

  async function main(){
    await ensureDataTables2();

    const tableEl = document.getElementById('report');

    // 1) IndexedDB-first: if cached exists, use it; otherwise fetch from web once and cache it.
    let data = await idbGet(KEY);
    if (!data){
      try{
        const res = await fetch('./report.json', { cache: 'no-store' });
        data = await res.json();
        await idbPut(KEY, data);
      }catch(e){
        console.error("Failed to fetch report.json and no cache available.", e);
        return;
      }
    }

    // Initialize table from cached (or freshly fetched) data
    updateStats(data);
    const rows = toRows(data);
    const dt = jQuery(tableEl).DataTable({
      data: rows,
      deferRender: true,
      autoWidth: false,
      pageLength: 25,
      lengthMenu: [[10,25,50,100,-1],[10,25,50,100,"All"]],
      order: [[0, "asc"]],
      dom: 'Qlfrtip',
      searchBuilder: { columns: [0,1,2,3,4,5,6] },
      columns: [
        { data: 0, render: (d,t)=> t==='display' ? linkOwnerOrg(d) : (d ?? '') },
        { data: 1 },
        { data: 2 },
        { data: 3, className:'dt-right',
          render:(d,t)=>{const n=Number(d||0); return t==='display'? n.toLocaleString(): n;} },
        { data: 4, render:(d,t)=> t==='display' ? linkUIDs(d) : (d ?? '') },
        { data: 5, className:'small' },
        { data: 6, className:'small' }
      ]
    });

    // Row expansion toggle
    jQuery('#report tbody').on('click', 'tr', function(){
      const row = dt.row(this);
      if (row.child.isShown()) { row.child.hide(); jQuery(this).removeClass('shown'); }
      else { row.child(buildDetails(row.data())).show(); jQuery(this).addClass('shown'); }
    });

    // Preset buttons (no weak preset anymore)
    const btnInformal = document.getElementById('preset-informal');
    const btnClear = document.getElementById('preset-clear');
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
