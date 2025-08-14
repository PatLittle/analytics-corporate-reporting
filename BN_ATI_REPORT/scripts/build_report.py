# BN_ATI_REPORT/scripts/build_report.py
import pandas as pd
import requests, io, time, re, json
from pathlib import Path

# ---------- Config ----------
A_URL = "https://open.canada.ca/data/en/datastore/dump/299a2e26-5103-4a49-ac3a-53db9fcc06c7?format=csv"
B_URL = "https://open.canada.ca/data/en/datastore/dump/e664cf3d-6cb7-4aaa-adfa-e459c2552e3e?format=csv"
C_URL = "https://open.canada.ca/data/en/datastore/dump/19383ca2-b01a-487d-88f7-e1ffbc7d39c2?format=csv"

# BEFORE (causes BN_ATI_REPORT/BN_ATI_REPORT/docs)
# OUT_DIR = Path("BN_ATI_REPORT/docs")

# AFTER (correct for working-directory: BN_ATI_REPORT)
OUT_DIR = Path("docs")
OUT_JSON = OUT_DIR / "report.json"
OUT_HTML = OUT_DIR / "index.html"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def download_csv(url, retries=4, chunk=1024*1024):
    last_err = None
    for i in range(retries):
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                buf = io.BytesIO()
                for part in r.iter_content(chunk_size=chunk):
                    if part:
                        buf.write(part)
                buf.seek(0)
            return pd.read_csv(buf, dtype=str, keep_default_na=False).fillna("")
        except Exception as e:
            last_err = e
            print(f"[download_csv] attempt {i+1}/{retries} failed: {e}")
            time.sleep(2 * (i+1))
    raise RuntimeError(f"Failed to download {url}: {last_err}")

def agg_unique_identifiers(s: pd.Series) -> str:
    vals = [str(x).strip() for x in s if str(x).strip()]
    return "; ".join(sorted(set(vals))) if vals else ""

def main():
    # 1) B: download & aggregate
    print("Downloading B …")
    dfB = download_csv(B_URL)
    needB = ["owner_org", "Request Number", "Number of Informal Requests", "Unique Identifier"]
    for c in needB:
        if c not in dfB.columns:
            raise ValueError(f"B missing expected column {c}")
    metric = "Number of Informal Requests"
    dfB[metric] = pd.to_numeric(dfB[metric], errors="coerce").fillna(0.0)
    dfB_agg = (
        dfB.groupby(["owner_org", "Request Number"], as_index=False)
           .agg({metric: "sum", "Unique Identifier": agg_unique_identifiers})
           .rename(columns={metric: "informal_requests_sum", "Unique Identifier": "unique_identifiers"})
    )
    dfB_agg["request_number_lc"] = dfB_agg["Request Number"].str.lower()
    print(f"B rows: {len(dfB):,}  (agg: {len(dfB_agg):,})")

    # 2) C: download & merge with B
    print("Downloading C …")
    dfC = download_csv(C_URL)
    for c in ["owner_org", "request_number", "summary_en", "summary_fr"]:
        if c not in dfC.columns:
            dfC[c] = ""
    dfC["request_number_lc"] = dfC["request_number"].str.lower()
    dfBC = dfC.merge(
        dfB_agg.drop(columns=["Request Number"]),
        on=["owner_org", "request_number_lc"],
        how="left",
    )
    dfBC["informal_requests_sum"] = pd.to_numeric(dfBC["informal_requests_sum"], errors="coerce").fillna(0.0)
    dfBC["__haystack"] = (dfBC["summary_en"] + " " + dfBC["summary_fr"]).str.lower()
    print(f"C rows: {len(dfC):,}  Merged BC rows: {len(dfBC):,}")

    # 3) A: download
    print("Downloading A …")
    dfA = download_csv(A_URL)
    for c in ["owner_org", "tracking_number"]:
        if c not in dfA.columns:
            raise ValueError(f"A missing expected column {c}")
    dfA["tn_lc"] = dfA["tracking_number"].str.lower()
    print(f"A rows: {len(dfA):,}")

    # 4) Match per owner_org (regex alternation)
    results = []
    orgs = sorted(set(dfA["owner_org"]).intersection(set(dfBC["owner_org"])))
    print(f"Matching across {len(orgs)} owner_org groups …")
    for org in orgs:
        a_org = dfA.loc[dfA["owner_org"] == org, ["tn_lc", "tracking_number"]].drop_duplicates()
        if a_org.empty:
            continue
        parts = [re.escape(t) for t in a_org["tn_lc"].tolist() if t]
        if not parts:
            continue
        pattern = "(?:" + "|".join(parts) + ")"
        bc_org = dfBC.loc[dfBC["owner_org"] == org, ["owner_org","request_number","informal_requests_sum",
                                                     "unique_identifiers","summary_en","summary_fr","__haystack"]].copy()
        if bc_org.empty:
            continue
        m = bc_org["__haystack"].str.contains(pattern, regex=True)
        hits = bc_org.loc[m].copy()
        if hits.empty:
            continue
        hits.loc[:, "_match_lc"] = hits["__haystack"].str.extract("(" + pattern + ")", expand=False)
        lut = dict(zip(a_org["tn_lc"], a_org["tracking_number"]))
        hits.loc[:, "tracking_number"] = hits["_match_lc"].map(lut).fillna(hits["_match_lc"])
        results.append(
            hits[["owner_org","tracking_number","request_number","informal_requests_sum",
                  "unique_identifiers","summary_en","summary_fr"]]
        )

    if results:
        df_out = pd.concat(results, ignore_index=True).drop_duplicates()
    else:
        df_out = pd.DataFrame(columns=["owner_org","tracking_number","request_number","informal_requests_sum",
                                       "unique_identifiers","summary_en","summary_fr"])
    print(f"Matches: {len(df_out):,}")

    # 5) Write report.json + HTML
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
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Wrote", OUT_JSON)

    INDEX_HTML = """<!DOCTYPE html>
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
    td.small{max-width:420px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
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

  <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.7.1/jquery.min.js" defer></script>
  <script src="https://wet-boew.github.io/wet-boew/wet-boew/js/wet-boew.min.js" defer></script>
  <script src="https://cdn.datatables.net/2.0.8/js/dataTables.min.js" defer></script>

  <script>
    (async function () {
      const res = await fetch('./report.json', {cache: 'no-store'});
      const data = await res.json();

      document.getElementById('count-a').textContent  = `A: ${data.meta.counts.A_rows.toLocaleString()}`;
      document.getElementById('count-b').textContent  = `B: ${data.meta.counts.B_rows.toLocaleString()}`;
      document.getElementById('count-c').textContent  = `C: ${data.meta.counts.C_rows.toLocaleString()}`;
      document.getElementById('count-bc').textContent = `BC: ${data.meta.counts.BC_rows.toLocaleString()}`;
      document.getElementById('count-m').textContent  = `Matches: ${data.meta.counts.matches.toLocaleString()}`;

      const tbody = document.querySelector('#report tbody');
      for (const r of data.rows) {
        const tr = document.createElement('tr');
        const tdOwner = document.createElement('td'); tdOwner.textContent = r.owner_org; tr.appendChild(tdOwner);
        const tdTN = document.createElement('td'); tdTN.textContent = r.tracking_number; tr.appendChild(tdTN);
        const tdReq = document.createElement('td'); tdReq.textContent = r.c_request_number || ''; tr.appendChild(tdReq);
        const tdSum = document.createElement('td'); tdSum.textContent = (r.informal_requests_sum || 0).toLocaleString(); tr.appendChild(tdSum);
        const tdUID = document.createElement('td'); tdUID.textContent = r.unique_identifiers || ''; tr.appendChild(tdUID);
        const tdEN = document.createElement('td'); tdEN.className = 'small'; tdEN.textContent = r.summary_en || ''; tr.appendChild(tdEN);
        const tdFR = document.createElement('td'); tdFR.className = 'small'; tdFR.textContent = r.summary_fr || ''; tr.appendChild(tdFR);
        tbody.appendChild(tr);
      }
      window.wb && window.wb.shell && window.wb.shell.init($(document));
    })();
  </script>
</body>
</html>
"""
    OUT_HTML.write_text(INDEX_HTML, encoding="utf-8")
    print("Wrote", OUT_HTML)

if __name__ == "__main__":
    main()
