#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs"
OUT_SQLITE = OUT_DIR / "data.sqlite"
OUT_HTML = OUT_DIR / "index.html"
TEMPLATE_FILE = ROOT / "templates/index.html"


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
    if not OUT_SQLITE.exists():
        raise FileNotFoundError("Missing docs/data.sqlite. Run build_db.py first")

    template_html = TEMPLATE_FILE.read_text(encoding="utf-8")

    loader_js = r"""
<script type="module">
import { createDbWorker } from "https://unpkg.com/sql.js-httpvfs@latest/dist/index.js";

(async () => {
  function addCSS(href){
    return new Promise((resolve,reject)=>{
      if ([...document.styleSheets].some(s => s.href && s.href.includes(href))) return resolve();
      const l=document.createElement('link'); l.rel='stylesheet'; l.href=href;
      l.onload=resolve; l.onerror=reject; document.head.appendChild(l);
    });
  }
  function addScript(src){
    return new Promise((resolve,reject)=>{
      if ([...document.scripts].some(s => s.src && s.src.includes(src))) return resolve();
      const s=document.createElement('script'); s.src=src; s.defer=true;
      s.onload=resolve; s.onerror=reject; document.head.appendChild(s);
    });
  }

  await addCSS("https://cdn.datatables.net/2.0.8/css/dataTables.dataTables.min.css");
  await addScript("https://ajax.googleapis.com/ajax/libs/jquery/3.7.1/jquery.min.js");
  await addScript("https://cdn.datatables.net/2.0.8/js/dataTables.min.js");
  await addScript("https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js");

  const worker = await createDbWorker([
    {
      from: "inline",
      config: { serverMode: "full", requestChunkSize: 4096, url: "./data.sqlite" }
    }
  ], "https://unpkg.com/sql.js-httpvfs@latest/dist/sqlite.worker.js");

  async function q(sql, params=[]) {
    return await worker.db.query(sql, params);
  }

  function escapeHtml(s){ return String(s ?? '').replace(/[&<>"']/g, m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }
  function linkOwnerOrg(val){
    const raw = String(val || ''); const slug = encodeURIComponent(raw.toLowerCase());
    const url = `https://search.open.canada.ca/briefing_titles/?owner_org=${slug}`;
    return `<a href="${url}" target="_blank" rel="noopener">${escapeHtml(raw)}</a>`;
  }
  function linkUIDs(val){
    const raw = String(val || ''); if (!raw.trim()) return '';
    const parts = raw.split(';').map(s=>s.trim()).filter(Boolean);
    const base = "https://open.canada.ca/en/search/ati/reference/";
    return parts.map(id => `<a href="${base}${encodeURIComponent(id)}" target="_blank" rel="noopener">${escapeHtml(id)}</a>`).join("<br>");
  }
  function linkTrackingNumber(owner_org, tracking_number){
    const org = String(owner_org || ''), tn = String(tracking_number || '');
    if (!org || !tn) return escapeHtml(tn);
    const url = `https://search.open.canada.ca/briefing_titles/record/${encodeURIComponent(org)},${encodeURIComponent(tn)}`;
    return `<a href="${url}" target="_blank" rel="noopener">${escapeHtml(tn)}</a>`;
  }
  function linkRequestNumber(owner_org, request_number){
    const org = String(owner_org || ''), rn = String(request_number || '');
    if (!org || !rn) return escapeHtml(rn);
    const filterVal = `owner_org:${org}|request_number:${rn}`;
    const url = "https://open.canada.ca/data/en/dataset/0797e893-751e-4695-8229-a5066e4fe43c/resource/19383ca2-b01a-487d-88f7-e1ffbc7d39c2?filters=" + encodeURIComponent(filterVal);
    return `<a href="${url}" target="_blank" rel="noopener">${escapeHtml(rn)}</a>`;
  }
  function buildDetails(row){
    const owner = row[0] ?? '', tn = row[1] ?? '', rn = row[2] ?? '';
    const sum = Number(row[3] || 0), uids = row[4] ?? '';
    const s_en = row[5] ?? '', s_fr = row[6] ?? '';
    return `
      <div class="dt-details">
        <h4>Full details</h4>
        <p><strong>owner_org:</strong> ${escapeHtml(owner)}</p>
        <p><strong>tracking_number:</strong> ${linkTrackingNumber(owner, tn)}</p>
        <p><strong>request_number:</strong> ${linkRequestNumber(owner, rn)}</p>
        <p><strong>Informal Requests (sum):</strong> ${sum.toLocaleString()}</p>
        <p><strong>Unique Identifier(s):</strong><br>${linkUIDs(uids)}</p>
        <p><strong>summary_en:</strong><br>${escapeHtml(s_en)}</p>
        <p><strong>summary_fr:</strong><br>${escapeHtml(s_fr)}</p>
      </div>`;
  }

  async function updateStats(){
    const rows = await q("SELECT key, value FROM meta_counts");
    const kv = Object.fromEntries(rows.map(r => [r.key, r.value]));
    const statsEl = document.getElementById('bn-ati-stats'); if (!statsEl) return;
    const A = Number(kv.A_rows||0).toLocaleString();
    const B = Number(kv.B_rows||0).toLocaleString();
    const C = Number(kv.C_rows||0).toLocaleString();
    const BC = Number(kv.BC_rows||0).toLocaleString();
    const matches = Number(kv.matches||0).toLocaleString();
    const strong = Number(kv.strong_matches||0).toLocaleString();
    const weak = Number(kv.weak_matches||0).toLocaleString();
    const linkA = `<a href="https://open.canada.ca/data/en/dataset/ee9bd7e8-90a5-45db-9287-85c8cf3589b6/resource/299a2e26-5103-4a49-ac3a-53db9fcc06c7" target="_blank" rel="noopener">Proactive Disclosure - Briefing Note Titles and Numbers</a>`;
    const linkB = `<a href="https://open.canada.ca/data/en/dataset/2916fad5-ebcc-4c86-b0f3-4f619b29f412/resource/e664cf3d-6cb7-4aaa-adfa-e459c2552e3e" target="_blank" rel="noopener">Analytics - ATI informal requests per summary</a>`;
    const linkC = `<a href="https://open.canada.ca/data/dataset/0797e893-751e-4695-8229-a5066e4fe43c/resource/19383ca2-b01a-487d-88f7-e1ffbc7d39c2" target="_blank" rel="noopener">Completed Access to Information Request Summaries dataset</a>`;
    statsEl.innerHTML = `
      <div>
        <strong>Summary</strong> —
        <br> ${linkA}: ${A}
        <br> ${linkB}: ${B}
        <br> ${linkC}: ${C}
        <br> Joined ATI Summaries and Informal Request Data (merged): ${BC}
        <br> Matches of BN Reference Number to ATI Summary Description Same Org: ${matches}
        <br> Strong Matches - Weak IDs Removed: ${strong}
        <br> Weak Matches BN Ref Numbers such as NA, 0, or '-' cause false positive matches: ${weak}
      </div>`;
  }

  await updateStats();
  const dm = document.querySelector('gcds-date-modified');
  if (dm) dm.setAttribute('date', new Date().toISOString().slice(0,10));

  const table = jQuery('#report').DataTable({
    serverSide: true,
    processing: true,
    searching: false,
    scrollX: true,
    lengthMenu: [[10,25,50,100],[10,25,50,100]],
    ajax: async (data, callback) => {
      const start = data.start || 0;
      const length = data.length || 25;
      const colMap = ["owner_org","tracking_number","request_number","informal_requests_sum","unique_identifiers"];
      const orderCol = colMap[(data.order?.[0]?.column ?? 0)] || "owner_org";
      const orderDir = (data.order?.[0]?.dir ?? "asc").toUpperCase() === "DESC" ? "DESC" : "ASC";
      const total = await q("SELECT COUNT(*) AS c FROM strong_matches");
      const recordsTotal = total[0]?.c || 0;
      const rows = await q(`SELECT owner_org, tracking_number, request_number, informal_requests_sum, unique_identifiers, summary_en, summary_fr FROM strong_matches ORDER BY ${orderCol} ${orderDir} LIMIT ? OFFSET ?`, [length, start]);
      const dataRows = rows.map(r => [r.owner_org, r.tracking_number, r.request_number, r.informal_requests_sum || 0, r.unique_identifiers || "", r.summary_en || "", r.summary_fr || ""]);
      callback({ draw: data.draw, recordsTotal, recordsFiltered: recordsTotal, data: dataRows });
    },
    columns: [
      { title: "owner_org", data: 0, render: (d,t)=> t==='display' ? linkOwnerOrg(d) : (d ?? '') },
      { title: "tracking_number", data: 1, render: (d,t,row)=> t==='display' ? linkTrackingNumber(row[0], d) : (d ?? '') },
      { title: "request_number", data: 2, render: (d,t,row)=> t==='display' ? linkRequestNumber(row[0], d) : (d ?? '') },
      { title: "Informal Requests (sum)", data: 3, className: 'dt-right', render:(d,t)=>{ const n = Number(d||0); return t==='display'? n.toLocaleString(): n; } },
      { title: "Unique Identifier(s)", data: 4, render:(d,t)=> t==='display' ? linkUIDs(d) : (d ?? '') },
      { title: "summary_en", data: 5, visible:false },
      { title: "summary_fr", data: 6, visible:false }
    ]
  });

  jQuery('#report tbody').on('click', 'tr', function(){
    const row = table.row(this);
    if (row.child.isShown()) { row.child.hide(); jQuery(this).removeClass('shown'); }
    else { row.child(buildDetails(row.data())).show(); jQuery(this).addClass('shown'); }
  });

  const weak = await q(`
    SELECT owner_org,
      SUM(CASE WHEN tracking_number='c' THEN 1 ELSE 0 END) as c,
      SUM(CASE WHEN tracking_number='1' THEN 1 ELSE 0 END) as one_,
      SUM(CASE WHEN tracking_number='0' THEN 1 ELSE 0 END) as zero_,
      SUM(CASE WHEN tracking_number='NA' THEN 1 ELSE 0 END) as NA_,
      SUM(CASE WHEN tracking_number='na' THEN 1 ELSE 0 END) as na_,
      SUM(CASE WHEN tracking_number='-' THEN 1 ELSE 0 END) as dash_,
      SUM(CASE WHEN tracking_number='REDACTED' THEN 1 ELSE 0 END) as red_,
      SUM(CASE WHEN tracking_number='[REDACTED]' THEN 1 ELSE 0 END) as bred_,
      SUM(CASE WHEN tracking_number='TBD-PM-00' THEN 1 ELSE 0 END) as tbd_
    FROM weak_matches
    GROUP BY owner_org
    ORDER BY owner_org
  `);

  const owners = weak.map(r => r.owner_org);
  const series = {
    "c": weak.map(r => r.c),
    "1": weak.map(r => r.one_),
    "0": weak.map(r => r.zero_),
    "NA": weak.map(r => r.NA_),
    "na": weak.map(r => r.na_),
    "-": weak.map(r => r.dash_),
    "REDACTED": weak.map(r => r.red_),
    "[REDACTED]": weak.map(r => r.bred_),
    "TBD-PM-00": weak.map(r => r.tbd_)
  };

  function colorPalette(n){
    const base = ["#8ecae6","#219ebc","#023047","#ffb703","#fb8500","#90be6d","#277da1","#577590","#f94144","#f3722c"];
    if (n<=base.length) return base.slice(0,n);
    const arr=[]; while(arr.length<n) arr.push(...base); return arr.slice(0,n);
  }
  const keys = Object.keys(series);
  const colors = colorPalette(keys.length);
  const datasets = keys.map((k,i)=>({ label:k, data:series[k], backgroundColor:colors[i], stack:"weak" }));

  new Chart(document.getElementById("weakChart"), {
    type: "bar",
    data: { labels: owners, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: { x: { stacked:true, ticks:{ autoSkip:true, maxRotation:45 } }, y: { stacked:true, beginAtZero:true } },
      plugins: { legend:{ position:"top", labels:{ font:{ size:16 } } }, title:{ display:true, text:"Weak BN IDs per Owner Org", font:{ size:20 } } }
    }
  });

  const tbody = document.querySelector("#weakTable tbody");
  if (tbody){
    for (let i=0; i<owners.length; i++){
      const tr = document.createElement("tr");
      const total = (series["c"][i]||0)+(series["1"][i]||0)+(series["0"][i]||0)+(series["NA"][i]||0)+(series["na"][i]||0)+(series["-"][i]||0)+(series["REDACTED"][i]||0)+(series["[REDACTED]"][i]||0)+(series["TBD-PM-00"][i]||0);
      const cells = [owners[i], series["c"][i]||0, series["1"][i]||0, series["0"][i]||0, series["NA"][i]||0, series["na"][i]||0, series["-"][i]||0, series["REDACTED"][i]||0, series["[REDACTED]"][i]||0, series["TBD-PM-00"][i]||0, total];
      for (const c of cells){ const td = document.createElement("td"); td.textContent = c; tr.appendChild(td); }
      tbody.appendChild(tr);
    }
  }
})();
</script>
"""

    final_html = inject_script_into_html(template_html, loader_js)
    final_html = final_html.replace("{{ build_date }}", date.today().isoformat())
    OUT_HTML.write_text(final_html, encoding="utf-8")
    print(f"🧾 Wrote {OUT_HTML}")


if __name__ == "__main__":
    main()
