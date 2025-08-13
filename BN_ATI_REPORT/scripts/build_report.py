import json, os, math, re
from pathlib import Path
from ckanapi import RemoteCKAN
import pandas as pd

BASE = os.environ.get("CKAN_BASE", "https://open.canada.ca/data/en")
A_ID = "299a2e26-5103-4a49-ac3a-53db9fcc06c7"  # has tracking_number
B_ID = "e664cf3d-6cb7-4aaa-adfa-e459c2552e3e"  # has request_number + Number of Informal Requests
C_ID = "19383ca2-b01a-487d-88f7-e1ffbc7d39c2"  # big table we fuzzy search within (contains text fields)

OUT_DIR = Path("docs")
SCHEMA_DIR = OUT_DIR / "schemas"
SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
OUT_JSON = OUT_DIR / "report.json"

def fetch_schema(rc, rid):
    info = rc.action.datastore_info(id=rid)
    (SCHEMA_DIR / f"{rid}.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
    fields = [f["id"] for f in info["fields"] if f["id"] != "_id"]
    return fields

def fetch_all(rc, rid, fields=None, chunk=500):
    # datastore_search pagination (no SQL)
    out = []
    offset = 0
    while True:
        res = rc.action.datastore_search(id=rid, limit=chunk, offset=offset, fields=fields)
        records = res.get("records", [])
        if not records:
            break
        out.extend(records)
        offset += len(records)
        if len(records) < chunk:
            break
    return out

def normalize_str(x):
    if x is None:
        return ""
    if isinstance(x, (dict, list)):
        return json.dumps(x, ensure_ascii=False)
    return str(x)

def concat_row_values(row):
    # Concatenate ALL field values as a single lowercase string for substring test
    return " | ".join(normalize_str(v) for v in row.values()).lower()

def main():
    with RemoteCKAN(BASE) as rc:
        # Save schemas (for traceability)
        a_fields = fetch_schema(rc, A_ID)
        b_fields = fetch_schema(rc, B_ID)
        c_fields = fetch_schema(rc, C_ID)

        # Pull data
        A = fetch_all(rc, A_ID, fields=a_fields)
        B = fetch_all(rc, B_ID, fields=b_fields)
        C = fetch_all(rc, C_ID, fields=c_fields)

    # DataFrames
    dfA = pd.DataFrame(A)
    dfB = pd.DataFrame(B)
    dfC = pd.DataFrame(C)

    # Defensive typing
    for col in ("owner_org", "tracking_number"):
        if col in dfA.columns:
            dfA[col] = dfA[col].astype(str)

    if "owner_org" in dfC.columns:
        dfC["owner_org"] = dfC["owner_org"].astype(str)

    if "request_number" in dfC.columns:
        dfC["request_number"] = dfC["request_number"].astype(str)

    if not dfB.empty:
        if "owner_org" in dfB.columns:
            dfB["owner_org"] = dfB["owner_org"].astype(str)
        if "request_number" in dfB.columns:
            dfB["request_number"] = dfB["request_number"].astype(str)
        # Normalize metric column name (as given)
        metric_col = "Number of Informal Requests"
        # Some portals rename / vary spaces; handle a couple of likely variants
        if metric_col not in dfB.columns:
            candidates = [c for c in dfB.columns if c.replace("_", " ").lower() == metric_col.lower()]
            if candidates:
                metric_col = candidates[0]
        dfB[metric_col] = pd.to_numeric(dfB[metric_col], errors="coerce").fillna(0)

        # Pre-aggregate B by (owner_org, request_number)
        aggB = (
            dfB.groupby(["owner_org", "request_number"], dropna=False)[metric_col]
            .sum()
            .reset_index()
            .rename(columns={metric_col: "informal_requests_sum"})
        )
    else:
        aggB = pd.DataFrame(columns=["owner_org", "request_number", "informal_requests_sum"])

    # Pre-index C by owner_org and create concatenated searchable string
    if not dfC.empty:
        dfC["_haystack"] = dfC.apply(concat_row_values, axis=1)
    else:
        dfC["_haystack"] = ""

    # Build matches
    results = []
    if not dfA.empty and not dfC.empty:
        # Iterate A rows; restrict C by owner_org; substring match tracking_number anywhere in C row
        for _, arow in dfA.iterrows():
            owner = str(arow.get("owner_org", ""))
            tn = str(arow.get("tracking_number", "")).strip()
            if not owner or not tn:
                continue
            lower_tn = tn.lower()

            c_subset = dfC[dfC["owner_org"] == owner]
            if c_subset.empty:
                continue

            matched = c_subset[c_subset["_haystack"].str.contains(re.escape(lower_tn), na=False, regex=True)]
            if matched.empty:
                continue

            # Bring request_number forward for the next join
            for _, crow in matched.iterrows():
                req = str(crow.get("request_number", "")) if "request_number" in crow else ""
                # Lookup sum in B
                if not aggB.empty and owner and req:
                    hit = aggB[(aggB["owner_org"] == owner) & (aggB["request_number"] == req)]
                    informal_sum = float(hit["informal_requests_sum"].iloc[0]) if not hit.empty else 0.0
                else:
                    informal_sum = 0.0

                # Keep a concise snippet to help readers see where it matched
                snippet_source = crow.to_dict()
                # Provide a small sample of columns for readability
                preview_cols = [c for c in ["title_en", "title_fr", "subject_en", "subject_fr", "description_en", "description_fr"] if c in crow.index]
                preview = {k: snippet_source.get(k) for k in preview_cols}
                # Fallback if those aren't present
                if not preview:
                    preview = {k: snippet_source.get(k) for k in list(snippet_source.keys())[:6]}

                results.append({
                    "owner_org": owner,
                    "tracking_number": tn,
                    "c_request_number": req,
                    "informal_requests_sum": informal_sum,
                    "c_record_preview": preview,
                })

    # Results to JSON (plus some metadata)
    payload = {
        "meta": {
            "source": {
                "base": BASE,
                "A_resource_id": A_ID,
                "B_resource_id": B_ID,
                "C_resource_id": C_ID,
            },
            "counts": {
                "A_rows": int(len(dfA)),
                "B_rows": int(len(dfB)),
                "C_rows": int(len(dfC)),
                "matches": int(len(results)),
            },
        },
        "rows": results,
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
