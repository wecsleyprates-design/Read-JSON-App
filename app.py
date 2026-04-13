import json
import re
import math
from collections import Counter
from io import StringIO

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Data Ingestion & Analysis",
    page_icon="📊",
    layout="wide",
)

st.markdown(
    """
    <style>
        .block-container { padding-top: 1.5rem; }
        .score-green  { color: #16a34a; font-size: 2rem; font-weight: 700; }
        .score-yellow { color: #ca8a04; font-size: 2rem; font-weight: 700; }
        .score-red    { color: #dc2626; font-size: 2rem; font-weight: 700; }
        .metric-label { font-size: 0.78rem; color: #6b7280; font-weight: 600; margin-bottom: 0.15rem; }
        .metric-value { font-size: 2rem; font-weight: 700; }
        .metric-sub   { font-size: 0.7rem; color: #9ca3af; margin-top: 0.2rem; }
        .badge-green  { background:#dcfce7; color:#166534; border-radius:4px; padding:2px 8px; font-size:0.72rem; font-weight:600; }
        .badge-yellow { background:#fef9c3; color:#854d0e; border-radius:4px; padding:2px 8px; font-size:0.72rem; font-weight:600; }
        .badge-red    { background:#fee2e2; color:#991b1b; border-radius:4px; padding:2px 8px; font-size:0.72rem; font-weight:600; }
        .info-box { background:#f0f9ff; border-left:4px solid #38bdf8; padding:0.75rem 1rem; border-radius:4px; font-size:0.82rem; color:#0c4a6e; margin-bottom:0.75rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


def info(text: str):
    st.markdown(f'<div class="info-box">ℹ️ {text}</div>', unsafe_allow_html=True)


def flatten_object(obj: dict, prefix: str = "") -> dict:
    result = {}
    for k, v in obj.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            result.update(flatten_object(v, key))
        else:
            result[key] = v
    return result


def strip_envelope(flat: dict) -> dict:
    data_keys  = [k for k in flat if k.startswith("data.")]
    other_keys = [k for k in flat if not k.startswith("data.")]
    if not data_keys:
        return flat
    result = {k: flat[k] for k in other_keys}
    for k in data_keys:
        result[k[len("data."):]] = flat[k]
    return result


def detect_type(values: list) -> tuple[str, str]:
    if not values:
        return "string", "str"
    all_booleans = all(isinstance(v, bool) or v in ("true", "false") for v in values)
    if all_booleans:
        return "boolean", "bool"
    all_numbers = all(
        (isinstance(v, (int, float)) and not isinstance(v, bool)) or (
            not isinstance(v, bool) and str(v).replace(".", "", 1).lstrip("-").isdigit()
        )
        for v in values
    )
    if all_numbers:
        return "number", "float"
    return "string", "str"


def semantic_group(col_name: str, col_type: str) -> str:
    lower = col_name.lower()
    if any(k in lower for k in ("id", "uuid", "hash", "token")):
        return "Identifiers"
    if any(k in lower for k in ("date", "time", "created", "updated")):
        return "Date & Time"
    if any(k in lower for k in ("address", "city", "state", "zip", "postal", "country", "region", "lat", "lon")):
        return "Location"
    if col_type == "number":
        return "Metrics & Numbers"
    if col_type == "boolean":
        return "Flags & Booleans"
    return "Text & Categories"


def sanitize_python_name(name: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if safe and safe[0].isdigit():
        safe = f"field_{safe}"
    return safe


def safe_val(val):
    if val is None:
        return ""
    if isinstance(val, float) and math.isnan(val):
        return ""
    if isinstance(val, (list, dict)):
        return str(val)
    return val


def profile_data(flat_rows: list[dict]) -> dict:
    if not flat_rows:
        return {"totalRows": 0, "columns": [], "qualityScore": 0,
                "completeness": 0, "consistency": 0, "pydanticSchema": ""}

    all_keys = list(dict.fromkeys(k for row in flat_rows for k in row))
    total_cells   = len(flat_rows) * len(all_keys)
    missing_cells = 0
    consistent_cells = 0

    column_profiles = []
    for col in all_keys:
        values    = [row.get(col) for row in flat_rows]
        non_null  = [v for v in values if v is not None and v != ""]
        null_count   = len(values) - len(non_null)
        unique_count = len(set(str(v) for v in non_null))
        missing_cells += null_count

        col_type, py_type = detect_type(non_null)
        sg = semantic_group(col, col_type)
        fill_pct = (len(non_null) / len(values) * 100) if values else 0

        for v in non_null:
            if col_type == "boolean" and (isinstance(v, bool) or v in ("true", "false")):
                consistent_cells += 1
            elif col_type == "number" and (
                (isinstance(v, (int, float)) and not isinstance(v, bool)) or
                (not isinstance(v, bool) and str(v).replace(".", "", 1).lstrip("-").isdigit())
            ):
                consistent_cells += 1
            elif col_type == "string":
                consistent_cells += 1

        column_profiles.append({
            "name": col, "type": col_type, "pyType": py_type,
            "semanticGroup": sg, "nullCount": null_count,
            "uniqueCount": unique_count, "fillPct": round(fill_pct, 1),
        })

    completeness = ((total_cells - missing_cells) / total_cells * 100) if total_cells else 0
    non_missing  = total_cells - missing_cells
    consistency  = (consistent_cells / non_missing * 100) if non_missing else 0
    quality_score = (completeness + consistency) / 2

    schema_lines = [
        "from pydantic import BaseModel, Field",
        "from typing import Optional, Any", "",
        "class InferredSchema(BaseModel):",
    ]
    for col in column_profiles:
        safe = sanitize_python_name(col["name"])
        if safe != col["name"]:
            schema_lines.append(f'    {safe}: Optional[{col["pyType"]}] = Field(alias="{col["name"]}")')
        else:
            schema_lines.append(f'    {safe}: Optional[{col["pyType"]}]')

    return {
        "totalRows":    len(flat_rows),
        "columns":      column_profiles,
        "qualityScore": round(quality_score, 2),
        "completeness": round(completeness, 2),
        "consistency":  round(consistency, 2),
        "pydanticSchema": "\n".join(schema_lines),
    }


def _process_json(raw_text: str, source_name: str):
    raw = json.loads(raw_text)
    if not isinstance(raw, list):
        raw = [raw]
    flat_rows = [
        strip_envelope(flatten_object(item)) if isinstance(item, dict) else {"value": item}
        for item in raw
    ]
    summary = profile_data(flat_rows)
    df      = pd.DataFrame(flat_rows)

    st.session_state.flat_rows = flat_rows
    st.session_state.summary   = summary
    st.session_state.df        = df

    existing_names = [h["name"] for h in st.session_state.upload_history]
    if source_name not in existing_names:
        st.session_state.upload_history.insert(0, {
            "name": source_name, "flat_rows": flat_rows,
            "summary": summary, "df": df,
        })
        st.session_state.upload_history = st.session_state.upload_history[:5]

    return len(flat_rows), len(summary["columns"])


for key, default in [
    ("flat_rows", []),
    ("summary", None),
    ("df", None),
    ("upload_history", []),
    ("pinned_fields", set()),
]:
    if key not in st.session_state:
        st.session_state[key] = default


col_logo, col_export = st.columns([8, 2])
with col_logo:
    st.markdown("## 📊 Data Ingestion & Analysis")
with col_export:
    if st.session_state.df is not None:
        export_json = st.session_state.df.to_json(orient="records", indent=2)
        st.download_button(
            label="⬇ Export JSON",
            data=export_json,
            file_name="export.json",
            mime="application/json",
            use_container_width=True,
        )

st.divider()

with st.container(border=True):
    st.markdown("**Upload Dataset**")
    input_tab_file, input_tab_paste = st.tabs(["📁 Upload File", "📋 Paste JSON"])

    with input_tab_file:
        uploaded_file = st.file_uploader(
            "Choose a JSON file", type=["json"], label_visibility="collapsed"
        )
        if uploaded_file is not None:
            try:
                nrows, ncols = _process_json(uploaded_file.read().decode("utf-8"), uploaded_file.name)
                st.success(f"✅ File processed — {nrows:,} rows, {ncols:,} fields.")
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON: {e}")
            except Exception as e:
                st.error(f"Error processing file: {e}")

    with input_tab_paste:
        st.caption("Paste a JSON object `{}` or array `[]` directly into the box below.")
        pasted = st.text_area(
            "paste", height=200,
            placeholder='[{"field": "value", ...}, ...]\nor\n{"field": "value", ...}',
            label_visibility="collapsed",
        )
        paste_name = st.text_input(
            "name", placeholder="Dataset name (optional, e.g. my_data)",
            label_visibility="collapsed",
        )
        if st.button("▶ Process Pasted JSON", use_container_width=True):
            if not pasted.strip():
                st.warning("Please paste some JSON first.")
            else:
                try:
                    name = (paste_name.strip() or "pasted_json")
                    if not name.endswith(".json"):
                        name += ".json"
                    nrows, ncols = _process_json(pasted, name)
                    st.success(f"✅ JSON processed — {nrows:,} rows, {ncols:,} fields.")
                except json.JSONDecodeError as e:
                    st.error(f"Invalid JSON: {e}")
                except Exception as e:
                    st.error(f"Error processing JSON: {e}")

if len(st.session_state.upload_history) > 1:
    with st.expander("📂 Upload History — switch between previously loaded files"):
        info(
            "Upload History lets you switch between the last 5 files you loaded "
            "without having to re-upload them. Useful when comparing two different "
            "API responses side by side."
        )
        for i, h in enumerate(st.session_state.upload_history):
            c1, c2, c3 = st.columns([6, 1, 1])
            with c1:
                st.markdown(
                    f"**{h['name']}** — {h['summary']['totalRows']:,} rows, "
                    f"{len(h['summary']['columns']):,} fields"
                )
            with c2:
                if st.button("Load", key=f"history_load_{i}", use_container_width=True):
                    st.session_state.flat_rows = h["flat_rows"]
                    st.session_state.summary   = h["summary"]
                    st.session_state.df        = h["df"]
                    st.rerun()
            with c3:
                if st.button("🗑", key=f"history_del_{i}", use_container_width=True, help="Remove from history"):
                    st.session_state.upload_history.pop(i)
                    st.rerun()

if len(st.session_state.upload_history) >= 2:
    with st.expander("🔍 Schema Consistency Checker — compare field lists across uploads"):
        info(
            "Schema Consistency Checker compares the field names between two uploads. "
            "It highlights fields that were added, removed, or changed type between "
            "two API responses. Useful for detecting when an upstream API has changed "
            "its structure, which could silently break your Redshift pipeline."
        )
        names = [h["name"] for h in st.session_state.upload_history]
        ca, cb = st.columns(2)
        with ca:
            sel_a = st.selectbox("File A", names, key="cmp_a")
        with cb:
            remaining = [n for n in names if n != sel_a]
            sel_b = st.selectbox("File B", remaining, key="cmp_b")

        ha = next(h for h in st.session_state.upload_history if h["name"] == sel_a)
        hb = next(h for h in st.session_state.upload_history if h["name"] == sel_b)

        fields_a = {c["name"]: c["type"] for c in ha["summary"]["columns"]}
        fields_b = {c["name"]: c["type"] for c in hb["summary"]["columns"]}

        added   = {k: fields_b[k] for k in fields_b if k not in fields_a}
        removed = {k: fields_a[k] for k in fields_a if k not in fields_b}
        changed = {k: (fields_a[k], fields_b[k]) for k in fields_a
                   if k in fields_b and fields_a[k] != fields_b[k]}

        cc1, cc2, cc3 = st.columns(3)
        with cc1:
            st.metric("Fields added", len(added))
        with cc2:
            st.metric("Fields removed", len(removed))
        with cc3:
            st.metric("Type changes", len(changed))

        if added:
            st.markdown("**🟢 Added fields** (present in B, missing in A)")
            st.dataframe(pd.DataFrame({"Field": list(added.keys()), "Type in B": list(added.values())}),
                         use_container_width=True, hide_index=True)
        if removed:
            st.markdown("**🔴 Removed fields** (present in A, missing in B)")
            st.dataframe(pd.DataFrame({"Field": list(removed.keys()), "Type in A": list(removed.values())}),
                         use_container_width=True, hide_index=True)
        if changed:
            st.markdown("**🟡 Type changes**")
            st.dataframe(pd.DataFrame({
                "Field": list(changed.keys()),
                "Type in A": [v[0] for v in changed.values()],
                "Type in B": [v[1] for v in changed.values()],
            }), use_container_width=True, hide_index=True)
        if not added and not removed and not changed:
            st.success("✅ Schemas are identical.")

if st.session_state.summary:
    summary   = st.session_state.summary
    flat_rows = st.session_state.flat_rows
    df        = st.session_state.df

    st.markdown("---")

    m1, m2, m3 = st.columns(3)
    with m1:
        with st.container(border=True):
            st.markdown('<div class="metric-label">Total Rows</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="metric-value">{summary["totalRows"]:,}</div>', unsafe_allow_html=True)
    with m2:
        with st.container(border=True):
            st.markdown('<div class="metric-label">Total Columns</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="metric-value">{len(summary["columns"]):,}</div>', unsafe_allow_html=True)
    with m3:
        with st.container(border=True):
            score = summary["qualityScore"]
            score_class = "score-green" if score >= 90 else ("score-yellow" if score >= 70 else "score-red")
            st.markdown('<div class="metric-label">🛡 Data Quality Score</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="{score_class}">{score}%</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="metric-sub">Completeness: {summary["completeness"]}% | '
                f'Consistency: {summary["consistency"]}%</div>',
                unsafe_allow_html=True,
            )

    st.markdown("---")

    tab_data, tab_freq, tab_compare = st.tabs([
        "📋 Data Table",
        "📊 Value Frequency",
        "🔀 Compare Records",
    ])

    with tab_data:
        info(
            "The Data Table shows every field as a row, with each record as a column. "
            "Use the search box to filter fields by name, and the group dropdown to "
            "focus on a specific semantic category (e.g. Location, Identifiers). "
            "Null Count, Unique Count and Fill % give a quick data quality view per field. "
            "Fill % colour: 🟢 ≥90%, 🟡 50–89%, 🔴 <50%. "
            "Use the page number to navigate (50 fields per page)."
        )

        c_search, c_group, c_page = st.columns([3, 4, 3])
        with c_search:
            search_term = st.text_input(
                "search", placeholder="🔍 Search field names…", label_visibility="collapsed"
            )
        all_groups = ["All"] + sorted(set(c["semanticGroup"] for c in summary["columns"]))
        with c_group:
            selected_group = st.selectbox("group", all_groups, label_visibility="collapsed")
        PAGE_SIZE = 50
        with c_page:
            page_num = st.number_input("Page", min_value=1, value=1, step=1, label_visibility="collapsed")

        if selected_group == "All":
            visible_col_profiles = [c for c in summary["columns"] if c["name"] in df.columns]
        else:
            visible_col_profiles = [
                c for c in summary["columns"]
                if c["semanticGroup"] == selected_group and c["name"] in df.columns
            ]
        if search_term:
            visible_col_profiles = [
                c for c in visible_col_profiles if search_term.lower() in c["name"].lower()
            ]

        visible_col_names = [c["name"] for c in visible_col_profiles]
        total_fields   = len(visible_col_names)
        total_pages    = max(1, math.ceil(total_fields / PAGE_SIZE))
        page_num       = min(page_num, total_pages)
        start_idx      = (page_num - 1) * PAGE_SIZE
        paged_profiles = visible_col_profiles[start_idx: start_idx + PAGE_SIZE]
        paged_names    = [c["name"] for c in paged_profiles]

        display_df = df[paged_names] if paged_names else df
        records    = display_df.reset_index(drop=True)

        type_map    = {c["name"]: c["type"]          for c in paged_profiles}
        py_type_map = {c["name"]: c["pyType"]        for c in paged_profiles}
        sg_map      = {c["name"]: c["semanticGroup"] for c in paged_profiles}
        fill_map    = {c["name"]: c["fillPct"]       for c in paged_profiles}
        null_map    = {c["name"]: c["nullCount"]     for c in paged_profiles}
        unique_map  = {c["name"]: c["uniqueCount"]   for c in paged_profiles}

        vertical_rows = []
        for col_name in paged_names:
            row = {
                "Field Name":     col_name,
                "Type":           type_map.get(col_name, ""),
                "Python Type":    py_type_map.get(col_name, ""),
                "Semantic Group": sg_map.get(col_name, ""),
                "Null Count":     null_map.get(col_name, 0),
                "Unique Count":   unique_map.get(col_name, 0),
                "Fill %":         fill_map.get(col_name, 0.0),
            }
            for i in range(len(records)):
                row[f"Value {i + 1}"] = safe_val(records.iloc[i].get(col_name))
            vertical_rows.append(row)

        vertical_df = pd.DataFrame(vertical_rows)
        display_vdf = vertical_df.copy()
        display_vdf["Fill %"] = display_vdf["Fill %"].apply(
            lambda v: f"🟢 {v:.1f}%" if v >= 90 else (f"🟡 {v:.1f}%" if v >= 50 else f"🔴 {v:.1f}%")
        )
        st.dataframe(display_vdf, use_container_width=True, height=520)
        st.caption(
            f"Page {page_num} of {total_pages} — "
            f"showing fields {start_idx + 1}–{min(start_idx + PAGE_SIZE, total_fields)} "
            f"of {total_fields:,} | {len(records):,} records"
        )

        st.markdown("**⬇ Export current view**")
        col_csv, col_json2 = st.columns(2)
        with col_csv:
            st.download_button(
                label="Download as CSV",
                data=vertical_df.to_csv(index=False),
                file_name="data_table_export.csv",
                mime="text/csv",
                use_container_width=True,
                help="Download the current filtered/paged view as a CSV file.",
            )
        with col_json2:
            st.download_button(
                label="Download as JSON",
                data=df.to_json(orient="records", indent=2),
                file_name="full_export.json",
                mime="application/json",
                use_container_width=True,
                help="Download the full dataset as a JSON file.",
            )

    with tab_freq:
        info(
            "The Value Frequency panel shows the distribution of values for any field "
            "you select. It answers: 'what values does this field actually take across "
            "all records, and how often?' Null/empty values are counted separately so "
            "you can see exactly how much missing data exists."
        )

        col_names = [c["name"] for c in summary["columns"]]
        sel_col = st.selectbox("Select a field to analyse", col_names, key="freq_col")

        values_raw = [row.get(sel_col) for row in flat_rows]
        null_count = sum(1 for v in values_raw if v is None or v == "" or (isinstance(v, float) and math.isnan(v)))
        non_null   = [v for v in values_raw if v is not None and v != "" and not (isinstance(v, float) and math.isnan(v))]

        f1, f2, f3 = st.columns(3)
        f1.metric("Total records", len(values_raw))
        f2.metric("Non-null values", len(non_null))
        f3.metric("Null / empty", null_count)

        if non_null:
            counts = Counter(str(v) for v in non_null)
            top_n  = st.slider("Show top N values", 5, 50, 20, key="freq_slider")
            top    = counts.most_common(top_n)

            freq_df = pd.DataFrame(top, columns=["Value", "Count"])
            freq_df["% of non-null"] = (freq_df["Count"] / len(non_null) * 100).round(1)
            freq_df["% of total"]    = (freq_df["Count"] / len(values_raw) * 100).round(1)

            st.dataframe(freq_df, use_container_width=True, hide_index=True, height=420)
            st.download_button(
                label="⬇ Export frequency table as CSV",
                data=freq_df.to_csv(index=False),
                file_name=f"frequency_{sel_col.replace('.', '_')}.csv",
                mime="text/csv",
            )
        else:
            st.warning("All values for this field are null or empty.")

    with tab_compare:
        info(
            "Compare Records picks two records and shows a full analysis of their differences. "
            "Summary metrics tell you at a glance how similar the records are. "
            "Differences are broken down by semantic group so you can see which areas "
            "(e.g. Location, Identifiers, Metrics) diverge the most. "
            "Ideal for debugging — e.g. why did one business pass KYB and another fail?"
        )

        n_records = len(flat_rows)
        if n_records < 2:
            st.warning("Need at least 2 records to compare. Upload a file with multiple records.")
        else:
            cr1, cr2 = st.columns(2)
            with cr1:
                rec_a = st.number_input("Record A", min_value=1, max_value=n_records, value=1, key="cmp_rec_a")
            with cr2:
                rec_b = st.number_input("Record B", min_value=1, max_value=n_records, value=2, key="cmp_rec_b")

            row_a = flat_rows[rec_a - 1]
            row_b = flat_rows[rec_b - 1]
            all_f = list(dict.fromkeys(list(row_a.keys()) + list(row_b.keys())))

            field_profile = {c["name"]: c for c in summary["columns"]}
            diff_rows, same_rows = [], []
            for f in all_f:
                va = safe_val(row_a.get(f))
                vb = safe_val(row_b.get(f))
                differs = str(va) != str(vb)
                sg = field_profile.get(f, {}).get("semanticGroup", "Other")
                entry = {"Field": f, "Semantic Group": sg,
                         f"Record {rec_a}": va, f"Record {rec_b}": vb}
                (diff_rows if differs else same_rows).append(entry)

            total_fields  = len(all_f)
            diff_count    = len(diff_rows)
            same_count    = len(same_rows)
            similarity    = round((same_count / total_fields * 100) if total_fields else 0, 1)

            st.markdown("#### Summary")
            km1, km2, km3, km4 = st.columns(4)
            km1.metric("Total Fields", total_fields)
            km2.metric("Fields Different", diff_count,
                       delta=f"-{diff_count}" if diff_count else None, delta_color="inverse")
            km3.metric("Fields Identical", same_count)
            km4.metric("Similarity", f"{similarity}%",
                       delta=f"{'🟢' if similarity >= 80 else '🟡' if similarity >= 50 else '🔴'}")

            st.markdown("---")

            if diff_rows:
                st.markdown("#### Differences by Semantic Group")
                group_counts: dict[str, int] = {}
                for r in diff_rows:
                    group_counts[r["Semantic Group"]] = group_counts.get(r["Semantic Group"], 0) + 1

                group_cols = st.columns(min(len(group_counts), 4))
                for idx, (grp, cnt) in enumerate(sorted(group_counts.items(), key=lambda x: -x[1])):
                    group_cols[idx % len(group_cols)].metric(grp, f"{cnt} diff{'s' if cnt > 1 else ''}")

                st.markdown("---")
                st.markdown(f"#### ⚡ {diff_count} Field(s) That Differ")

                all_diff_groups = ["All"] + sorted(group_counts.keys())
                sel_diff_group = st.selectbox("Filter by group", all_diff_groups, key="cmp_grp_filter")

                filtered_diffs = diff_rows if sel_diff_group == "All" \
                    else [r for r in diff_rows if r["Semantic Group"] == sel_diff_group]

                cmp_df = pd.DataFrame(filtered_diffs)
                st.dataframe(cmp_df, use_container_width=True, hide_index=True, height=420)

                st.download_button(
                    label="⬇ Export differences as CSV",
                    data=cmp_df.to_csv(index=False),
                    file_name=f"diff_record{rec_a}_vs_record{rec_b}.csv",
                    mime="text/csv",
                )

                if same_rows:
                    with st.expander(f"✅ View {same_count} identical field(s)"):
                        st.dataframe(pd.DataFrame(same_rows), use_container_width=True,
                                     hide_index=True, height=300)
            else:
                st.success("✅ Both records are identical across all fields.")

else:
    st.info("Upload a JSON file above to get started.")
