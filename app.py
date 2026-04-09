import json
import re

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
    </style>
    """,
    unsafe_allow_html=True,
)


def flatten_object(obj: dict, prefix: str = "") -> dict:
    result = {}
    for k, v in obj.items():
        key = f"{prefix}_{k}" if prefix else k
        if isinstance(v, dict):
            result.update(flatten_object(v, key))
        else:
            result[key] = v
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


def profile_data(flat_rows: list[dict]) -> dict:
    if not flat_rows:
        return {"totalRows": 0, "columns": [], "qualityScore": 0, "completeness": 0, "consistency": 0, "pydanticSchema": ""}

    all_keys = list(dict.fromkeys(k for row in flat_rows for k in row))
    total_cells = len(flat_rows) * len(all_keys)
    missing_cells = 0
    consistent_cells = 0

    column_profiles = []
    for col in all_keys:
        values = [row.get(col) for row in flat_rows]
        non_null = [v for v in values if v is not None and v != ""]
        unique_count = len(set(str(v) for v in non_null))
        null_count = len(values) - len(non_null)
        missing_cells += null_count

        col_type, py_type = detect_type(non_null)
        sg = semantic_group(col, col_type)

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
            "semanticGroup": sg, "nullCount": null_count, "uniqueCount": unique_count,
        })

    completeness = ((total_cells - missing_cells) / total_cells * 100) if total_cells else 0
    non_missing = total_cells - missing_cells
    consistency = (consistent_cells / non_missing * 100) if non_missing else 0
    quality_score = (completeness + consistency) / 2

    schema_lines = ["from pydantic import BaseModel, Field", "from typing import Optional, Any", "", "class InferredSchema(BaseModel):"]
    for col in column_profiles:
        safe = sanitize_python_name(col["name"])
        if safe != col["name"]:
            schema_lines.append(f'    {safe}: Optional[{col["pyType"]}] = Field(alias="{col["name"]}")')
        else:
            schema_lines.append(f'    {safe}: Optional[{col["pyType"]}]')

    return {
        "totalRows": len(flat_rows),
        "columns": column_profiles,
        "qualityScore": round(quality_score, 2),
        "completeness": round(completeness, 2),
        "consistency": round(consistency, 2),
        "pydanticSchema": "\n".join(schema_lines),
    }


if "flat_rows" not in st.session_state:
    st.session_state.flat_rows = []
if "summary" not in st.session_state:
    st.session_state.summary = None
if "df" not in st.session_state:
    st.session_state.df = None


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
    uploaded_file = st.file_uploader("Choose a JSON file", type=["json"], label_visibility="collapsed")
    if uploaded_file is not None:
        try:
            raw = json.loads(uploaded_file.read().decode("utf-8"))
            if not isinstance(raw, list):
                raw = [raw]
            flat_rows = [flatten_object(item) if isinstance(item, dict) else {"value": item} for item in raw]
            st.session_state.flat_rows = flat_rows
            st.session_state.summary = profile_data(flat_rows)
            st.session_state.df = pd.DataFrame(flat_rows)
            st.success(f"✅ File processed successfully — {len(flat_rows):,} rows loaded.")
        except json.JSONDecodeError as e:
            st.error(f"Invalid JSON: {e}")
        except Exception as e:
            st.error(f"Error processing file: {e}")

if st.session_state.summary:
    summary = st.session_state.summary
    flat_rows = st.session_state.flat_rows
    df = st.session_state.df

    st.markdown("---")

    m1, m2, m3 = st.columns(3)
    with m1:
        with st.container(border=True):
            st.markdown('<div class="metric-label">Total Rows</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="metric-value">{summary["totalRows"]:,}</div>', unsafe_allow_html=True)
    with m2:
        with st.container(border=True):
            st.markdown('<div class="metric-label">Total Columns</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="metric-value">{len(summary["columns"])}</div>', unsafe_allow_html=True)
    with m3:
        with st.container(border=True):
            score = summary["qualityScore"]
            score_class = "score-green" if score >= 90 else ("score-yellow" if score >= 70 else "score-red")
            st.markdown('<div class="metric-label">🛡 Data Quality Score</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="{score_class}">{score}%</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="metric-sub">Completeness: {summary["completeness"]}% | Consistency: {summary["consistency"]}%</div>',
                unsafe_allow_html=True,
            )

    st.markdown("---")

    tab_data, tab_profile = st.tabs(["📋 Data Table", "🔬 Column Profile"])

    with tab_data:
        c_search, c_toggle, c_group = st.columns([3, 2, 4])

        with c_search:
            search_term = st.text_input(
                "search", placeholder="🔍 Search all columns…", label_visibility="collapsed"
            )
        with c_toggle:
            view_mode = st.radio(
                "view", ["Horizontal", "Vertical"],
                index=1, horizontal=True, label_visibility="collapsed"
            )

        all_groups = ["All"] + sorted(set(c["semanticGroup"] for c in summary["columns"]))
        with c_group:
            selected_group = st.selectbox("group", all_groups, label_visibility="collapsed")

        if selected_group == "All":
            visible_col_profiles = [c for c in summary["columns"] if c["name"] in df.columns]
        else:
            visible_col_profiles = [
                c for c in summary["columns"]
                if c["semanticGroup"] == selected_group and c["name"] in df.columns
            ]
        visible_col_names = [c["name"] for c in visible_col_profiles]

        display_df = df[visible_col_names] if visible_col_names else df

        if search_term:
            mask = display_df.apply(
                lambda row: row.astype(str).str.contains(search_term, case=False, na=False).any(), axis=1
            )
            display_df = display_df[mask]

        if view_mode == "Vertical":
            records = display_df.reset_index(drop=True)
            type_map = {c["name"]: c["type"] for c in visible_col_profiles}
            vertical_rows = []
            for col_name in visible_col_names:
                row = {"Field Name": col_name, "Type": type_map.get(col_name, "")}
                for i in range(len(records)):
                    val = records.iloc[i].get(col_name)
                    row[f"Record {i + 1}"] = (
                        "" if val is None or (isinstance(val, float) and str(val) == "nan") else val
                    )
                vertical_rows.append(row)
            st.dataframe(pd.DataFrame(vertical_rows), use_container_width=True, height=500)
        else:
            st.dataframe(display_df, use_container_width=True, height=500)

        st.caption(f"Showing {len(display_df):,} of {len(df):,} rows | {len(visible_col_names)} columns")

    with tab_profile:
        profile_df = pd.DataFrame(summary["columns"]).rename(columns={
            "name": "Column", "type": "Type", "pyType": "Python Type",
            "semanticGroup": "Semantic Group", "nullCount": "Null Count", "uniqueCount": "Unique Count",
        })[["Column", "Type", "Python Type", "Semantic Group", "Null Count", "Unique Count"]]
        st.dataframe(profile_df, use_container_width=True, height=500)

else:
    st.info("Upload a JSON file above to get started.")
