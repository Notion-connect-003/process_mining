from fastapi import HTTPException
import pandas as pd

from core.analysis_service import (
    create_log_diagnostics,
    get_filter_options,
    normalize_filter_column_settings,
)


def normalize_header_name(value):
    return str(value or "").strip()


def build_header_lookup(headers):
    exact_lookup = {}
    casefold_lookup = {}

    for header in headers:
        normalized_header = normalize_header_name(header)
        if not normalized_header:
            continue
        exact_lookup.setdefault(normalized_header, normalized_header)
        casefold_lookup.setdefault(normalized_header.casefold(), normalized_header)

    return exact_lookup, casefold_lookup


def suggest_column_name(headers, field_name, column_candidates, preferred_name=""):
    exact_lookup, casefold_lookup = build_header_lookup(headers)
    requested_name = normalize_header_name(preferred_name)

    if requested_name:
        exact_match = exact_lookup.get(requested_name)
        if exact_match:
            return exact_match

        casefold_match = casefold_lookup.get(requested_name.casefold())
        if casefold_match:
            return casefold_match

    for candidate_name in column_candidates.get(field_name, []):
        normalized_candidate = normalize_header_name(candidate_name)
        if not normalized_candidate:
            continue

        exact_match = exact_lookup.get(normalized_candidate)
        if exact_match:
            return exact_match

        casefold_match = casefold_lookup.get(normalized_candidate.casefold())
        if casefold_match:
            return casefold_match

    return ""


def resolve_required_column_name(
    headers,
    field_name,
    preferred_name="",
    *,
    default_headers,
    column_candidates,
    column_display_labels,
):
    resolved_name = suggest_column_name(
        headers,
        field_name,
        column_candidates,
        preferred_name=preferred_name,
    )
    if resolved_name:
        return resolved_name

    requested_name = (
        normalize_header_name(preferred_name)
        or default_headers.get(field_name)
        or (column_candidates.get(field_name) or [""])[0]
    )
    available_headers = ", ".join(
        normalized_header
        for normalized_header in (normalize_header_name(header) for header in headers)
        if normalized_header
    ) or "(none)"
    field_label = column_display_labels.get(field_name, field_name)
    raise ValueError(
        f"{field_label}列 '{requested_name}' が見つかりません。"
        f"CSVのヘッダーを確認してください。利用可能な列: {available_headers}"
    )


def build_column_selection_payload(headers, *, default_headers, column_candidates):
    return {
        "headers": headers,
        "default_selection": {
            field_name: suggest_column_name(
                headers,
                field_name,
                column_candidates,
                preferred_name=default_header,
            )
            for field_name, default_header in default_headers.items()
        },
    }


def validate_selected_columns(case_id_column, activity_column, timestamp_column):
    selected_columns = {
        "\u30b1\u30fc\u30b9ID": case_id_column,
        "\u30a2\u30af\u30c6\u30a3\u30d3\u30c6\u30a3": activity_column,
        "\u30bf\u30a4\u30e0\u30b9\u30bf\u30f3\u30d7": timestamp_column,
    }

    missing_fields = [
        field_label
        for field_label, column_name in selected_columns.items()
        if not str(column_name or "").strip()
    ]
    if missing_fields:
        raise ValueError(
            f"\u6b21\u306e\u5217\u3092\u9078\u629e\u3057\u3066\u304f\u3060\u3055\u3044: {' / '.join(missing_fields)}"
        )

    normalized_columns = [
        column_name.strip() for column_name in selected_columns.values()
    ]
    if len(set(normalized_columns)) != len(normalized_columns):
        raise ValueError(
            "\u30b1\u30fc\u30b9ID\u5217 / \u30a2\u30af\u30c6\u30a3\u30d3\u30c6\u30a3\u5217 / "
            "\u30bf\u30a4\u30e0\u30b9\u30bf\u30f3\u30d7\u5217\u306b\u306f\u305d\u308c\u305e\u308c\u7570\u306a\u308b\u5217\u3092\u9078\u629e\u3057\u3066\u304f\u3060\u3055\u3044\u3002"
        )


def read_raw_log_dataframe(file_source):
    if hasattr(file_source, "seek"):
        file_source.seek(0)

    try:
        raw_df = pd.read_csv(file_source, dtype=str, keep_default_na=False)
    finally:
        if hasattr(file_source, "seek"):
            file_source.seek(0)

    return raw_df


def resolve_profile_file_source(form, profile_sample_file):
    uploaded_file = form.get("csv_file")

    if uploaded_file and uploaded_file.filename:
        uploaded_file.file.seek(0)
        return uploaded_file.file, uploaded_file.filename

    return profile_sample_file, profile_sample_file.name


def get_static_version(base_dir):
    static_dir = base_dir / "static"
    static_file_versions = [
        entry.stat().st_mtime_ns
        for entry in static_dir.rglob("*")
        if entry.is_file()
    ]
    return str(max(static_file_versions, default=0))


def build_analysis_payload(analysis, row_limit=None, row_offset=0):
    total_row_count = len(analysis["rows"])
    safe_row_offset = max(0, int(row_offset or 0))

    if row_limit is None:
        safe_row_limit = total_row_count
        rows = analysis["rows"][safe_row_offset:]
    else:
        safe_row_limit = max(0, int(row_limit))
        rows = analysis["rows"][safe_row_offset : safe_row_offset + safe_row_limit]

    page_end_row_number = safe_row_offset + len(rows)
    has_previous_page = safe_row_offset > 0
    has_next_page = page_end_row_number < total_row_count
    previous_row_offset = max(0, safe_row_offset - safe_row_limit) if has_previous_page else None
    next_row_offset = page_end_row_number if has_next_page else None

    return {
        "analysis_name": analysis["analysis_name"],
        "sheet_name": analysis["sheet_name"],
        "output_file_name": analysis.get("output_file_name"),
        "row_count": total_row_count,
        "returned_row_count": len(rows),
        "row_offset": safe_row_offset,
        "page_size": safe_row_limit,
        "page_start_row_number": safe_row_offset + 1 if rows else 0,
        "page_end_row_number": page_end_row_number,
        "has_previous_page": has_previous_page,
        "has_next_page": has_next_page,
        "previous_row_offset": previous_row_offset,
        "next_row_offset": next_row_offset,
        "rows": rows,
        "excel_file": analysis["excel_file"],
    }


def build_variant_coverage_payload(total_case_count, variant_items):
    covered_case_count = sum(int(variant_item["count"]) for variant_item in variant_items)
    return {
        "displayed_variant_count": len(variant_items),
        "covered_case_count": covered_case_count,
        "total_case_count": int(total_case_count),
        "ratio": round(covered_case_count / total_case_count, 4) if total_case_count else 0.0,
    }


def get_filter_options_payload(run_data, query_filter_options_fn):
    filter_options = run_data.get("filter_options")
    if filter_options is None:
        prepared_parquet_path = run_data.get("prepared_parquet_path")
        if not prepared_parquet_path:
            raise HTTPException(
                status_code=500,
                detail="フィルター候補を取得する前に分析を実行してください。",
            )
        filter_options = query_filter_options_fn(
            prepared_parquet_path,
            filter_column_settings=run_data.get("column_settings"),
        )
        run_data["filter_options"] = filter_options

    return filter_options


def build_preview_response(
    run_id,
    source_file_name,
    selected_analysis_keys,
    result,
    run_data,
    *,
    build_column_settings_payload_fn,
    get_filter_options_payload_fn,
    preview_row_count,
):
    return {
        "run_id": run_id,
        "source_file_name": source_file_name,
        "selected_analysis_keys": selected_analysis_keys,
        "case_count": result["case_count"],
        "event_count": result["event_count"],
        "group_columns": result.get("group_columns", []),
        "group_mode": result.get("group_mode", False),
        "group_summary": result.get("group_summary", {}),
        "applied_filters": run_data.get("base_filter_params"),
        "column_settings": build_column_settings_payload_fn(run_data.get("column_settings")),
        "filter_options": get_filter_options_payload_fn(run_data),
        "analyses": {
            analysis_key: build_analysis_payload(analysis, preview_row_count)
            for analysis_key, analysis in result["analyses"].items()
        },
    }


def build_log_profile_payload(
    raw_df,
    source_file_name,
    *,
    case_id_column="",
    activity_column="",
    timestamp_column="",
    filter_column_settings=None,
    include_diagnostics=False,
    default_headers,
    column_candidates,
    build_column_settings_payload_fn,
):
    headers = [str(column_name) for column_name in raw_df.columns.tolist()]
    selection_payload = build_column_selection_payload(
        headers,
        default_headers=default_headers,
        column_candidates=column_candidates,
    )
    resolved_case_id_column = suggest_column_name(
        headers,
        "case_id_column",
        column_candidates,
        preferred_name=case_id_column,
    )
    resolved_activity_column = suggest_column_name(
        headers,
        "activity_column",
        column_candidates,
        preferred_name=activity_column,
    )
    resolved_timestamp_column = suggest_column_name(
        headers,
        "timestamp_column",
        column_candidates,
        preferred_name=timestamp_column,
    )
    normalized_filter_column_settings = normalize_filter_column_settings(
        **(filter_column_settings or {})
    )

    return {
        "source_file_name": source_file_name,
        **selection_payload,
        "column_settings": build_column_settings_payload_fn(
            {
                "case_id_column": resolved_case_id_column,
                "activity_column": resolved_activity_column,
                "timestamp_column": resolved_timestamp_column,
                **normalized_filter_column_settings,
            }
        ),
        "filter_options": get_filter_options(
            raw_df,
            filter_column_settings=normalized_filter_column_settings,
        ),
        "diagnostics": (
            create_log_diagnostics(
                raw_df,
                case_id_column=resolved_case_id_column,
                activity_column=resolved_activity_column,
                timestamp_column=resolved_timestamp_column,
                filter_column_settings=normalized_filter_column_settings,
            )
            if include_diagnostics
            else None
        ),
    }


def get_analysis_options(default_analysis_keys, get_available_analysis_definitions_fn):
    analysis_definitions = get_available_analysis_definitions_fn()
    analysis_options = []

    for analysis_key in default_analysis_keys:
        analysis_options.append(
            {
                "key": analysis_key,
                "label": analysis_definitions[analysis_key]["config"]["analysis_name"],
            }
        )

    return analysis_options
