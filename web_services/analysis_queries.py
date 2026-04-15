from collections import OrderedDict
import pandas as pd

from fastapi import HTTPException

from web_reports.detail_report import format_duration_text_for_report

from 共通スクリプト.analysis_service import (
    FLOW_PATTERN_CASE_COUNT_COLUMN,
    FLOW_PATTERN_COLUMN,
    create_pattern_flow_snapshot,
    create_rule_based_insights,
    normalize_filter_params,
    select_pattern_rows_for_flow,
)
from 共通スクリプト.duckdb_service import (
    query_analysis_records,
    query_bottleneck_summary,
    query_dashboard_summary,
    query_filtered_meta,
    query_impact_summary,
    query_root_cause_summary,
    query_transition_records_for_patterns,
    query_variant_summary,
)

from web_config.app_settings import FILTER_PARAM_NAMES

PROCESS_FLOW_PATTERN_CAP = 300
MAX_PATTERN_FLOW_CACHE = 24
LARGE_DATASET_FLOW_FAST_PATH_THRESHOLD = 1_000_000

def get_run_group_columns(run_data):
    return list(
        (run_data.get("result") or {}).get("group_columns")
        or run_data.get("group_columns")
        or []
    )

def get_run_variant_pattern(run_data, variant_id=None, pattern_index=None, filter_params=None):
    if pattern_index is not None:
        _, _, _, pattern = get_pattern_summary_row(run_data, pattern_index)
        return pattern

    if variant_id is not None:
        variant_item = get_variant_item(run_data, variant_id, filter_params=filter_params)
        return variant_item["pattern"]

    return None

def build_filter_cache_key(filter_params):
    normalized_filters = normalize_filter_params(**(filter_params or {}))
    return tuple(
        normalized_filters.get(filter_name)
        for filter_name in FILTER_PARAM_NAMES
    )

def get_filtered_meta_for_run(run_data, filter_params=None, variant_pattern=None):
    return query_filtered_meta(
        run_data["prepared_parquet_path"],
        filter_params=filter_params,
        filter_column_settings=run_data.get("column_settings"),
        variant_pattern=variant_pattern,
    )

def extract_pattern_text_from_row(row):
    for column_name, value in (row or {}).items():
        normalized_column_name = str(column_name or "").lower()
        normalized_value = str(value or "").strip()
        if not normalized_value:
            continue
        if "pattern" in normalized_column_name or "パターン" in str(column_name or ""):
            return normalized_value
    return ""

def build_pattern_index_entries_from_rows(pattern_rows):
    return [
        {
            "index": int(index),
            "pattern": extract_pattern_text_from_row(row),
        }
        for index, row in enumerate(pattern_rows or [])
    ]

def get_pattern_index_for_pattern(run_data, pattern_text):
    normalized_pattern = str(pattern_text or "").strip()
    if not normalized_pattern:
        return None

    pattern_analysis = run_data["result"]["analyses"].get("pattern")
    if not pattern_analysis:
        return None

    pattern_index_entries = run_data.get("pattern_index_entries")
    if pattern_index_entries is None:
        pattern_index_entries = build_pattern_index_entries_from_rows(pattern_analysis.get("rows", []))
        run_data["pattern_index_entries"] = pattern_index_entries

    for pattern_index, entry in enumerate(pattern_index_entries):
        if str(entry.get("pattern") or "").strip() == normalized_pattern:
            return pattern_index

    for pattern_index, row in enumerate(pattern_analysis.get("rows", [])):
        for column_name, value in row.items():
            normalized_column_name = str(column_name or "").lower()
            normalized_value = str(value or "").strip()
            if not normalized_value:
                continue
            if "pattern" not in normalized_column_name and "パターン" not in str(column_name or ""):
                continue
            if normalized_value == normalized_pattern:
                return pattern_index

    return None

def build_variant_response_item(variant_item, run_data=None):
    activities = variant_item["activities"]
    repeated_activity_count = len(activities) - len({str(activity or "").strip() for activity in activities if str(activity or "").strip()})
    repeat_flag = variant_item.get("repeat_flag", "○" if repeated_activity_count > 0 else "")
    return {
        "variant_id": variant_item["variant_id"],
        "activities": activities,
        "activity_count": variant_item.get("activity_count", len(activities)),
        "pattern": variant_item.get("pattern", ""),
        "pattern_index": (
            get_pattern_index_for_pattern(run_data, variant_item.get("pattern"))
            if run_data is not None
            else None
        ),
        "count": variant_item["count"],
        "ratio": variant_item["ratio"],
        "avg_case_duration_sec": variant_item.get("avg_case_duration_sec", 0.0),
        "avg_case_duration_text": variant_item.get("avg_case_duration_text", "0s"),
        "repeat_flag": repeat_flag,
    }

def _to_int(value, default=0):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return int(default)

def _to_float(value, default=0.0):
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return float(default)

def build_variant_items_from_pattern_rows(pattern_rows):
    if not pattern_rows:
        return []

    total_cases = sum(
        _to_int(row.get("ケース数", row.get("case_count", 0)))
        for row in pattern_rows
    )
    items = []

    for index, row in enumerate(pattern_rows, start=1):
        pattern_text = str(
            row.get("処理順パターン")
            or row.get("pattern")
            or ""
        ).strip()
        activities = [step.strip() for step in pattern_text.split("→") if step.strip()]
        case_count = _to_int(row.get("ケース数", row.get("case_count", 0)))
        ratio_pct = row.get("ケース比率(%)", row.get("case_ratio_pct"))
        avg_case_duration_min = _to_float(
            row.get("平均ケース処理時間(分)", row.get("平均ケース時間(分)", row.get("avg_case_duration_min", 0.0)))
        )

        if ratio_pct in (None, ""):
            ratio = round(case_count / total_cases, 4) if total_cases else 0.0
        else:
            ratio = round(_to_float(ratio_pct) / 100, 4)

        avg_case_duration_sec = round(avg_case_duration_min * 60, 2)
        items.append(
            {
                "variant_id": index,
                "activities": activities,
                "activity_count": len(activities),
                "pattern": pattern_text,
                "count": case_count,
                "ratio": ratio,
                "avg_case_duration_sec": avg_case_duration_sec,
                "avg_case_duration_text": format_duration_text_for_report(avg_case_duration_sec),
                "repeat_flag": "○" if len(set(activities)) < len(activities) else "",
            }
        )

    return items

def is_unfiltered_request(filter_cache_key):
    return all(filter_value is None for filter_value in filter_cache_key)

def get_variant_items(run_data, filter_params=None, variant_pattern=None):
    cache_key = (build_filter_cache_key(filter_params), str(variant_pattern or "").strip() or None)
    variant_cache = run_data.setdefault("variant_cache", {})

    if cache_key not in variant_cache:
        filter_cache_key = cache_key[0]
        if variant_pattern is None and is_unfiltered_request(filter_cache_key):
            pattern_analysis = run_data["result"]["analyses"].get("pattern")
            pattern_rows = pattern_analysis.get("rows", []) if pattern_analysis else []
            if pattern_rows:
                variant_cache[cache_key] = build_variant_items_from_pattern_rows(pattern_rows)
                return variant_cache[cache_key]

        variant_cache[cache_key] = query_variant_summary(
            run_data["prepared_parquet_path"],
            filter_params=filter_params,
            filter_column_settings=run_data.get("column_settings"),
            variant_pattern=variant_pattern,
            limit=None,
        )

    return variant_cache[cache_key]

def get_variant_item(run_data, variant_id, filter_params=None):
    safe_variant_id = int(variant_id)

    for variant_item in get_variant_items(run_data, filter_params=filter_params):
        if variant_item["variant_id"] == safe_variant_id:
            return variant_item

    raise HTTPException(status_code=404, detail="バリアントが見つかりません。")

def get_pattern_summary_row(run_data, pattern_index):
    pattern_analysis = run_data["result"]["analyses"].get("pattern")

    if not pattern_analysis:
        raise HTTPException(status_code=400, detail="処理順パターン分析を利用できません。")

    pattern_rows = pattern_analysis["rows"]
    safe_pattern_index = int(pattern_index)
    if safe_pattern_index < 0 or safe_pattern_index >= len(pattern_rows):
        raise HTTPException(status_code=404, detail="パターン番号が見つかりません。")

    pattern_index_entries = run_data.get("pattern_index_entries")
    if pattern_index_entries is None:
        pattern_index_entries = build_pattern_index_entries_from_rows(pattern_rows)
        run_data["pattern_index_entries"] = pattern_index_entries

    if safe_pattern_index >= len(pattern_index_entries):
        raise HTTPException(status_code=404, detail="パターン番号が見つかりません。")

    summary_row = pattern_rows[safe_pattern_index]
    pattern_entry = pattern_index_entries[safe_pattern_index]
    pattern = str(pattern_entry.get("pattern") or "").strip() or extract_pattern_text_from_row(summary_row)

    if not pattern:
        raise HTTPException(status_code=500, detail="パターン文字列を解決できませんでした。")

    return pattern_analysis, summary_row, pattern_entry, pattern

def get_analysis_data(run_data, analysis_key, filter_params=None, variant_pattern=None):
    normalized_filter_key = build_filter_cache_key(filter_params)
    analysis_cache = run_data.setdefault("analysis_cache", {})

    if variant_pattern is None and all(filter_value is None for filter_value in normalized_filter_key):
        analysis = run_data["result"]["analyses"].get(analysis_key)
        if analysis:
            return analysis

        cache_key = ("analysis", analysis_key, normalized_filter_key, None)
        if cache_key not in analysis_cache:
            analysis_cache[cache_key] = query_analysis_records(
                run_data["prepared_parquet_path"],
                analysis_key=analysis_key,
                filter_params=filter_params,
                filter_column_settings=run_data.get("column_settings"),
                variant_pattern=None,
            )
        return analysis_cache[cache_key]

    cache_key = ("analysis", analysis_key, normalized_filter_key, str(variant_pattern or "").strip() or None)

    if cache_key not in analysis_cache:
        analysis_cache[cache_key] = query_analysis_records(
            run_data["prepared_parquet_path"],
            analysis_key=analysis_key,
            filter_params=filter_params,
            filter_column_settings=run_data.get("column_settings"),
            variant_pattern=variant_pattern,
        )

    return analysis_cache[cache_key]

def get_bottleneck_summary(run_data, variant_id=None, pattern_index=None, filter_params=None):
    cache_key = (
        "bottleneck",
        None if variant_id is None else int(variant_id),
        None if pattern_index is None else int(pattern_index),
        build_filter_cache_key(filter_params),
    )
    cache = run_data.setdefault("bottleneck_cache", {})

    if cache_key not in cache:
        cache[cache_key] = query_bottleneck_summary(
            run_data["prepared_parquet_path"],
            filter_params=filter_params,
            filter_column_settings=run_data.get("column_settings"),
            variant_pattern=get_run_variant_pattern(
                run_data,
                variant_id=variant_id,
                pattern_index=pattern_index,
                filter_params=filter_params,
            ),
            limit=None,
        )

    return cache[cache_key]

def get_dashboard_summary(run_data, filter_params=None, prepared_df=None, variant_pattern=None):
    cache_key = (build_filter_cache_key(filter_params), str(variant_pattern or "").strip() or None)
    cache = run_data.setdefault("dashboard_cache", {})

    if cache_key not in cache:
        if variant_pattern:
            resolved_bottleneck_summary = query_bottleneck_summary(
                run_data["prepared_parquet_path"],
                filter_params=filter_params,
                filter_column_settings=run_data.get("column_settings"),
                variant_pattern=variant_pattern,
                limit=None,
            )
        else:
            resolved_bottleneck_summary = get_bottleneck_summary(run_data, filter_params=filter_params)
        cache[cache_key] = query_dashboard_summary(
            run_data["prepared_parquet_path"],
            filter_params=filter_params,
            filter_column_settings=run_data.get("column_settings"),
            variant_pattern=variant_pattern,
            variant_items=get_variant_items(
                run_data,
                filter_params=filter_params,
                variant_pattern=variant_pattern,
            )[:10],
            bottleneck_summary=resolved_bottleneck_summary,
            coverage_limit=10,
        )

    return cache[cache_key]

def get_root_cause_summary(run_data, filter_params=None, prepared_df=None, variant_pattern=None):
    cache_key = (build_filter_cache_key(filter_params), str(variant_pattern or "").strip() or None)
    cache = run_data.setdefault("root_cause_cache", {})

    if cache_key not in cache:
        cache[cache_key] = query_root_cause_summary(
            run_data["prepared_parquet_path"],
            filter_params=filter_params,
            filter_column_settings=run_data.get("column_settings"),
            variant_pattern=variant_pattern,
            limit=10,
        )

    return cache[cache_key]

def get_impact_summary(run_data, filter_params=None, prepared_df=None, variant_pattern=None):
    cache_key = (build_filter_cache_key(filter_params), str(variant_pattern or "").strip() or None)
    cache = run_data.setdefault("impact_cache", {})

    if cache_key not in cache:
        cache[cache_key] = query_impact_summary(
            run_data["prepared_parquet_path"],
            filter_params=filter_params,
            filter_column_settings=run_data.get("column_settings"),
            variant_pattern=variant_pattern,
            limit=None,
        )

    return cache[cache_key]

def get_rule_based_insights_summary(
    run_data,
    analysis_key,
    analysis_rows=None,
    filter_params=None,
    prepared_df=None,
    variant_pattern=None,
    dashboard_summary=None,
    bottleneck_summary=None,
    impact_summary=None,
):
    cache_key = (
        str(analysis_key or "").strip().lower(),
        build_filter_cache_key(filter_params),
        str(variant_pattern or "").strip() or None,
    )
    cache = run_data.setdefault("insights_cache", {})

    if cache_key not in cache:
        normalized_analysis_key = str(analysis_key or "").strip().lower()
        filtered_df = prepared_df
        if filtered_df is None:
            filtered_df = pd.DataFrame(columns=["case_id", "activity", "duration_sec"])

        resolved_bottleneck_summary = bottleneck_summary
        resolved_impact_summary = impact_summary
        if normalized_analysis_key in {"frequency", "transition", "pattern"}:
            if resolved_bottleneck_summary is None:
                resolved_bottleneck_summary = {
                    "activity_bottlenecks": [],
                    "transition_bottlenecks": [],
                    "activity_heatmap": {},
                    "transition_heatmap": {},
                }
            if resolved_impact_summary is None:
                resolved_impact_summary = {
                    "has_data": False,
                    "rows": [],
                }
        else:
            if resolved_bottleneck_summary is None:
                resolved_bottleneck_summary = get_bottleneck_summary(
                    run_data,
                    filter_params=filter_params,
                )
            if resolved_impact_summary is None:
                resolved_impact_summary = get_impact_summary(
                    run_data,
                    filter_params=filter_params,
                    variant_pattern=variant_pattern,
                )

        cache[cache_key] = create_rule_based_insights(
            filtered_df,
            analysis_key=analysis_key,
            analysis_rows=analysis_rows,
            dashboard_summary=dashboard_summary or get_dashboard_summary(
                run_data,
                filter_params=filter_params,
                variant_pattern=variant_pattern,
            ),
            bottleneck_summary=resolved_bottleneck_summary,
            impact_summary=resolved_impact_summary,
            max_items=5,
        )

    return cache[cache_key]

def get_pattern_flow_snapshot(
    run_data,
    pattern_percent,
    pattern_count,
    activity_percent,
    connection_percent,
    variant_id=None,
    filter_params=None,
    snapshot_builder=create_pattern_flow_snapshot,
    large_dataset_flow_fast_path_threshold=LARGE_DATASET_FLOW_FAST_PATH_THRESHOLD,
):
    filter_cache_key = build_filter_cache_key(filter_params)
    if variant_id is None:
        cache_key = (
            "pattern",
            int(pattern_percent),
            None if pattern_count is None else int(pattern_count),
            int(activity_percent),
            int(connection_percent),
            filter_cache_key,
        )
    else:
        cache_key = (
            "variant",
            int(variant_id),
            int(activity_percent),
            int(connection_percent),
            filter_cache_key,
        )
    cache = run_data.setdefault("pattern_flow_cache", OrderedDict())

    cached_snapshot = cache.get(cache_key)
    if cached_snapshot is not None:
        cache.move_to_end(cache_key)
        return cached_snapshot

    analyses = run_data["result"]["analyses"]
    variant_item = None
    variant_pattern = None
    if variant_id is not None:
        variant_item = get_variant_item(run_data, variant_id, filter_params=filter_params)
        variant_pattern = variant_item["pattern"]

    scoped_meta = get_filtered_meta_for_run(
        run_data,
        filter_params=filter_params,
        variant_pattern=variant_pattern,
    )
    scoped_event_count = int(scoped_meta.get("event_count") or 0)
    use_lightweight_flow = scoped_event_count >= large_dataset_flow_fast_path_threshold

    if variant_id is None:
        pattern_analysis = get_analysis_data(run_data, "pattern", filter_params=filter_params)
        frequency_analysis = (
            analyses.get("frequency")
            if is_unfiltered_request(filter_cache_key)
            else get_analysis_data(run_data, "frequency", filter_params=filter_params)
        )
        selected_transition_rows = []
        pattern_selection = select_pattern_rows_for_flow(
            pattern_analysis["rows"],
            pattern_percent=pattern_percent,
            pattern_count=pattern_count,
            pattern_cap=PROCESS_FLOW_PATTERN_CAP,
        )
        selected_patterns = []
        for row in pattern_selection["selected_pattern_rows"]:
            selected_pattern = extract_pattern_text_from_row(row)
            if selected_pattern:
                selected_patterns.append(selected_pattern)
        if not use_lightweight_flow and selected_patterns:
            selected_transition_rows = query_transition_records_for_patterns(
                run_data["prepared_parquet_path"],
                selected_patterns,
                filter_params=filter_params,
                filter_column_settings=run_data.get("column_settings"),
            )

        snapshot = snapshot_builder(
            pattern_rows=pattern_analysis["rows"],
            prepared_df=None,
            transition_rows=selected_transition_rows,
            frequency_rows=(frequency_analysis or {}).get("rows", []),
            pattern_percent=pattern_percent,
            pattern_count=pattern_count,
            activity_percent=activity_percent,
            connection_percent=connection_percent,
            pattern_cap=PROCESS_FLOW_PATTERN_CAP,
        )
    else:
        if use_lightweight_flow:
            snapshot = snapshot_builder(
                pattern_rows=[
                    {
                        FLOW_PATTERN_CASE_COUNT_COLUMN: int(variant_item["count"]),
                        FLOW_PATTERN_COLUMN: variant_pattern,
                    }
                ],
                prepared_df=None,
                frequency_rows=[],
                pattern_percent=100,
                pattern_count=1,
                activity_percent=activity_percent,
                connection_percent=connection_percent,
                pattern_cap=1,
            )
        else:
            selected_transition_rows = query_transition_records_for_patterns(
                run_data["prepared_parquet_path"],
                [variant_pattern],
                filter_params=filter_params,
                filter_column_settings=run_data.get("column_settings"),
            )
            snapshot = snapshot_builder(
                pattern_rows=[
                    {
                        FLOW_PATTERN_CASE_COUNT_COLUMN: int(variant_item["count"]),
                        FLOW_PATTERN_COLUMN: variant_pattern,
                    }
                ],
                prepared_df=None,
                transition_rows=selected_transition_rows,
                frequency_rows=[],
                pattern_percent=100,
                pattern_count=1,
                activity_percent=activity_percent,
                connection_percent=connection_percent,
                pattern_cap=1,
            )
        snapshot["selected_variant"] = build_variant_response_item(variant_item, run_data=run_data)

    if use_lightweight_flow:
        snapshot["is_large_dataset_optimized"] = True

    cache[cache_key] = snapshot
    cache.move_to_end(cache_key)
    while len(cache) > MAX_PATTERN_FLOW_CACHE:
        cache.popitem(last=False)

    return snapshot
