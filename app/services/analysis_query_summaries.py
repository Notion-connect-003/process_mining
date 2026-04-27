from collections import OrderedDict
import pandas as pd

from fastapi import HTTPException

from excel.detail.report import format_duration_text_for_report
from app.services.cache_keys import build_filter_cache_key as build_filter_cache_key_impl
from app.services.analysis_query_records import (
    build_filter_cache_key,
    get_run_variant_pattern,
    get_variant_items,
)

from core.analysis_service import (
    FLOW_PATTERN_CASE_COUNT_COLUMN,
    FLOW_PATTERN_COLUMN,
    create_pattern_flow_snapshot,
    create_rule_based_insights,
    normalize_filter_params,
    select_pattern_rows_for_flow,
)
from core.duckdb_service import (
    query_analysis_records,
    query_bottleneck_summary,
    query_dashboard_summary,
    query_filtered_meta,
    query_impact_summary,
    query_root_cause_summary,
    query_transition_records_for_patterns,
    query_variant_summary,
)

PROCESS_FLOW_PATTERN_CAP = 300
MAX_PATTERN_FLOW_CACHE = 24
LARGE_DATASET_FLOW_FAST_PATH_THRESHOLD = 1_000_000

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
