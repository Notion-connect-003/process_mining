from collections import OrderedDict
import pandas as pd

from fastapi import HTTPException

from excel.detail.report import format_duration_text_for_report
from app.services.cache_keys import build_filter_cache_key as build_filter_cache_key_impl
from app.services.analysis_query_records import (
    build_filter_cache_key,
    build_variant_response_item,
    extract_pattern_text_from_row,
    get_analysis_data,
    get_filtered_meta_for_run,
    get_variant_item,
    is_unfiltered_request,
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



