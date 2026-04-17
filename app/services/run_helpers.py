from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
import shutil
from uuid import uuid4

from fastapi import HTTPException, Request

from app.services.analysis_queries import build_pattern_index_entries_from_rows

from core.analysis_service import (
    get_filter_options,
    merge_filter_params,
    normalize_filter_column_settings,
    normalize_filter_params,
)
from core.duckdb_service import persist_prepared_parquet


from app.config.app_settings import FILTER_PARAM_NAMES
FILTER_COLUMN_NAMES = ("filter_column_1", "filter_column_2", "filter_column_3")
FILTER_LABEL_NAMES = ("filter_label_1", "filter_label_2", "filter_label_3")

RUN_STORE = OrderedDict()


def validate_filter_column_settings(filter_column_settings):
    selected_filter_columns = [
        filter_config["column_name"]
        for filter_config in filter_column_settings.values()
        if filter_config["column_name"]
    ]

    if len(selected_filter_columns) != len(set(selected_filter_columns)):
        raise ValueError(
            "グループ/カテゴリー フィルター①〜③ にはそれぞれ異なる列を選択してください。"
        )


def get_run_storage_dir(run_id, run_storage_dir):
    return Path(run_storage_dir) / str(run_id)


def get_run_prepared_parquet_path(run_id, run_storage_dir):
    return get_run_storage_dir(run_id, run_storage_dir) / "prepared.parquet"


def cleanup_run_storage(run_id, run_storage_dir):
    run_storage_dir_path = get_run_storage_dir(run_id, run_storage_dir)
    if run_storage_dir_path.exists():
        shutil.rmtree(run_storage_dir_path, ignore_errors=True)


def save_run_data(
    source_file_name,
    selected_analysis_keys,
    prepared_df,
    result,
    column_settings,
    base_filter_params,
    run_storage_dir,
    max_stored_runs,
):
    run_id = uuid4().hex
    prepared_row_count = int(len(prepared_df))
    pattern_rows = ((result or {}).get("analyses", {}).get("pattern") or {}).get("rows", [])
    filter_options = get_filter_options(
        prepared_df,
        filter_column_settings=column_settings,
    )
    prepared_parquet_path = get_run_prepared_parquet_path(run_id, run_storage_dir)
    persist_prepared_parquet(prepared_df, prepared_parquet_path)
    RUN_STORE[run_id] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_file_name": source_file_name,
        "selected_analysis_keys": selected_analysis_keys,
        "prepared_df": None,
        "prepared_row_count": prepared_row_count,
        "prepared_parquet_path": str(prepared_parquet_path),
        "result": result,
        "column_settings": column_settings,
        "base_filter_params": base_filter_params,
        "pattern_index_entries": build_pattern_index_entries_from_rows(pattern_rows),
        "pattern_flow_cache": OrderedDict(),
        "variant_cache": {},
        "bottleneck_cache": {},
        "dashboard_cache": {},
        "root_cause_cache": {},
        "impact_cache": {},
        "insights_cache": {},
        "ai_insights_cache": {},
        "analysis_cache": {},
        "filter_options": filter_options,
    }
    RUN_STORE.move_to_end(run_id)

    while len(RUN_STORE) > max_stored_runs:
        removed_run_id, _ = RUN_STORE.popitem(last=False)
        cleanup_run_storage(removed_run_id, run_storage_dir)

    return run_id


def get_run_data(run_id):
    run_data = RUN_STORE.get(run_id)

    if not run_data:
        raise HTTPException(status_code=404, detail="分析データが見つかりません。")

    RUN_STORE.move_to_end(run_id)
    return run_data


def has_parquet_backing(run_data):
    prepared_parquet_path = str(run_data.get("prepared_parquet_path") or "").strip()
    return prepared_parquet_path.endswith(".parquet")


def get_request_filter_params(request: Request):
    return normalize_filter_params(
        **{
            filter_name: request.query_params.get(filter_name)
            for filter_name in FILTER_PARAM_NAMES
        }
    )


def get_form_filter_params(form):
    return normalize_filter_params(
        **{
            filter_name: form.get(filter_name)
            for filter_name in FILTER_PARAM_NAMES
        }
    )


def get_form_filter_column_settings(form):
    raw_settings = {
        setting_name: form.get(setting_name)
        for setting_name in (*FILTER_COLUMN_NAMES, *FILTER_LABEL_NAMES)
    }
    return normalize_filter_column_settings(**raw_settings)


def get_effective_filter_params(run_data, filter_params=None):
    return merge_filter_params(run_data.get("base_filter_params"), filter_params)


def build_column_settings_payload(column_settings):
    raw_column_settings = column_settings or {}
    filter_slot_names = ("filter_value_1", "filter_value_2", "filter_value_3")

    if any(
        isinstance(raw_column_settings.get(filter_slot_name), dict)
        for filter_slot_name in filter_slot_names
    ):
        default_filter_settings = normalize_filter_column_settings()
        normalized_filter_settings = {
            filter_slot_name: {
                "column_name": str(
                    (raw_column_settings.get(filter_slot_name) or {}).get("column_name")
                    or ""
                ).strip()
                or None,
                "label": str(
                    (raw_column_settings.get(filter_slot_name) or {}).get("label")
                    or ""
                ).strip()
                or default_filter_settings[filter_slot_name]["label"],
            }
            for filter_slot_name in filter_slot_names
        }
    else:
        normalized_filter_settings = normalize_filter_column_settings(
            **raw_column_settings
        )

    return {
        "case_id_column": str(raw_column_settings.get("case_id_column") or "").strip(),
        "activity_column": str(raw_column_settings.get("activity_column") or "").strip(),
        "timestamp_column": str(raw_column_settings.get("timestamp_column") or "").strip(),
        "filters": [
            {
                "slot": filter_key,
                **normalized_filter_settings[filter_key],
            }
            for filter_key in normalized_filter_settings
        ],
    }


def build_filter_summary_text(filter_params, column_settings):
    normalized_filters = normalize_filter_params(**(filter_params or {}))
    column_payload = build_column_settings_payload(column_settings)
    filter_label_map = {
        filter_item["slot"]: filter_item["label"]
        for filter_item in column_payload.get("filters", [])
    }
    summary_items = []

    if normalized_filters.get("date_from"):
        summary_items.append(f"開始日: {normalized_filters['date_from']}")
    if normalized_filters.get("date_to"):
        summary_items.append(f"終了日: {normalized_filters['date_to']}")

    for filter_slot in ("filter_value_1", "filter_value_2", "filter_value_3"):
        if normalized_filters.get(filter_slot):
            summary_items.append(
                f"{filter_label_map.get(filter_slot, filter_slot)}: {normalized_filters[filter_slot]}"
            )

    activity_values = normalized_filters.get("activity_values")
    if activity_values:
        activity_label = (
            "アクティビティ 含む"
            if normalized_filters.get("activity_mode") != "exclude"
            else "アクティビティ 除外"
        )
        summary_items.append(f"{activity_label}: {activity_values}")

    start_activity_values = normalized_filters.get("start_activity_values")
    if start_activity_values:
        summary_items.append(f"開始アクティビティ: {start_activity_values}")

    end_activity_values = normalized_filters.get("end_activity_values")
    if end_activity_values:
        summary_items.append(f"終了アクティビティ: {end_activity_values}")

    return " / ".join(summary_items) if summary_items else "未適用"
