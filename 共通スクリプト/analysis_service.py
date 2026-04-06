import math
from collections import defaultdict

import pandas as pd

from 共通スクリプト.data_loader import prepare_event_log, read_csv_data
from 共通スクリプト.Excel出力.excel_exporter import (
    convert_analysis_result_to_records,
    export_analysis_to_excel,
)
from 共通スクリプト.分析.前後処理分析.transition_analysis import (
    ANALYSIS_CONFIG as TRANSITION_ANALYSIS_CONFIG,
    create_transition_analysis,
)
from 共通スクリプト.分析.処理順パターン分析.pattern_analysis import (
    ANALYSIS_CONFIG as PATTERN_ANALYSIS_CONFIG,
    create_pattern_analysis,
)
from 共通スクリプト.分析.頻度分析.frequency_analysis import (
    ANALYSIS_CONFIG as FREQUENCY_ANALYSIS_CONFIG,
    create_frequency_analysis,
)


ANALYSIS_DEFINITIONS = {
    "frequency": {
        "create_function": create_frequency_analysis,
        "config": FREQUENCY_ANALYSIS_CONFIG,
    },
    "transition": {
        "create_function": create_transition_analysis,
        "config": TRANSITION_ANALYSIS_CONFIG,
    },
    "pattern": {
        "create_function": create_pattern_analysis,
        "config": PATTERN_ANALYSIS_CONFIG,
    },
}

DEFAULT_ANALYSIS_KEYS = ["frequency", "transition", "pattern"]

FLOW_FREQUENCY_ACTIVITY_COLUMN = "アクティビティ"
FLOW_FREQUENCY_EVENT_COUNT_COLUMN = "イベント件数"
FLOW_FREQUENCY_CASE_COUNT_COLUMN = "ケース数"
FLOW_FREQUENCY_AVG_DURATION_COLUMN = "平均時間(分)"
FLOW_FREQUENCY_RATIO_COLUMN = "イベント比率(%)"
FLOW_TRANSITION_FROM_COLUMN = "前処理アクティビティ名"
FLOW_TRANSITION_TO_COLUMN = "後処理アクティビティ名"
FLOW_TRANSITION_COUNT_COLUMN = "遷移件数"
FLOW_TRANSITION_AVG_WAIT_COLUMN = "平均待ち時間(分)"
FLOW_TRANSITION_RATIO_COLUMN = "遷移比率(%)"
FLOW_PATTERN_CASE_COUNT_COLUMN = "ケース数"
FLOW_PATTERN_COLUMN = "処理順パターン"
FLOW_PATTERN_CASE_RATIO_COLUMN = "ケース比率(%)"
FLOW_PATTERN_AVG_CASE_DURATION_COLUMN = "平均ケース時間(分)"
FLOW_PATH_SEPARATOR = "→"
FLOW_PATTERN_CAP = 500
FLOW_LAYOUT_SWEEP_ITERATIONS = 4
FILTER_SLOT_KEYS = ("filter_value_1", "filter_value_2", "filter_value_3")
FILTER_PARAM_KEYS = ("date_from", "date_to", *FILTER_SLOT_KEYS)
DEFAULT_FILTER_LABELS = {
    "filter_value_1": "グループ/カテゴリー フィルター①",
    "filter_value_2": "グループ/カテゴリー フィルター②",
    "filter_value_3": "グループ/カテゴリー フィルター③",
}
INSIGHT_ATTENTION_ACTIVITY_KEYWORDS = (
    "差戻し",
    "差し戻し",
    "再提出",
    "再申請",
    "手戻り",
    "再確認",
)

# -----------------------------------------------------------------------------
# Analysis execution
# -----------------------------------------------------------------------------


def get_available_analysis_definitions():
    return ANALYSIS_DEFINITIONS.copy()


def resolve_analysis_keys(selected_analysis_keys=None):
    if selected_analysis_keys is None:
        analysis_keys = DEFAULT_ANALYSIS_KEYS
    else:
        analysis_keys = selected_analysis_keys

    if not analysis_keys:
        raise ValueError("Please select at least one analysis key.")

    return analysis_keys


def load_prepared_event_log(
    file_source,
    case_id_column,
    activity_column,
    timestamp_column,
):
    raw_df = read_csv_data(
        file_path=file_source,
        case_id_column=case_id_column,
        activity_column=activity_column,
    )
    return prepare_event_log(
        df=raw_df,
        case_id_column=case_id_column,
        activity_column=activity_column,
        timestamp_column=timestamp_column,
    )


def analyze_prepared_event_log(
    prepared_df,
    selected_analysis_keys=None,
    output_root_dir=None,
    export_excel=False,
):
    analysis_keys = resolve_analysis_keys(selected_analysis_keys)
    analysis_results = {}

    for analysis_key in analysis_keys:
        if analysis_key not in ANALYSIS_DEFINITIONS:
            raise ValueError(f"Unsupported analysis key: {analysis_key}")

        definition = ANALYSIS_DEFINITIONS[analysis_key]
        result_df = definition["create_function"](prepared_df)
        analysis_config = definition["config"]

        excel_file = None
        if export_excel:
            excel_file = export_analysis_to_excel(
                df=result_df,
                output_root_dir=output_root_dir,
                analysis_name=analysis_config["analysis_name"],
                output_file_name=analysis_config["output_file_name"],
                sheet_name=analysis_config["sheet_name"],
                display_columns=analysis_config["display_columns"],
            )

        analysis_results[analysis_key] = {
            "analysis_name": analysis_config["analysis_name"],
            "sheet_name": analysis_config["sheet_name"],
            "output_file_name": analysis_config["output_file_name"],
            "rows": convert_analysis_result_to_records(
                result_df,
                analysis_config["display_columns"],
            ),
            "excel_file": str(excel_file.resolve()) if excel_file else None,
        }

    return {
        "case_count": int(prepared_df["case_id"].nunique()),
        "event_count": int(len(prepared_df)),
        "analyses": analysis_results,
    }


def create_analysis_records(prepared_df, analysis_key):
    if analysis_key not in ANALYSIS_DEFINITIONS:
        raise ValueError(f"Unsupported analysis key: {analysis_key}")

    definition = ANALYSIS_DEFINITIONS[analysis_key]
    analysis_config = definition["config"]
    result_df = definition["create_function"](prepared_df)

    return {
        "analysis_name": analysis_config["analysis_name"],
        "sheet_name": analysis_config["sheet_name"],
        "output_file_name": analysis_config["output_file_name"],
        "rows": convert_analysis_result_to_records(
            result_df,
            analysis_config["display_columns"],
        ),
        "excel_file": None,
    }


# -----------------------------------------------------------------------------
# Variant and pattern helpers
# -----------------------------------------------------------------------------

def build_case_variant_table(prepared_df):
    return (
        prepared_df.sort_values(["case_id", "sequence_no"])
        .groupby("case_id")["activity"]
        .apply(lambda series: tuple(series.tolist()))
        .reset_index(name="activities")
    )


def build_case_pattern_table(prepared_df):
    case_variant_df = build_case_variant_table(prepared_df)
    case_variant_df["pattern"] = case_variant_df["activities"].apply(
        lambda activities: FLOW_PATH_SEPARATOR.join(activities)
    )
    return case_variant_df[["case_id", "pattern"]]


def create_variant_summary(prepared_df, limit=10):
    case_variant_df = build_case_variant_table(prepared_df)
    case_variant_df["pattern"] = case_variant_df["activities"].apply(
        lambda activities: FLOW_PATH_SEPARATOR.join(activities)
    )
    case_duration_df = (
        prepared_df.groupby("case_id", as_index=False)
        .agg(case_duration_sec=("duration_sec", "sum"))
    )
    case_variant_df = case_variant_df.merge(case_duration_df, on="case_id", how="left")

    total_cases = int(case_variant_df["case_id"].nunique())
    variant_summary_df = (
        case_variant_df.groupby(["activities", "pattern"])
        .agg(
            count=("case_id", "count"),
            avg_case_duration_sec=("case_duration_sec", "mean"),
        )
        .reset_index()
        .sort_values(["count", "pattern"], ascending=[False, True])
        .reset_index(drop=True)
    )
    variant_summary_df["avg_case_duration_sec"] = (
        variant_summary_df["avg_case_duration_sec"]
        .fillna(0)
        .round(2)
    )

    if limit is not None:
        variant_summary_df = variant_summary_df.head(max(0, int(limit))).reset_index(drop=True)

    return [
        {
            "variant_id": index + 1,
            "activities": list(row["activities"]),
            "activity_count": int(len(row["activities"])),
            "pattern": row["pattern"],
            "count": int(row["count"]),
            "ratio": round(float(row["count"]) / total_cases, 4) if total_cases else 0.0,
            "avg_case_duration_sec": float(row["avg_case_duration_sec"]),
            "avg_case_duration_text": _format_duration_text(row["avg_case_duration_sec"]),
        }
        for index, row in enumerate(variant_summary_df.to_dict(orient="records"))
    ]


def create_pattern_index_entries(prepared_df):
    pattern_summary_df = create_pattern_analysis(prepared_df)
    return pattern_summary_df.to_dict(orient="records")


HEAT_CLASS_COUNT = 5


def build_transition_key(from_activity, to_activity):
    return f"{from_activity}__TO__{to_activity}"


# -----------------------------------------------------------------------------
# Filter normalization and log diagnostics
# -----------------------------------------------------------------------------


def normalize_filter_params(
    date_from=None,
    date_to=None,
    filter_value_1=None,
    filter_value_2=None,
    filter_value_3=None,
    activity_mode=None,
    activity_values=None,
    **_,
):
    normalized_activity_mode = str(activity_mode or "").strip().lower()
    if normalized_activity_mode not in {"include", "exclude"}:
        normalized_activity_mode = None

    if isinstance(activity_values, (list, tuple, set)):
        raw_activity_values = [str(value or "").strip() for value in activity_values]
    else:
        raw_activity_values = [
            value.strip()
            for value in str(activity_values or "").split(",")
        ]
    normalized_activity_values = list(dict.fromkeys([value for value in raw_activity_values if value]))

    raw_params = {
        "date_from": date_from,
        "date_to": date_to,
        "filter_value_1": filter_value_1,
        "filter_value_2": filter_value_2,
        "filter_value_3": filter_value_3,
        "activity_mode": normalized_activity_mode,
        "activity_values": ",".join(normalized_activity_values) if normalized_activity_values else None,
    }

    return {
        filter_key: (str(filter_value).strip() if str(filter_value or "").strip() else None)
        for filter_key, filter_value in raw_params.items()
    }


def _normalize_filter_slot_setting(filter_key, raw_setting):
    raw_setting = raw_setting or {}
    column_name = str(raw_setting.get("column_name") or "").strip() or None
    label = str(raw_setting.get("label") or "").strip() or DEFAULT_FILTER_LABELS[filter_key]
    return {
        "column_name": column_name,
        "label": label,
    }


def normalize_filter_column_settings(
    filter_column_1=None,
    filter_column_2=None,
    filter_column_3=None,
    filter_label_1=None,
    filter_label_2=None,
    filter_label_3=None,
    filter_value_1=None,
    filter_value_2=None,
    filter_value_3=None,
    **_,
):
    if any(isinstance(raw_value, dict) for raw_value in (filter_value_1, filter_value_2, filter_value_3)):
        raw_settings = {
            "filter_value_1": filter_value_1 if isinstance(filter_value_1, dict) else {},
            "filter_value_2": filter_value_2 if isinstance(filter_value_2, dict) else {},
            "filter_value_3": filter_value_3 if isinstance(filter_value_3, dict) else {},
        }
    else:
        raw_settings = {
            "filter_value_1": {
                "column_name": filter_column_1,
                "label": filter_label_1,
            },
            "filter_value_2": {
                "column_name": filter_column_2,
                "label": filter_label_2,
            },
            "filter_value_3": {
                "column_name": filter_column_3,
                "label": filter_label_3,
            },
        }

    normalized_settings = {}
    for filter_key in FILTER_SLOT_KEYS:
        normalized_settings[filter_key] = _normalize_filter_slot_setting(
            filter_key,
            raw_settings.get(filter_key),
        )

    return normalized_settings


def merge_filter_params(base_filter_params=None, override_filter_params=None):
    merged_filters = normalize_filter_params(**(base_filter_params or {}))
    merged_filters.update(
        {
            filter_key: filter_value
            for filter_key, filter_value in normalize_filter_params(**(override_filter_params or {})).items()
            if filter_value is not None
        }
    )
    return merged_filters


def _parse_filter_datetime(value, is_end=False):
    if not value:
        return None

    parsed_value = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed_value):
        return None

    if is_end and len(str(value)) <= 10:
        return parsed_value.normalize() + pd.Timedelta(days=1)

    return parsed_value


def filter_prepared_df(prepared_df, filter_params=None, filter_column_settings=None):
    if not filter_params:
        return prepared_df

    normalized_filters = normalize_filter_params(**filter_params)
    normalized_column_settings = normalize_filter_column_settings(**(filter_column_settings or {}))
    filtered_df = prepared_df

    if "timestamp" in filtered_df.columns:
        from_boundary = _parse_filter_datetime(normalized_filters["date_from"])
        if from_boundary is not None:
            filtered_df = filtered_df[filtered_df["timestamp"] >= from_boundary]

        to_boundary = _parse_filter_datetime(normalized_filters["date_to"], is_end=True)
        if to_boundary is not None:
            filtered_df = filtered_df[filtered_df["timestamp"] < to_boundary]

    for filter_key in FILTER_SLOT_KEYS:
        filter_value = normalized_filters.get(filter_key)
        column_name = normalized_column_settings.get(filter_key, {}).get("column_name")
        if not filter_value or not column_name or column_name not in filtered_df.columns:
            continue

        # Phase 1 keeps matching event rows even if a case becomes partial after filtering.
        filtered_df = filtered_df[
            filtered_df[column_name].astype(str).str.strip() == filter_value
        ]

    activity_mode = normalized_filters.get("activity_mode")
    activity_values = [
        value.strip()
        for value in str(normalized_filters.get("activity_values") or "").split(",")
        if value.strip()
    ]
    if activity_mode in {"include", "exclude"} and activity_values and "activity" in filtered_df.columns:
        activity_mask = filtered_df["activity"].astype(str).str.strip().isin(activity_values)
        # Case-level filtering: keep all events of cases that contain (or don't contain) the target activity.
        matching_case_ids = filtered_df.loc[activity_mask, "case_id"].unique()
        case_mask = filtered_df["case_id"].isin(matching_case_ids)
        filtered_df = filtered_df[case_mask] if activity_mode == "include" else filtered_df[~case_mask]

    return filtered_df.copy()


def get_filter_options(prepared_df, filter_column_settings=None):
    normalized_column_settings = normalize_filter_column_settings(**(filter_column_settings or {}))
    filters = []

    for filter_key in FILTER_SLOT_KEYS:
        column_name = normalized_column_settings[filter_key]["column_name"]
        label = normalized_column_settings[filter_key]["label"]
        options = []

        if column_name and column_name in prepared_df.columns:
            values = (
                prepared_df[column_name]
                .dropna()
                .astype(str)
                .str.strip()
            )
            options = sorted(
                {
                    value
                    for value in values.tolist()
                    if value
                }
            )

        filters.append(
            {
                "slot": filter_key,
                "label": label,
                "column_name": column_name,
                "options": options,
            }
        )

    return {"filters": filters}


def create_log_diagnostics(
    raw_df,
    case_id_column=None,
    activity_column=None,
    timestamp_column=None,
    filter_column_settings=None,
    sample_limit=5,
    unique_limit=200,
):
    normalized_column_settings = normalize_filter_column_settings(**(filter_column_settings or {}))
    record_count = int(len(raw_df))
    # Duplicate rows are judged by full-row equality across all columns.
    duplicate_row_count = int(raw_df.duplicated(keep="first").sum()) if record_count else 0
    deduplicated_record_count = int(record_count - duplicate_row_count)
    diagnostics = {
        "record_count": record_count,
        "event_count": int(len(raw_df)),
        "case_count": None,
        "activity_type_count": None,
        "duplicate_row_count": duplicate_row_count,
        "duplicate_status": "あり" if duplicate_row_count > 0 else "なし",
        "deduplicated_record_count": deduplicated_record_count,
        "duplicate_rate": round((duplicate_row_count / record_count), 4) if record_count else 0.0,
        "time_range": None,
        "missing_counts": {
            "case_id": None,
            "activity": None,
            "timestamp": None,
        },
        "headers": [str(column_name) for column_name in raw_df.columns.tolist()],
        "columns": [],
        "filters": [],
    }

    if case_id_column and case_id_column in raw_df.columns:
        case_values = raw_df[case_id_column].replace("", pd.NA)
        diagnostics["case_count"] = int(case_values.dropna().astype(str).str.strip().replace("", pd.NA).dropna().nunique())
        diagnostics["missing_counts"]["case_id"] = int(case_values.isna().sum())

    if activity_column and activity_column in raw_df.columns:
        activity_values = raw_df[activity_column].replace("", pd.NA)
        diagnostics["activity_type_count"] = int(
            activity_values.dropna().astype(str).str.strip().replace("", pd.NA).dropna().nunique()
        )
        diagnostics["missing_counts"]["activity"] = int(activity_values.isna().sum())

    if timestamp_column and timestamp_column in raw_df.columns:
        raw_timestamps = raw_df[timestamp_column].replace("", pd.NA)
        diagnostics["missing_counts"]["timestamp"] = int(raw_timestamps.isna().sum())
        parsed_timestamps = pd.to_datetime(raw_timestamps, errors="coerce")
        valid_timestamps = parsed_timestamps.dropna()
        if not valid_timestamps.empty:
            diagnostics["time_range"] = {
                "min": valid_timestamps.min().isoformat(),
                "max": valid_timestamps.max().isoformat(),
            }

    for column_name in raw_df.columns.tolist():
        normalized_values = (
            raw_df[column_name]
            .dropna()
            .astype(str)
            .str.strip()
        )
        non_blank_values = [value for value in normalized_values.tolist() if value]
        unique_values = list(dict.fromkeys(non_blank_values))
        diagnostics["columns"].append(
            {
                "name": str(column_name),
                "sample_values": unique_values[:sample_limit],
                "unique_count": int(len(set(unique_values))),
                "preview_unique_values": unique_values[: min(unique_limit, sample_limit * 4)],
                "missing_count": int(raw_df[column_name].replace("", pd.NA).isna().sum()),
            }
        )

    filter_options = get_filter_options(
        raw_df,
        filter_column_settings=normalized_column_settings,
    )
    diagnostics["filters"] = filter_options["filters"]

    return diagnostics


# -----------------------------------------------------------------------------
# Root cause, bottleneck, and case trace helpers
# -----------------------------------------------------------------------------


def _build_case_attribute_duration_table(prepared_df, column_name):
    if prepared_df.empty or column_name not in prepared_df.columns:
        return pd.DataFrame(columns=["case_id", "value", "case_duration_sec"])

    case_value_df = (
        prepared_df[["case_id", "sequence_no", column_name]]
        .copy()
        .sort_values(["case_id", "sequence_no"])
    )
    case_value_df["value"] = (
        case_value_df[column_name]
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
    )
    case_value_df = (
        case_value_df.dropna(subset=["value"])
        .groupby("case_id", as_index=False)["value"]
        .first()
    )

    if case_value_df.empty:
        return pd.DataFrame(columns=["case_id", "value", "case_duration_sec"])

    case_duration_df = (
        prepared_df.groupby("case_id", as_index=False)
        .agg(case_duration_sec=("duration_sec", "sum"))
    )

    return case_value_df.merge(case_duration_df, on="case_id", how="inner")


def build_root_cause_group_rows(prepared_df, column_name, total_case_count, limit=10):
    case_attribute_duration_df = _build_case_attribute_duration_table(prepared_df, column_name)

    if case_attribute_duration_df.empty:
        return {
            "total_value_count": 0,
            "returned_value_count": 0,
            "rows": [],
        }

    summary_df = (
        case_attribute_duration_df.groupby("value", as_index=False)
        .agg(
            case_count=("case_id", "nunique"),
            avg_case_duration_sec=("case_duration_sec", "mean"),
            median_case_duration_sec=("case_duration_sec", "median"),
            max_case_duration_sec=("case_duration_sec", "max"),
        )
    )
    summary_df["case_ratio_pct"] = (
        summary_df["case_count"] / total_case_count * 100
        if total_case_count
        else 0.0
    )
    summary_df = summary_df.sort_values(
        ["avg_case_duration_sec", "case_count", "value"],
        ascending=[False, False, True],
    ).reset_index(drop=True)

    total_value_count = int(len(summary_df))
    if limit is not None:
        summary_df = summary_df.head(max(0, int(limit))).reset_index(drop=True)

    numeric_columns = [
        "avg_case_duration_sec",
        "median_case_duration_sec",
        "max_case_duration_sec",
        "case_ratio_pct",
    ]
    summary_df[numeric_columns] = summary_df[numeric_columns].round(2)

    return {
        "total_value_count": total_value_count,
        "returned_value_count": int(len(summary_df)),
        "rows": [
            {
                "value": row["value"],
                "case_count": int(row["case_count"]),
                "case_ratio_pct": float(row["case_ratio_pct"]),
                "avg_case_duration_sec": float(row["avg_case_duration_sec"]),
                "avg_case_duration_text": _format_duration_text(row["avg_case_duration_sec"]),
                "median_case_duration_sec": float(row["median_case_duration_sec"]),
                "median_case_duration_text": _format_duration_text(row["median_case_duration_sec"]),
                "max_case_duration_sec": float(row["max_case_duration_sec"]),
                "max_case_duration_text": _format_duration_text(row["max_case_duration_sec"]),
            }
            for row in summary_df.to_dict(orient="records")
        ],
    }


def create_root_cause_summary(prepared_df, filter_column_settings=None, limit=10):
    normalized_column_settings = normalize_filter_column_settings(**(filter_column_settings or {}))
    total_case_count = int(prepared_df["case_id"].nunique()) if not prepared_df.empty else 0
    groups = []

    for filter_key in FILTER_SLOT_KEYS:
        filter_setting = normalized_column_settings.get(filter_key, {})
        column_name = filter_setting.get("column_name")
        if not column_name:
            continue

        group_rows_payload = build_root_cause_group_rows(
            prepared_df,
            column_name=column_name,
            total_case_count=total_case_count,
            limit=limit,
        )
        groups.append(
            {
                "slot": filter_key,
                "label": filter_setting.get("label") or DEFAULT_FILTER_LABELS[filter_key],
                "column_name": column_name,
                "total_value_count": group_rows_payload["total_value_count"],
                "returned_value_count": group_rows_payload["returned_value_count"],
                "rows": group_rows_payload["rows"],
            }
        )

    return {
        "has_data": not prepared_df.empty,
        "configured_group_count": len(groups),
        "groups": groups,
    }

def filter_prepared_df_by_pattern(prepared_df, pattern):
    case_pattern_df = build_case_pattern_table(prepared_df)
    matched_case_ids = case_pattern_df.loc[case_pattern_df["pattern"] == pattern, "case_id"]

    if matched_case_ids.empty:
        return prepared_df.iloc[0:0].copy()

    return prepared_df[prepared_df["case_id"].isin(matched_case_ids)].copy()


def build_duration_interval_table(prepared_df):
    interval_df = prepared_df.sort_values(["case_id", "sequence_no"]).copy()
    interval_df["next_activity"] = interval_df.groupby("case_id")["activity"].shift(-1)
    interval_df = interval_df[interval_df["next_activity"].notna()].copy()
    interval_df["transition_key"] = (
        interval_df["activity"].astype(str)
        + "__TO__"
        + interval_df["next_activity"].astype(str)
    )
    return interval_df


def _append_duration_metrics(summary_df):
    duration_metric_pairs = (
        ("avg_duration_sec", "avg_duration_hours"),
        ("median_duration_sec", "median_duration_hours"),
        ("max_duration_sec", "max_duration_hours"),
    )

    for duration_sec_column, duration_hour_column in duration_metric_pairs:
        summary_df[duration_hour_column] = summary_df[duration_sec_column] / 3600

    return summary_df


def build_transition_impact_rows(prepared_df, limit=None):
    interval_df = build_duration_interval_table(prepared_df)

    if interval_df.empty:
        return {
            "total_transition_count": 0,
            "returned_transition_count": 0,
            "rows": [],
        }

    transition_summary_df = (
        interval_df.groupby(["activity", "next_activity", "transition_key"])
        .agg(
            count=("case_id", "count"),
            case_count=("case_id", "nunique"),
            avg_duration_sec=("duration_sec", "mean"),
            max_duration_sec=("duration_sec", "max"),
            total_duration_sec=("duration_sec", "sum"),
        )
        .reset_index()
        .rename(
            columns={
                "activity": "from_activity",
                "next_activity": "to_activity",
            }
        )
    )

    total_wait_sec = float(transition_summary_df["total_duration_sec"].sum())
    transition_summary_df["wait_share_pct"] = (
        (transition_summary_df["total_duration_sec"] / total_wait_sec) * 100
        if total_wait_sec > 0
        else 0.0
    )
    transition_summary_df["impact_score"] = (
        transition_summary_df["avg_duration_sec"] * transition_summary_df["case_count"]
    )
    total_impact_score = float(transition_summary_df["impact_score"].sum())
    transition_summary_df["impact_share_pct"] = (
        (transition_summary_df["impact_score"] / total_impact_score) * 100
        if total_impact_score > 0
        else 0.0
    )
    transition_summary_df["transition_label"] = (
        transition_summary_df["from_activity"].astype(str)
        + f" {FLOW_PATH_SEPARATOR} "
        + transition_summary_df["to_activity"].astype(str)
    )
    transition_summary_df = transition_summary_df.sort_values(
        ["impact_score", "avg_duration_sec", "case_count", "transition_label"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)

    total_transition_count = int(len(transition_summary_df))

    if limit is not None:
        transition_summary_df = transition_summary_df.head(max(0, int(limit))).reset_index(drop=True)

    numeric_columns = [
        "avg_duration_sec",
        "max_duration_sec",
        "wait_share_pct",
        "impact_score",
        "impact_share_pct",
    ]
    transition_summary_df[numeric_columns] = transition_summary_df[numeric_columns].round(2)

    rows = []
    for index, row in enumerate(transition_summary_df.to_dict(orient="records"), start=1):
        rows.append(
            {
                "rank": index,
                "transition_label": row["transition_label"],
                "from_activity": row["from_activity"],
                "to_activity": row["to_activity"],
                "transition_key": row["transition_key"],
                "count": int(row["count"]),
                "case_count": int(row["case_count"]),
                "avg_duration_sec": float(row["avg_duration_sec"]),
                "avg_duration_text": _format_duration_text(row["avg_duration_sec"]),
                "max_duration_sec": float(row["max_duration_sec"]),
                "max_duration_text": _format_duration_text(row["max_duration_sec"]),
                "wait_share_pct": float(row["wait_share_pct"]),
                "impact_score": float(row["impact_score"]),
                "impact_share_pct": float(row["impact_share_pct"]),
            }
        )

    return {
        "total_transition_count": total_transition_count,
        "returned_transition_count": len(rows),
        "rows": rows,
    }


def create_impact_summary(prepared_df, limit=None):
    impact_rows_payload = build_transition_impact_rows(prepared_df, limit=limit)
    return {
        "has_data": not prepared_df.empty,
        "total_transition_count": impact_rows_payload["total_transition_count"],
        "returned_transition_count": impact_rows_payload["returned_transition_count"],
        "rows": impact_rows_payload["rows"],
    }


def _finalize_bottleneck_rows(summary_df, key_columns, limit=None):
    if summary_df.empty:
        return []

    summary_df = _append_duration_metrics(summary_df)
    summary_df = summary_df.sort_values(
        ["avg_duration_sec", "median_duration_sec", "max_duration_sec", "count", *key_columns],
        ascending=[False, False, False, False, *([True] * len(key_columns))],
    ).reset_index(drop=True)

    if limit is not None:
        summary_df = summary_df.head(max(0, int(limit))).reset_index(drop=True)

    numeric_columns = [
        "avg_duration_sec",
        "median_duration_sec",
        "max_duration_sec",
        "avg_duration_hours",
        "median_duration_hours",
        "max_duration_hours",
    ]
    summary_df[numeric_columns] = summary_df[numeric_columns].round(2)

    return [
        {
            **{
                key_column: row[key_column]
                for key_column in key_columns
            },
            "count": int(row["count"]),
            "case_count": int(row["case_count"]),
            "avg_duration_sec": float(row["avg_duration_sec"]),
            "median_duration_sec": float(row["median_duration_sec"]),
            "max_duration_sec": float(row["max_duration_sec"]),
            "avg_duration_hours": float(row["avg_duration_hours"]),
            "median_duration_hours": float(row["median_duration_hours"]),
            "max_duration_hours": float(row["max_duration_hours"]),
        }
        for row in summary_df.to_dict(orient="records")
    ]


def _build_heatmap(items, key_name):
    max_avg_duration_sec = max(
        (float(item["avg_duration_sec"]) for item in items),
        default=0.0,
    )
    heatmap = {}

    for item in items:
        heat_score = (
            float(item["avg_duration_sec"]) / max_avg_duration_sec
            if max_avg_duration_sec > 0
            else 0.0
        )
        heat_score = round(max(0.0, min(1.0, heat_score)), 4)

        if heat_score <= 0.2:
            heat_level = 1
        elif heat_score <= 0.4:
            heat_level = 2
        elif heat_score <= 0.6:
            heat_level = 3
        elif heat_score <= 0.8:
            heat_level = 4
        else:
            heat_level = 5
        heatmap[item[key_name]] = {
            "avg_duration_sec": float(item["avg_duration_sec"]),
            "avg_duration_hours": float(item["avg_duration_hours"]),
            "heat_score": heat_score,
            "heat_level": heat_level,
            "heat_class": f"heat-{heat_level}",
        }

    return heatmap


def _format_duration_text(duration_sec):
    total_seconds = max(0, int(round(float(duration_sec or 0))))
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts = []

    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def create_dashboard_summary(prepared_df, variant_items=None, bottleneck_summary=None, coverage_limit=10):
    total_records = int(len(prepared_df))
    total_cases = int(prepared_df["case_id"].nunique()) if total_records else 0
    activity_type_count = 0

    if total_records and "activity" in prepared_df.columns:
        activity_values = (
            prepared_df["activity"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
        )
        activity_type_count = int(activity_values.nunique())

    if total_cases:
        case_duration_series = (
            prepared_df.groupby("case_id")["duration_sec"]
            .sum()
            .astype(float)
        )
        avg_case_duration_sec = round(float(case_duration_series.mean()), 2)
        median_case_duration_sec = round(float(case_duration_series.median()), 2)
        max_case_duration_sec = round(float(case_duration_series.max()), 2)
    else:
        avg_case_duration_sec = 0.0
        median_case_duration_sec = 0.0
        max_case_duration_sec = 0.0

    top_variant_items = list(variant_items or [])[: max(0, int(coverage_limit))]
    if variant_items is None:
        top_variant_items = create_variant_summary(prepared_df, limit=coverage_limit)

    covered_case_count = sum(int(variant_item["count"]) for variant_item in top_variant_items)
    top10_variant_coverage_ratio = round(
        covered_case_count / total_cases,
        4,
    ) if total_cases else 0.0

    resolved_bottleneck_summary = bottleneck_summary or create_bottleneck_summary(prepared_df, limit=1)
    top_transition_bottleneck = (
        resolved_bottleneck_summary["transition_bottlenecks"][0]
        if resolved_bottleneck_summary["transition_bottlenecks"]
        else None
    )
    top_bottleneck_avg_wait_sec = (
        float(top_transition_bottleneck["avg_duration_sec"])
        if top_transition_bottleneck
        else 0.0
    )

    return {
        "has_data": total_records > 0,
        "total_cases": total_cases,
        "total_records": total_records,
        "activity_type_count": activity_type_count,
        "avg_case_duration_sec": avg_case_duration_sec,
        "avg_case_duration_text": _format_duration_text(avg_case_duration_sec),
        "median_case_duration_sec": median_case_duration_sec,
        "median_case_duration_text": _format_duration_text(median_case_duration_sec),
        "max_case_duration_sec": max_case_duration_sec,
        "max_case_duration_text": _format_duration_text(max_case_duration_sec),
        "top10_variant_coverage_ratio": top10_variant_coverage_ratio,
        "top10_variant_coverage_pct": round(top10_variant_coverage_ratio * 100, 2),
        "top_bottleneck_transition_label": (
            f"{top_transition_bottleneck['from_activity']} {FLOW_PATH_SEPARATOR} {top_transition_bottleneck['to_activity']}"
            if top_transition_bottleneck
            else ""
        ),
        "top_bottleneck_avg_wait_sec": top_bottleneck_avg_wait_sec,
        "top_bottleneck_avg_wait_hours": round(top_bottleneck_avg_wait_sec / 3600, 2)
        if top_transition_bottleneck
        else 0.0,
        "top_bottleneck_avg_wait_text": (
            _format_duration_text(top_bottleneck_avg_wait_sec)
            if top_transition_bottleneck
            else ""
        ),
    }


def _append_insight(items, max_items, insight_id, text, source_keys):
    if not text or len(items) >= max_items:
        return
    items.append(
        {
            "id": insight_id,
            "text": text,
            "source_keys": list(source_keys),
        }
    )


def _collect_attention_activities(prepared_df):
    if prepared_df.empty or "activity" not in prepared_df.columns:
        return []

    activity_values = [
        str(activity_name).strip()
        for activity_name in prepared_df["activity"].dropna().tolist()
        if str(activity_name).strip()
    ]
    attention_activities = []
    for activity_name in activity_values:
        if (
            any(keyword in activity_name for keyword in INSIGHT_ATTENTION_ACTIVITY_KEYWORDS)
            and activity_name not in attention_activities
        ):
            attention_activities.append(activity_name)

    return attention_activities


def _build_frequency_insights(items, analysis_rows, max_items):
    if not analysis_rows:
        return

    top_activity_row = analysis_rows[0]
    top_activity = str(top_activity_row.get(FLOW_FREQUENCY_ACTIVITY_COLUMN) or "").strip()
    top_event_count = int(top_activity_row.get(FLOW_FREQUENCY_EVENT_COUNT_COLUMN) or 0)
    top_ratio = float(top_activity_row.get(FLOW_FREQUENCY_RATIO_COLUMN) or 0.0)
    _append_insight(
        items,
        max_items,
        "top_activity",
        f"最も件数が多い activity は「{top_activity}」で、{top_event_count:,} 件（全イベントの {top_ratio:.1f}%）を占めています。",
        ["analysis"],
    )

    top3_ratio = sum(float(row.get(FLOW_FREQUENCY_RATIO_COLUMN) or 0.0) for row in analysis_rows[:3])
    _append_insight(
        items,
        max_items,
        "event_distribution",
        f"上位3 activity で全イベントの {top3_ratio:.1f}% を占めており、イベント分布の偏りを確認できます。",
        ["analysis"],
    )

    if top_ratio >= 40.0:
        _append_insight(
            items,
            max_items,
            "activity_concentration",
            f"「{top_activity}」への集中度が高く、単一 activity への依存が大きい構成です。",
            ["analysis"],
        )
        return

    longest_avg_row = max(
        analysis_rows,
        key=lambda row: float(row.get(FLOW_FREQUENCY_AVG_DURATION_COLUMN) or 0.0),
    )
    longest_activity = str(longest_avg_row.get(FLOW_FREQUENCY_ACTIVITY_COLUMN) or "").strip()
    longest_avg_duration = float(longest_avg_row.get(FLOW_FREQUENCY_AVG_DURATION_COLUMN) or 0.0)
    _append_insight(
        items,
        max_items,
        "slowest_activity",
        f"平均処理時間が最も長い activity は「{longest_activity}」で、平均 {longest_avg_duration:.2f} 分です。",
        ["analysis"],
    )


def _build_transition_insights(items, analysis_rows, max_items):
    if not analysis_rows:
        return

    top_transition_row = analysis_rows[0]
    top_transition_label = (
        f"{top_transition_row.get(FLOW_TRANSITION_FROM_COLUMN, '')} {FLOW_PATH_SEPARATOR} "
        f"{top_transition_row.get(FLOW_TRANSITION_TO_COLUMN, '')}"
    ).strip()
    top_transition_ratio = float(top_transition_row.get(FLOW_TRANSITION_RATIO_COLUMN) or 0.0)
    _append_insight(
        items,
        max_items,
        "top_transition",
        f"最も多い前後関係は「{top_transition_label}」で、全遷移の {top_transition_ratio:.1f}% を占めています。",
        ["analysis"],
    )

    loop_row = next(
        (
            row for row in analysis_rows
            if str(row.get(FLOW_TRANSITION_FROM_COLUMN) or "").strip()
            == str(row.get(FLOW_TRANSITION_TO_COLUMN) or "").strip()
            and str(row.get(FLOW_TRANSITION_FROM_COLUMN) or "").strip()
        ),
        None,
    )
    if loop_row:
        activity_name = str(loop_row.get(FLOW_TRANSITION_FROM_COLUMN) or "").strip()
        _append_insight(
            items,
            max_items,
            "loop_transition",
            f"「{activity_name}」の自己遷移が見つかっており、ループが発生している可能性があります。",
            ["analysis"],
        )
    else:
        return_like_row = next(
            (
                row for row in analysis_rows
                if any(
                    keyword in str(row.get(FLOW_TRANSITION_FROM_COLUMN) or "").strip()
                    or keyword in str(row.get(FLOW_TRANSITION_TO_COLUMN) or "").strip()
                    for keyword in INSIGHT_ATTENTION_ACTIVITY_KEYWORDS
                )
            ),
            None,
        )
        if return_like_row:
            transition_label = (
                f"{return_like_row.get(FLOW_TRANSITION_FROM_COLUMN, '')} {FLOW_PATH_SEPARATOR} "
                f"{return_like_row.get(FLOW_TRANSITION_TO_COLUMN, '')}"
            ).strip()
            _append_insight(
                items,
                max_items,
                "return_transition",
                f"「{transition_label}」が含まれており、差戻しや再提出を含む前後関係が見られます。",
                ["analysis"],
            )

    longest_wait_row = max(
        analysis_rows,
        key=lambda row: float(row.get(FLOW_TRANSITION_AVG_WAIT_COLUMN) or 0.0),
    )
    longest_wait_value = float(longest_wait_row.get(FLOW_TRANSITION_AVG_WAIT_COLUMN) or 0.0)
    if longest_wait_value > 0:
        transition_label = (
            f"{longest_wait_row.get(FLOW_TRANSITION_FROM_COLUMN, '')} {FLOW_PATH_SEPARATOR} "
            f"{longest_wait_row.get(FLOW_TRANSITION_TO_COLUMN, '')}"
        ).strip()
        _append_insight(
            items,
            max_items,
            "slowest_transition_wait",
            f"平均待ち時間が最も長い遷移は「{transition_label}」で、平均 {longest_wait_value:.2f} 分です。",
            ["analysis"],
        )


def _build_pattern_insights(items, analysis_rows, max_items):
    if not analysis_rows:
        return

    top_pattern_row = analysis_rows[0]
    top_pattern = str(top_pattern_row.get(FLOW_PATTERN_COLUMN) or "").strip()
    top_pattern_case_count = int(top_pattern_row.get(FLOW_PATTERN_CASE_COUNT_COLUMN) or 0)
    top_pattern_ratio = float(top_pattern_row.get(FLOW_PATTERN_CASE_RATIO_COLUMN) or 0.0)
    _append_insight(
        items,
        max_items,
        "top_pattern",
        f"最頻出パターンは「{top_pattern}」で、{top_pattern_case_count:,} ケース（{top_pattern_ratio:.1f}%）です。",
        ["analysis"],
    )

    pattern_count = len(analysis_rows)
    if pattern_count >= 5:
        _append_insight(
            items,
            max_items,
            "pattern_variability",
            f"処理順パターンは {pattern_count} 種類あり、標準フローから外れる例外パターンが一定数存在します。",
            ["analysis"],
        )
    else:
        _append_insight(
            items,
            max_items,
            "pattern_variability",
            f"処理順パターンは {pattern_count} 種類に収まっており、比較的まとまったフロー構成です。",
            ["analysis"],
        )

    if top_pattern_ratio >= 60.0:
        _append_insight(
            items,
            max_items,
            "standard_vs_exception",
            "最頻出パターンの占有率が高く、標準フローが比較的明確です。",
            ["analysis"],
        )
        return

    longest_pattern_row = max(
        analysis_rows,
        key=lambda row: float(row.get(FLOW_PATTERN_AVG_CASE_DURATION_COLUMN) or 0.0),
    )
    longest_pattern = str(longest_pattern_row.get(FLOW_PATTERN_COLUMN) or "").strip()
    longest_pattern_duration = float(longest_pattern_row.get(FLOW_PATTERN_AVG_CASE_DURATION_COLUMN) or 0.0)
    _append_insight(
        items,
        max_items,
        "standard_vs_exception",
        f"最頻出パターンへの集中は限定的で、例外寄りの「{longest_pattern}」は平均 {longest_pattern_duration:.2f} 分かかっています。",
        ["analysis"],
    )


def create_rule_based_insights(
    prepared_df,
    analysis_key=None,
    analysis_rows=None,
    dashboard_summary=None,
    bottleneck_summary=None,
    impact_summary=None,
    max_items=5,
):
    safe_max_items = max(0, int(max_items or 0))
    resolved_dashboard_summary = dashboard_summary or create_dashboard_summary(prepared_df)
    resolved_bottleneck_summary = bottleneck_summary or create_bottleneck_summary(prepared_df, limit=10)
    resolved_impact_summary = impact_summary or create_impact_summary(prepared_df, limit=10)
    normalized_analysis_key = str(analysis_key or "").strip().lower()
    analysis_rows = list(analysis_rows or [])
    insights_payload = {
        "mode": "rule_based",
        "title": "自動インサイト",
        "description": "既存集計から重要ポイントを自動で要約しています。",
        "has_data": bool(resolved_dashboard_summary.get("has_data")),
        "items": [],
    }

    if safe_max_items == 0 or not insights_payload["has_data"]:
        return insights_payload

    _append_insight(
        insights_payload["items"],
        safe_max_items,
        "scope",
        f"対象は {int(resolved_dashboard_summary['total_cases']):,} ケース / {int(resolved_dashboard_summary['total_records']):,} イベントです。",
        ["dashboard"],
    )

    if normalized_analysis_key == "frequency":
        insights_payload["description"] = "頻度分析の上位 activity とイベント分布の偏りを要約しています。"
        _build_frequency_insights(insights_payload["items"], analysis_rows, safe_max_items)
    elif normalized_analysis_key == "transition":
        insights_payload["description"] = "前後処理分析から、ループ・差戻し・遷移の特徴を要約しています。"
        _build_transition_insights(insights_payload["items"], analysis_rows, safe_max_items)
    elif normalized_analysis_key == "pattern":
        insights_payload["description"] = "処理順パターン分析から、主要パターンと標準フロー / 例外の傾向を要約しています。"
        _build_pattern_insights(insights_payload["items"], analysis_rows, safe_max_items)
    else:
        top_impact_row = resolved_impact_summary["rows"][0] if resolved_impact_summary.get("rows") else None
        top_activity_bottleneck = (
            resolved_bottleneck_summary["activity_bottlenecks"][0]
            if resolved_bottleneck_summary.get("activity_bottlenecks")
            else None
        )
        top_transition_bottleneck = (
            resolved_bottleneck_summary["transition_bottlenecks"][0]
            if resolved_bottleneck_summary.get("transition_bottlenecks")
            else None
        )
        if top_impact_row:
            _append_insight(
                insights_payload["items"],
                safe_max_items,
                "top_impact_transition",
                (
                    f"改善インパクトが最大の遷移は「{str(top_impact_row['transition_label'])}」で、"
                    f"平均待ち時間 {top_impact_row['avg_duration_text']}、"
                    f"{int(top_impact_row['case_count']):,} ケースに発生しています。"
                ),
                ["impact"],
            )
        if top_activity_bottleneck:
            _append_insight(
                insights_payload["items"],
                safe_max_items,
                "top_activity_bottleneck",
                (
                    f"平均待ち時間が最大の Activity bottleneck は「{top_activity_bottleneck['activity']}」で、"
                    f"平均待ち時間 {_format_duration_text(top_activity_bottleneck['avg_duration_sec'])} です。"
                ),
                ["bottleneck"],
            )
        if top_transition_bottleneck:
            _append_insight(
                insights_payload["items"],
                safe_max_items,
                "top_transition_bottleneck",
                (
                    f"平均待ち時間が最大の Transition bottleneck は"
                    f"「{top_transition_bottleneck['from_activity']} {FLOW_PATH_SEPARATOR} {top_transition_bottleneck['to_activity']}」で、"
                    f"平均待ち時間 {_format_duration_text(top_transition_bottleneck['avg_duration_sec'])} です。"
                ),
                ["bottleneck"],
            )

    if len(insights_payload["items"]) < safe_max_items:
        _append_insight(
            insights_payload["items"],
            safe_max_items,
            "top10_variant_coverage",
            f"上位10 Variant で全ケースの {float(resolved_dashboard_summary['top10_variant_coverage_pct']):.1f}% をカバーしています。",
            ["dashboard", "variant"],
        )

    if len(insights_payload["items"]) < safe_max_items:
        attention_activities = _collect_attention_activities(prepared_df)
        if attention_activities:
            display_names = "、".join(attention_activities[:3])
            suffix = " など" if len(attention_activities) > 3 else ""
            _append_insight(
                insights_payload["items"],
                safe_max_items,
                "attention_activities",
                f"「{display_names}」{suffix} の activity が含まれており、差戻しや再提出が発生している可能性があります。",
                ["prepared_df"],
            )

    return insights_payload


def create_bottleneck_summary(prepared_df, limit=10):
    interval_df = build_duration_interval_table(prepared_df)

    if interval_df.empty:
        return {
            "activity_bottlenecks": [],
            "transition_bottlenecks": [],
            "activity_heatmap": {},
            "transition_heatmap": {},
        }

    activity_summary_df = (
        interval_df.groupby("activity")
        .agg(
            count=("case_id", "count"),
            case_count=("case_id", "nunique"),
            avg_duration_sec=("duration_sec", "mean"),
            median_duration_sec=("duration_sec", "median"),
            max_duration_sec=("duration_sec", "max"),
        )
        .reset_index()
    )

    transition_summary_df = (
        interval_df.groupby(["activity", "next_activity", "transition_key"])
        .agg(
            count=("case_id", "count"),
            case_count=("case_id", "nunique"),
            avg_duration_sec=("duration_sec", "mean"),
            median_duration_sec=("duration_sec", "median"),
            max_duration_sec=("duration_sec", "max"),
        )
        .reset_index()
        .rename(
            columns={
                "activity": "from_activity",
                "next_activity": "to_activity",
            }
        )
    )

    activity_bottlenecks = _finalize_bottleneck_rows(
        activity_summary_df,
        ["activity"],
        limit=limit,
    )
    transition_bottlenecks = _finalize_bottleneck_rows(
        transition_summary_df,
        ["from_activity", "to_activity", "transition_key"],
        limit=limit,
    )

    return {
        "activity_bottlenecks": activity_bottlenecks,
        "transition_bottlenecks": transition_bottlenecks,
        "activity_heatmap": _build_heatmap(activity_bottlenecks, "activity"),
        "transition_heatmap": _build_heatmap(transition_bottlenecks, "transition_key"),
    }


def create_transition_case_drilldown(
    prepared_df,
    from_activity,
    to_activity,
    limit=20,
):
    interval_df = build_duration_interval_table(prepared_df)
    filtered_df = interval_df[
        (interval_df["activity"] == from_activity)
        & (interval_df["next_activity"] == to_activity)
    ].copy()

    if filtered_df.empty:
        return []

    filtered_df = filtered_df.sort_values(
        ["duration_sec", "case_id", "start_time"],
        ascending=[False, True, True],
    ).reset_index(drop=True)

    if limit is not None:
        filtered_df = filtered_df.head(max(0, int(limit))).reset_index(drop=True)

    filtered_df["duration_sec"] = filtered_df["duration_sec"].round(2)
    return [
        {
            "case_id": row["case_id"],
            "duration_sec": float(row["duration_sec"]),
            "duration_text": _format_duration_text(row["duration_sec"]),
            "from_time": row["start_time"].isoformat(),
            "to_time": row["next_time"].isoformat(),
        }
        for row in filtered_df.to_dict(orient="records")
    ]


def create_activity_case_drilldown(
    prepared_df,
    activity,
    limit=20,
):
    interval_df = build_duration_interval_table(prepared_df)
    filtered_df = interval_df[interval_df["activity"] == activity].copy()

    if filtered_df.empty:
        return []

    filtered_df = filtered_df.sort_values(
        ["duration_sec", "case_id", "start_time"],
        ascending=[False, True, True],
    ).reset_index(drop=True)

    if limit is not None:
        filtered_df = filtered_df.head(max(0, int(limit))).reset_index(drop=True)

    filtered_df["duration_sec"] = filtered_df["duration_sec"].round(2)
    return [
        {
            "case_id": row["case_id"],
            "activity": row["activity"],
            "next_activity": row["next_activity"],
            "duration_sec": float(row["duration_sec"]),
            "duration_text": _format_duration_text(row["duration_sec"]),
            "from_time": row["start_time"].isoformat(),
            "to_time": row["next_time"].isoformat(),
        }
        for row in filtered_df.to_dict(orient="records")
    ]


def create_case_trace_details(prepared_df, case_id):
    normalized_case_id = str(case_id or "").strip()
    if not normalized_case_id:
        raise ValueError("Case ID is required.")

    case_df = prepared_df[prepared_df["case_id"] == normalized_case_id].copy()
    if case_df.empty:
        return {
            "case_id": normalized_case_id,
            "found": False,
            "summary": None,
            "events": [],
        }

    # Keep event order stable for timeline rendering.
    case_df = case_df.sort_values(["sequence_no", "start_time"]).reset_index(drop=True)
    case_df["next_activity"] = case_df["activity"].shift(-1)

    total_duration_sec = round(float(case_df["duration_sec"].sum()), 2)
    start_time = case_df["start_time"].min()
    end_time = case_df["next_time"].max()

    return {
        "case_id": normalized_case_id,
        "found": True,
        "summary": {
            "event_count": int(len(case_df)),
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "total_duration_sec": total_duration_sec,
            "total_duration_text": _format_duration_text(total_duration_sec),
        },
        "events": [
            {
                "sequence_no": int(row["sequence_no"]),
                "activity": row["activity"],
                "timestamp": row["start_time"].isoformat(),
                "next_activity": (
                    row["next_activity"]
                    if isinstance(row["next_activity"], str) and row["next_activity"]
                    else None
                ),
                "wait_to_next_sec": (
                    float(round(row["duration_sec"], 2))
                    if isinstance(row["next_activity"], str) and row["next_activity"]
                    else None
                ),
                "wait_to_next_text": (
                    _format_duration_text(row["duration_sec"])
                    if isinstance(row["next_activity"], str) and row["next_activity"]
                    else ""
                ),
            }
            for row in case_df.to_dict(orient="records")
        ],
    }


# -----------------------------------------------------------------------------
# Process map graph construction
# -----------------------------------------------------------------------------

def clamp_flow_percent(percent):
    try:
        numeric_percent = int(percent)
    except (TypeError, ValueError):
        numeric_percent = 0

    return max(0, min(100, numeric_percent))


def _calculate_flow_limit(total_count, percent, minimum=1):
    if total_count <= 0 or percent <= 0:
        return 0

    return min(total_count, max(minimum, math.ceil(total_count * (percent / 100))))


def _parse_pattern_steps(row):
    pattern = str(row.get(FLOW_PATTERN_COLUMN) or "").strip()
    if not pattern:
        return []

    return [
        step.strip()
        for step in pattern.split(FLOW_PATH_SEPARATOR)
        if step.strip()
    ]


def _get_transition_avg_duration_min(row):
    raw_value = (
        row.get(TRANSITION_ANALYSIS_CONFIG["display_columns"].get("avg_duration_min", ""))
        or row.get(FLOW_TRANSITION_AVG_WAIT_COLUMN)
        or row.get("avg_duration_min")
    )
    try:
        return round(float(raw_value or 0.0), 2)
    except (TypeError, ValueError):
        return 0.0


def _build_selected_pattern_prepared_df(prepared_df, selected_pattern_rows):
    if prepared_df is None or prepared_df.empty or not selected_pattern_rows:
        return prepared_df.iloc[0:0].copy() if prepared_df is not None else None

    selected_patterns = {
        str(row.get(FLOW_PATTERN_COLUMN) or "").strip()
        for row in selected_pattern_rows
        if str(row.get(FLOW_PATTERN_COLUMN) or "").strip()
    }
    if not selected_patterns:
        return prepared_df.iloc[0:0].copy()

    case_pattern_df = build_case_pattern_table(prepared_df)
    selected_case_ids = case_pattern_df.loc[
        case_pattern_df["pattern"].isin(selected_patterns),
        "case_id",
    ]
    if selected_case_ids.empty:
        return prepared_df.iloc[0:0].copy()

    return prepared_df[prepared_df["case_id"].isin(selected_case_ids)].copy()


def _build_flow_graph(pattern_rows, transition_rows=None, frequency_rows=None):
    transition_rows = transition_rows or []
    frequency_rows = frequency_rows or []
    node_map = {}
    edge_map = {}

    def ensure_node(name):
        node_name = str(name or "").strip()
        if not node_name:
            return None

        if node_name not in node_map:
            node_map[node_name] = {
                "name": node_name,
                "weight": 0,
                "caseWeight": 0,
                "positionTotal": 0,
                "positionWeight": 0,
                "incoming": 0,
                "outgoing": 0,
                "layerScore": 0,
                "layer": 0,
                "orderScore": 0,
            }

        return node_map[node_name]

    for row in frequency_rows:
        activity_name = str(row.get(FLOW_FREQUENCY_ACTIVITY_COLUMN) or "").strip()
        if not activity_name:
            continue

        node = ensure_node(activity_name)
        node["weight"] = max(node["weight"], int(row.get(FLOW_FREQUENCY_EVENT_COUNT_COLUMN) or 0))
        node["caseWeight"] = max(node["caseWeight"], int(row.get(FLOW_FREQUENCY_CASE_COUNT_COLUMN) or 0))

    for row in pattern_rows:
        case_count = int(row.get(FLOW_PATTERN_CASE_COUNT_COLUMN) or 0)
        steps = _parse_pattern_steps(row)

        for step_index, step in enumerate(steps):
            node = ensure_node(step)
            node["positionTotal"] += step_index * case_count
            node["positionWeight"] += case_count

            if node["weight"] == 0:
                node["weight"] = case_count

            if node["caseWeight"] == 0:
                node["caseWeight"] = case_count

            if step_index == len(steps) - 1:
                continue

            next_step = steps[step_index + 1]
            ensure_node(next_step)

            if transition_rows:
                continue

            edge_key = (step, next_step)
            if edge_key not in edge_map:
                edge_map[edge_key] = {
                    "source": step,
                    "target": next_step,
                    "count": 0,
                    "avg_duration_min": 0.0,
                    "avg_duration_sec": 0.0,
                    "avg_duration_text": "",
                }

            edge_map[edge_key]["count"] += case_count

    for row in transition_rows:
        source_name = str(row.get(FLOW_TRANSITION_FROM_COLUMN) or "").strip()
        target_name = str(row.get(FLOW_TRANSITION_TO_COLUMN) or "").strip()
        transition_count = int(row.get(FLOW_TRANSITION_COUNT_COLUMN) or 0)

        if not source_name or not target_name or transition_count <= 0:
            continue

        ensure_node(source_name)
        ensure_node(target_name)

        edge_key = (source_name, target_name)
        if edge_key not in edge_map:
            edge_map[edge_key] = {
                "source": source_name,
                "target": target_name,
                "count": 0,
                "avg_duration_min": 0.0,
                "avg_duration_sec": 0.0,
                "avg_duration_text": "",
            }

        edge_map[edge_key]["count"] = max(edge_map[edge_key]["count"], transition_count)
        avg_duration_min = _get_transition_avg_duration_min(row)
        edge_map[edge_key]["avg_duration_min"] = avg_duration_min
        edge_map[edge_key]["avg_duration_sec"] = round(avg_duration_min * 60, 2)
        edge_map[edge_key]["avg_duration_text"] = _format_duration_text(avg_duration_min * 60)

    nodes = list(node_map.values())
    edges = [
        edge
        for edge in edge_map.values()
        if edge["source"] != edge["target"] and edge["count"] > 0
    ]
    node_lookup = {node["name"]: node for node in nodes}

    for edge in edges:
        source_node = node_lookup.get(edge["source"])
        target_node = node_lookup.get(edge["target"])

        if source_node:
            source_node["outgoing"] += edge["count"]

        if target_node:
            target_node["incoming"] += edge["count"]

    for node in nodes:
        if node["positionWeight"] > 0:
            node["layerScore"] = node["positionTotal"] / node["positionWeight"]
        else:
            node["layerScore"] = 0

        node["layer"] = max(0, round(node["layerScore"]))

        if node["weight"] == 0:
            node["weight"] = max(node["incoming"], node["outgoing"], node["caseWeight"], 1)

        if node["caseWeight"] == 0:
            node["caseWeight"] = max(node["incoming"], node["outgoing"], node["weight"], 1)

    return _apply_flow_layout(nodes, edges)


def _filter_flow_graph(nodes, edges, activity_percent=100, connection_percent=100):
    total_node_count = len(nodes)
    total_edge_count = len(edges)

    if not total_node_count:
        return {
            "nodes": [],
            "edges": [],
            "available_activity_count": 0,
            "visible_activity_count": 0,
            "available_connection_count": total_edge_count,
            "visible_connection_count": 0,
        }

    requested_activity_percent = clamp_flow_percent(activity_percent)
    requested_connection_percent = clamp_flow_percent(connection_percent)
    activity_limit = _calculate_flow_limit(
        total_node_count,
        requested_activity_percent,
        minimum=2 if total_node_count > 1 else 1,
    )

    selected_nodes = sorted(
        nodes,
        key=lambda node: (-node["weight"], node["name"]),
    )[:activity_limit]
    selected_node_names = {node["name"] for node in selected_nodes}

    candidate_edges = [
        edge
        for edge in edges
        if edge["source"] in selected_node_names and edge["target"] in selected_node_names
    ]
    connection_limit = _calculate_flow_limit(
        len(candidate_edges),
        requested_connection_percent,
    )
    selected_edges = candidate_edges[:connection_limit]

    # Show all selected nodes regardless of whether they have visible edges.
    # This prevents the flow from going empty when the slider reduces activity_percent
    # to the point where selected nodes have no mutual edges.
    visible_nodes = [
        {
            **node,
        }
        for node in selected_nodes
    ]
    visible_edges = [
        {
            **edge,
        }
        for edge in selected_edges
    ]
    # Re-index orderScore to keep it compact for the subset, but keep layer/weight from parent
    nodes_by_layer = defaultdict(list)
    for n in visible_nodes:
        nodes_by_layer[n["layer"]].append(n)
    for layer in nodes_by_layer:
        nodes_by_layer[layer].sort(key=lambda x: (x.get("orderScore", 0), x["name"]))
        for i, n in enumerate(nodes_by_layer[layer]):
            n["orderScore"] = i

    return {
        "nodes": visible_nodes,
        "edges": visible_edges,
        "available_activity_count": total_node_count,
        "visible_activity_count": len(visible_nodes),
        "available_connection_count": total_edge_count,
        "visible_connection_count": len(visible_edges),
    }


def _reindex_layer_nodes(layer_nodes):
    for index, node in enumerate(layer_nodes):
        node["orderScore"] = index


def _count_edge_crossings(edges, node_lookup):
    crossing_score = 0

    for left_index, left_edge in enumerate(edges):
        left_source = node_lookup.get(left_edge["source"])
        left_target = node_lookup.get(left_edge["target"])

        if not left_source or not left_target:
            continue

        for right_edge in edges[left_index + 1:]:
            right_source = node_lookup.get(right_edge["source"])
            right_target = node_lookup.get(right_edge["target"])

            if not right_source or not right_target:
                continue

            source_diff = left_source["orderScore"] - right_source["orderScore"]
            target_diff = left_target["orderScore"] - right_target["orderScore"]

            if source_diff == 0 or target_diff == 0:
                continue

            if source_diff * target_diff < 0:
                crossing_score += min(left_edge["count"], right_edge["count"])

    return crossing_score


def _count_layer_crossings(layer, edges, node_lookup):
    outgoing_groups = defaultdict(list)
    incoming_groups = defaultdict(list)

    for edge in edges:
        source_node = node_lookup.get(edge["source"])
        target_node = node_lookup.get(edge["target"])

        if not source_node or not target_node:
            continue

        if source_node["layer"] == layer and target_node["layer"] > layer:
            outgoing_groups[target_node["layer"]].append(edge)

        if target_node["layer"] == layer and source_node["layer"] < layer:
            incoming_groups[source_node["layer"]].append(edge)

    return sum(
        _count_edge_crossings(group_edges, node_lookup)
        for group_edges in outgoing_groups.values()
    ) + sum(
        _count_edge_crossings(group_edges, node_lookup)
        for group_edges in incoming_groups.values()
    )


def _optimize_layer_by_swaps(layer_nodes, edges, node_lookup, max_swaps=100):
    if len(layer_nodes) < 2:
        return

    layer = layer_nodes[0]["layer"]
    updated = True
    swap_count = 0

    while updated and swap_count < max_swaps:
        updated = False

        for index in range(len(layer_nodes) - 1):
            current_score = _count_layer_crossings(layer, edges, node_lookup)
            first_node = layer_nodes[index]
            second_node = layer_nodes[index + 1]

            layer_nodes[index], layer_nodes[index + 1] = second_node, first_node
            _reindex_layer_nodes(layer_nodes)

            swapped_score = _count_layer_crossings(layer, edges, node_lookup)
            if swapped_score < current_score:
                updated = True
                swap_count += 1
                if swap_count >= max_swaps:
                    break
                continue

            layer_nodes[index], layer_nodes[index + 1] = first_node, second_node
            _reindex_layer_nodes(layer_nodes)


def _incoming_barycenter(node, edges, node_lookup):
    total_weight = 0
    total_score = 0

    for edge in edges:
        if edge["target"] != node["name"]:
            continue

        source_node = node_lookup.get(edge["source"])
        if not source_node or source_node["layer"] >= node["layer"]:
            continue

        distance = max(1, node["layer"] - source_node["layer"])
        weight = edge["count"] / distance
        total_weight += weight
        total_score += source_node["orderScore"] * weight

    if total_weight == 0:
        return node["orderScore"]

    return total_score / total_weight


def _outgoing_barycenter(node, edges, node_lookup):
    total_weight = 0
    total_score = 0

    for edge in edges:
        if edge["source"] != node["name"]:
            continue

        target_node = node_lookup.get(edge["target"])
        if not target_node or target_node["layer"] <= node["layer"]:
            continue

        distance = max(1, target_node["layer"] - node["layer"])
        weight = edge["count"] / distance
        total_weight += weight
        total_score += target_node["orderScore"] * weight

    if total_weight == 0:
        return node["orderScore"]

    return total_score / total_weight


def _apply_flow_layout(nodes, edges):
    if not nodes:
        return [], []

    edges = sorted(edges, key=lambda edge: (-edge["count"], edge["source"], edge["target"]))

    layer_values = sorted({node["layer"] for node in nodes})
    layer_map = {layer_value: index for index, layer_value in enumerate(layer_values)}
    nodes_by_layer = defaultdict(list)

    for node in nodes:
        node["layer"] = layer_map[node["layer"]]
        nodes_by_layer[node["layer"]].append(node)

    for layer in sorted(nodes_by_layer):
        nodes_by_layer[layer].sort(
            key=lambda node: (node["layerScore"], -node["weight"], node["name"])
        )
        _reindex_layer_nodes(nodes_by_layer[layer])

    node_lookup = {node["name"]: node for node in nodes}
    max_layer = max(nodes_by_layer) if nodes_by_layer else 0

    # Repeat the sweep so dense graphs keep a stable left-to-right order.
    for _ in range(FLOW_LAYOUT_SWEEP_ITERATIONS):
        for layer in range(1, max_layer + 1):
            layer_nodes = nodes_by_layer.get(layer, [])
            layer_nodes.sort(
                key=lambda node: (
                    _incoming_barycenter(node, edges, node_lookup),
                    -node["weight"],
                    node["name"],
                )
            )
            _reindex_layer_nodes(layer_nodes)

        for layer in range(max_layer - 1, -1, -1):
            layer_nodes = nodes_by_layer.get(layer, [])
            layer_nodes.sort(
                key=lambda node: (
                    _outgoing_barycenter(node, edges, node_lookup),
                    -node["weight"],
                    node["name"],
                )
            )
            _reindex_layer_nodes(layer_nodes)

    for layer in range(1, max_layer):
        # Dense graph safety: don't spend too much time on huge layers
        layer_nodes = nodes_by_layer.get(layer, [])
        if len(layer_nodes) > 50:
            continue
        _optimize_layer_by_swaps(layer_nodes, edges, node_lookup, max_swaps=50)

    ordered_nodes = []
    for layer in sorted(nodes_by_layer):
        ordered_nodes.extend(
            sorted(
                nodes_by_layer[layer],
                key=lambda node: (node["orderScore"], -node["weight"], node["name"]),
            )
        )

    return ordered_nodes, edges


# -----------------------------------------------------------------------------
# Process map, variant flow, and pattern detail snapshots
# -----------------------------------------------------------------------------

def create_pattern_flow_snapshot(
    pattern_rows,
    prepared_df=None,
    frequency_rows=None,
    pattern_percent=10,
    pattern_count=None,
    activity_percent=40,
    connection_percent=30,
    pattern_cap=FLOW_PATTERN_CAP,
):
    frequency_rows = frequency_rows or []
    cap = max(0, int(pattern_cap or 0))
    requested_pattern_percent = clamp_flow_percent(pattern_percent)
    requested_activity_percent = clamp_flow_percent(activity_percent)
    requested_connection_percent = clamp_flow_percent(connection_percent)
    sorted_pattern_rows = sorted(
        pattern_rows,
        key=lambda row: (
            -int(row.get(FLOW_PATTERN_CASE_COUNT_COLUMN) or 0),
            str(row.get(FLOW_PATTERN_COLUMN) or ""),
        ),
    )

    effective_pattern_count = min(len(sorted_pattern_rows), cap)
    requested_pattern_count = None if pattern_count is None else max(0, int(pattern_count or 0))
    if requested_pattern_count is None:
        used_pattern_count = _calculate_flow_limit(
            effective_pattern_count,
            requested_pattern_percent,
        )
    else:
        used_pattern_count = min(effective_pattern_count, requested_pattern_count)
    selected_pattern_rows = sorted_pattern_rows[:used_pattern_count]
    selected_frequency_rows = list(frequency_rows or [])
    selected_transition_rows = []

    if prepared_df is not None:
        selected_pattern_df = _build_selected_pattern_prepared_df(prepared_df, selected_pattern_rows)
        if selected_pattern_df is not None and not selected_pattern_df.empty:
            selected_transition_rows = convert_analysis_result_to_records(
                create_transition_analysis(selected_pattern_df),
                TRANSITION_ANALYSIS_CONFIG["display_columns"],
            )

    nodes, edges = _build_flow_graph(
        pattern_rows=selected_pattern_rows,
        transition_rows=selected_transition_rows,
        frequency_rows=selected_frequency_rows,
    )
    filtered_graph = _filter_flow_graph(
        nodes=nodes,
        edges=edges,
        activity_percent=requested_activity_percent,
        connection_percent=requested_connection_percent,
    )

    return {
        "pattern_window": {
            "requested_percent": requested_pattern_percent,
            "requested_count": requested_pattern_count,
            "total_pattern_count": len(sorted_pattern_rows),
            "effective_pattern_count": effective_pattern_count,
            "used_pattern_count": used_pattern_count,
            "cap": cap,
        },
        "activity_window": {
            "requested_percent": requested_activity_percent,
            "available_activity_count": filtered_graph["available_activity_count"],
            "visible_activity_count": filtered_graph["visible_activity_count"],
        },
        "connection_window": {
            "requested_percent": requested_connection_percent,
            "available_connection_count": filtered_graph["available_connection_count"],
            "visible_connection_count": filtered_graph["visible_connection_count"],
        },
        "flow_data": {
            "nodes": filtered_graph["nodes"],
            "edges": filtered_graph["edges"],
        },
    }


def create_variant_flow_snapshot(
    prepared_df,
    variant_pattern,
    activity_percent=100,
    connection_percent=100,
):
    filtered_df = filter_prepared_df_by_pattern(prepared_df, variant_pattern)

    if filtered_df.empty:
        raise ValueError("Variant was not found.")

    return create_pattern_flow_snapshot(
        pattern_rows=[
            {
                FLOW_PATTERN_CASE_COUNT_COLUMN: int(filtered_df["case_id"].nunique()),
                FLOW_PATTERN_COLUMN: variant_pattern,
            }
        ],
        prepared_df=filtered_df,
        pattern_percent=100,
        pattern_count=1,
        activity_percent=100,
        connection_percent=100,
        pattern_cap=1,
    )


def create_pattern_bottleneck_details(prepared_df, pattern):
    pattern_df = filter_prepared_df_by_pattern(prepared_df, pattern)

    if pattern_df.empty:
        raise ValueError("Pattern was not found.")

    pattern_df = pattern_df.sort_values(["case_id", "sequence_no"]).copy()
    transition_df = build_duration_interval_table(pattern_df)

    if transition_df.empty:
        step_metrics = []
        bottleneck_transition = None
    else:
        step_metrics_df = (
            transition_df.groupby(["sequence_no", "activity", "next_activity"])
            .agg(
                case_count=("case_id", "count"),
                avg_duration_min=("duration_min", "mean"),
                median_duration_min=("duration_min", "median"),
                min_duration_min=("duration_min", "min"),
                max_duration_min=("duration_min", "max"),
                total_duration_min=("duration_min", "sum"),
            )
            .reset_index()
            .sort_values(["sequence_no", "activity", "next_activity"])
            .reset_index(drop=True)
        )
        numeric_columns = [
            "avg_duration_min",
            "median_duration_min",
            "min_duration_min",
            "max_duration_min",
            "total_duration_min",
        ]
        step_metrics_df[numeric_columns] = step_metrics_df[numeric_columns].round(2)

        total_wait_min = step_metrics_df["total_duration_min"].sum()
        if total_wait_min > 0:
            step_metrics_df["wait_share_pct"] = (
                step_metrics_df["total_duration_min"] / total_wait_min * 100
            ).round(2)
        else:
            step_metrics_df["wait_share_pct"] = 0.0

        step_metrics_df["transition_label"] = (
            step_metrics_df["activity"] + " → " + step_metrics_df["next_activity"]
        )
        step_metrics = [
            {
                "sequence_no": int(row["sequence_no"]),
                "activity": row["activity"],
                "next_activity": row["next_activity"],
                "case_count": int(row["case_count"]),
                "avg_duration_min": float(row["avg_duration_min"]),
                "median_duration_min": float(row["median_duration_min"]),
                "min_duration_min": float(row["min_duration_min"]),
                "max_duration_min": float(row["max_duration_min"]),
                "total_duration_min": float(row["total_duration_min"]),
                "wait_share_pct": float(row["wait_share_pct"]),
                "transition_label": row["transition_label"],
                "transition_key": build_transition_key(row["activity"], row["next_activity"]),
            }
            for row in step_metrics_df.to_dict(orient="records")
        ]

        bottleneck_row = step_metrics_df.sort_values(
            [
                "avg_duration_min",
                "median_duration_min",
                "max_duration_min",
                "sequence_no",
            ],
            ascending=[False, False, False, True],
        ).iloc[0]
        bottleneck_transition = {
            "sequence_no": int(bottleneck_row["sequence_no"]),
            "from_activity": bottleneck_row["activity"],
            "to_activity": bottleneck_row["next_activity"],
            "transition_label": bottleneck_row["transition_label"],
            "transition_key": build_transition_key(
                bottleneck_row["activity"],
                bottleneck_row["next_activity"],
            ),
            "avg_duration_min": float(bottleneck_row["avg_duration_min"]),
            "median_duration_min": float(bottleneck_row["median_duration_min"]),
            "max_duration_min": float(bottleneck_row["max_duration_min"]),
            "wait_share_pct": float(bottleneck_row["wait_share_pct"]),
        }

    case_summary_df = (
        pattern_df.groupby("case_id")
        .agg(
            start_time=("start_time", "min"),
            end_time=("next_time", "max"),
            case_total_duration_min=("duration_min", "sum"),
        )
        .reset_index()
        .sort_values(["case_total_duration_min", "case_id"], ascending=[False, True])
        .reset_index(drop=True)
    )
    case_summary_df["case_total_duration_min"] = case_summary_df["case_total_duration_min"].round(2)

    total_case_count = prepared_df["case_id"].nunique()
    matched_case_count = int(case_summary_df["case_id"].nunique())

    return {
        "pattern": pattern,
        "pattern_steps": pattern.split(FLOW_PATH_SEPARATOR),
        "case_count": matched_case_count,
        "case_ratio_pct": round(matched_case_count / total_case_count * 100, 2),
        "avg_case_duration_min": round(float(case_summary_df["case_total_duration_min"].mean()), 2),
        "median_case_duration_min": round(float(case_summary_df["case_total_duration_min"].median()), 2),
        "min_case_duration_min": round(float(case_summary_df["case_total_duration_min"].min()), 2),
        "max_case_duration_min": round(float(case_summary_df["case_total_duration_min"].max()), 2),
        "bottleneck_transition": bottleneck_transition,
        "step_metrics": step_metrics,
        "case_examples": [
            {
                "case_id": row["case_id"],
                "start_time": row["start_time"].isoformat(),
                "end_time": row["end_time"].isoformat(),
                "case_total_duration_min": float(row["case_total_duration_min"]),
            }
            for row in case_summary_df.head(20).to_dict(orient="records")
        ],
    }


# -----------------------------------------------------------------------------
# Compatibility entry point
# -----------------------------------------------------------------------------

def analyze_event_log(
    file_source,
    case_id_column,
    activity_column,
    timestamp_column,
    selected_analysis_keys=None,
    output_root_dir=None,
    export_excel=False,
):
    prepared_df = load_prepared_event_log(
        file_source=file_source,
        case_id_column=case_id_column,
        activity_column=activity_column,
        timestamp_column=timestamp_column,
    )
    return analyze_prepared_event_log(
        prepared_df=prepared_df,
        selected_analysis_keys=selected_analysis_keys,
        output_root_dir=output_root_dir,
        export_excel=export_excel,
    )
