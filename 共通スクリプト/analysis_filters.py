import pandas as pd

from 共通スクリプト.analysis_constants import (
    FILTER_SLOT_KEYS,
    FILTER_PARAM_KEYS,
    DEFAULT_FILTER_LABELS,
)


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
    start_activity_values=None,
    end_activity_values=None,
    **_,
):
    normalized_activity_mode = str(activity_mode or "").strip().lower()
    if normalized_activity_mode not in {"include", "exclude"}:
        normalized_activity_mode = None

    normalized_activity_values = _normalize_multi_value_param(activity_values)
    normalized_start_activity_values = _normalize_multi_value_param(start_activity_values)
    normalized_end_activity_values = _normalize_multi_value_param(end_activity_values)

    raw_params = {
        "date_from": date_from,
        "date_to": date_to,
        "filter_value_1": filter_value_1,
        "filter_value_2": filter_value_2,
        "filter_value_3": filter_value_3,
        "activity_mode": normalized_activity_mode,
        "activity_values": ",".join(normalized_activity_values) if normalized_activity_values else None,
        "start_activity_values": ",".join(normalized_start_activity_values) if normalized_start_activity_values else None,
        "end_activity_values": ",".join(normalized_end_activity_values) if normalized_end_activity_values else None,
    }

    return {
        filter_key: (str(filter_value).strip() if str(filter_value or "").strip() else None)
        for filter_key, filter_value in raw_params.items()
    }


def _normalize_multi_value_param(raw_value):
    if isinstance(raw_value, (list, tuple, set)):
        values = [str(value or "").strip() for value in raw_value]
    else:
        values = [value.strip() for value in str(raw_value or "").split(",")]
    return list(dict.fromkeys([value for value in values if value]))


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


def detect_group_columns(filter_params, filter_column_settings):
    """
    列が選択されているが値が未選択のスロットを「グルーピング軸」と判定する。
    返却値: [列名, ...] （①→②→③の順序を保持）
    """
    normalized_filters = normalize_filter_params(**(filter_params or {}))
    normalized_settings = normalize_filter_column_settings(**(filter_column_settings or {}))
    group_columns = []
    for slot in FILTER_SLOT_KEYS:
        col = normalized_settings.get(slot, {}).get("column_name")
        val = normalized_filters.get(slot)
        if col and not val:  # 列あり・値なし → グループ軸
            group_columns.append(col)
    return group_columns


def _build_case_duration_minutes(prepared_df):
    if prepared_df.empty or "case_id" not in prepared_df.columns:
        return pd.DataFrame(columns=["case_id", "case_duration_min"])

    if "duration_sec" in prepared_df.columns:
        case_duration_df = (
            prepared_df.groupby("case_id", as_index=False)
            .agg(case_duration_sec=("duration_sec", "sum"))
        )
        case_duration_df["case_duration_min"] = (
            case_duration_df["case_duration_sec"].astype(float) / 60
        ).round(2)
        return case_duration_df[["case_id", "case_duration_min"]]

    if {"start_time", "next_time"}.issubset(prepared_df.columns):
        case_duration_df = (
            prepared_df.groupby("case_id", as_index=False)
            .agg(
                start_time=("start_time", "min"),
                end_time=("next_time", "max"),
            )
        )
        case_duration_df["case_duration_min"] = (
            (case_duration_df["end_time"] - case_duration_df["start_time"]).dt.total_seconds() / 60
        ).round(2)
        return case_duration_df[["case_id", "case_duration_min"]]

    return pd.DataFrame(columns=["case_id", "case_duration_min"])


def _build_case_group_value_table(prepared_df, column_name):
    if prepared_df.empty or column_name not in prepared_df.columns:
        return pd.DataFrame(columns=["case_id", "value"])

    sort_columns = ["case_id"]
    if "sequence_no" in prepared_df.columns:
        sort_columns.append("sequence_no")
    elif "timestamp" in prepared_df.columns:
        sort_columns.append("timestamp")

    case_value_df = prepared_df[["case_id", column_name, *sort_columns[1:]]].copy()
    case_value_df = case_value_df.dropna(subset=[column_name])
    if case_value_df.empty:
        return pd.DataFrame(columns=["case_id", "value"])

    case_value_df[column_name] = case_value_df[column_name].astype(str).str.strip()
    case_value_df = case_value_df[case_value_df[column_name] != ""]
    if case_value_df.empty:
        return pd.DataFrame(columns=["case_id", "value"])

    case_value_df = (
        case_value_df.sort_values(sort_columns)
        .drop_duplicates(subset=["case_id"], keep="first")
        .rename(columns={column_name: "value"})
    )
    return case_value_df[["case_id", "value"]]


def _round_optional(value):
    if pd.isna(value):
        return None
    return round(float(value), 2)


def build_group_summary(prepared_df, group_columns):
    """
    グループ列ごとの集計サマリーを生成する。
    フロントエンドのKPIカード・タブ切り替えで使用。

    返却値:
    {
        "__meta__": {
            "total_case_count": int,
            "total_event_count": int,
            "avg_duration_min": float,
            "median_duration_min": float,
            "max_duration_min": float,
            "total_duration_min": float,
        },
        "column_name": {
            "value_1": {
                "case_count": int,
                "case_ratio_pct": float,
                "event_count": int,
                "event_ratio_pct": float,
                "avg_duration_min": float,
                "median_duration_min": float,
                "max_duration_min": float,
                "total_duration_min": float,
            },
            ...
        }
    }
    """
    if not group_columns:
        return {}

    total_case_count = int(prepared_df["case_id"].nunique()) if "case_id" in prepared_df.columns else 0
    total_event_count = int(len(prepared_df))
    case_duration_df = _build_case_duration_minutes(prepared_df)
    case_duration_series = case_duration_df["case_duration_min"].astype(float) if not case_duration_df.empty else pd.Series(dtype=float)

    summary = {}
    valid_columns = []
    for col in (group_columns or []):
        if col not in prepared_df.columns:
            continue
        valid_columns.append(col)

        event_counts_df = (
            prepared_df.groupby(col)
            .agg(
                event_count=("activity", "count"),
            )
            .reset_index()
            .rename(columns={col: "value"})
        )

        case_value_df = _build_case_group_value_table(prepared_df, col)
        case_stats_df = pd.DataFrame(columns=["value", "case_count", "avg_duration_min", "median_duration_min", "max_duration_min", "total_duration_min"])
        if not case_value_df.empty:
            case_stats_df = (
                case_value_df.merge(case_duration_df, on="case_id", how="left")
                .groupby("value", as_index=False)
                .agg(
                    case_count=("case_id", "nunique"),
                    avg_duration_min=("case_duration_min", "mean"),
                    median_duration_min=("case_duration_min", "median"),
                    max_duration_min=("case_duration_min", "max"),
                    total_duration_min=("case_duration_min", "sum"),
                )
            )

        grouped = event_counts_df.merge(case_stats_df, on="value", how="outer")
        grouped["event_count"] = grouped["event_count"].fillna(0).astype(int)
        grouped["case_count"] = grouped["case_count"].fillna(0).astype(int)
        grouped["case_ratio_pct"] = (
            grouped["case_count"] / total_case_count * 100
        ).round(2) if total_case_count else 0.0
        grouped["event_ratio_pct"] = (
            grouped["event_count"] / total_event_count * 100
        ).round(2) if total_event_count else 0.0

        summary[col] = {}
        for _, row in grouped.iterrows():
            entry = {
                "case_count": int(row["case_count"]),
                "case_ratio_pct": float(row["case_ratio_pct"]),
                "event_count": int(row["event_count"]),
                "event_ratio_pct": float(row["event_ratio_pct"]),
                "avg_duration_min": _round_optional(row.get("avg_duration_min")),
                "median_duration_min": _round_optional(row.get("median_duration_min")),
                "max_duration_min": _round_optional(row.get("max_duration_min")),
                "total_duration_min": _round_optional(row.get("total_duration_min")),
            }
            summary[col][str(row["value"])] = entry

    if not valid_columns:
        return {}

    summary["__meta__"] = {
        "total_case_count": total_case_count,
        "total_event_count": total_event_count,
        "avg_duration_min": round(float(case_duration_series.mean()), 2) if not case_duration_series.empty else 0.0,
        "median_duration_min": round(float(case_duration_series.median()), 2) if not case_duration_series.empty else 0.0,
        "max_duration_min": round(float(case_duration_series.max()), 2) if not case_duration_series.empty else 0.0,
        "total_duration_min": round(float(case_duration_series.sum()), 2) if not case_duration_series.empty else 0.0,
    }

    return summary


def filter_by_start_end_activity(
    df,
    case_id_column,
    activity_column,
    timestamp_column,
    start_activities=None,
    end_activities=None,
):
    normalized_start_activities = _normalize_multi_value_param(start_activities)
    normalized_end_activities = _normalize_multi_value_param(end_activities)

    if not normalized_start_activities and not normalized_end_activities:
        return df

    required_columns = [case_id_column, activity_column, timestamp_column]
    if df.empty or any(column_name not in df.columns for column_name in required_columns):
        return df

    sort_columns = [case_id_column, timestamp_column]
    if "sequence_no" in df.columns and "sequence_no" not in sort_columns:
        sort_columns.append("sequence_no")

    ranked_df = df.sort_values(sort_columns, kind="mergesort")
    valid_case_ids = set(ranked_df[case_id_column].dropna().unique().tolist())

    if normalized_start_activities:
        first_events = ranked_df.groupby(case_id_column, sort=False).first()
        start_case_ids = set(
            first_events[first_events[activity_column].astype(str).str.strip().isin(normalized_start_activities)].index.tolist()
        )
        valid_case_ids &= start_case_ids

    if normalized_end_activities:
        last_events = ranked_df.groupby(case_id_column, sort=False).last()
        end_case_ids = set(
            last_events[last_events[activity_column].astype(str).str.strip().isin(normalized_end_activities)].index.tolist()
        )
        valid_case_ids &= end_case_ids

    return df[df[case_id_column].isin(valid_case_ids)]


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

    filtered_df = filter_by_start_end_activity(
        filtered_df,
        "case_id",
        "activity",
        "timestamp",
        start_activities=normalized_filters.get("start_activity_values"),
        end_activities=normalized_filters.get("end_activity_values"),
    )

    activity_mode = normalized_filters.get("activity_mode")
    activity_values = _normalize_multi_value_param(normalized_filters.get("activity_values"))
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
    all_activity_names = []

    if "activity" in prepared_df.columns:
        activity_values = (
            prepared_df["activity"]
            .dropna()
            .astype(str)
            .str.strip()
        )
        all_activity_names = sorted(
            {
                value
                for value in activity_values.tolist()
                if value
            }
        )

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

    return {
        "filters": filters,
        "all_activity_names": all_activity_names,
    }


def create_log_diagnostics(
    raw_df,
    case_id_column=None,
    activity_column=None,
    timestamp_column=None,
    filter_column_settings=None,
    sample_limit=5,
    unique_limit=200,
):
    import pandas as pd
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
