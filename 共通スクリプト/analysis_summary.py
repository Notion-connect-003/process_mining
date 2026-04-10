import pandas as pd

from 共通スクリプト.analysis_constants import (
    DEFAULT_FILTER_LABELS,
    FILTER_SLOT_KEYS,
    FLOW_PATH_SEPARATOR,
)
from 共通スクリプト.analysis_core import (
    _append_duration_metrics,
    build_case_pattern_table,
    build_duration_interval_table,
    build_heatmap,
    build_transition_key,
    filter_prepared_df_by_pattern,
    format_duration_text,
)
from 共通スクリプト.analysis_filters import normalize_filter_column_settings


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
                "avg_case_duration_text": format_duration_text(row["avg_case_duration_sec"]),
                "median_case_duration_sec": float(row["median_case_duration_sec"]),
                "median_case_duration_text": format_duration_text(row["median_case_duration_sec"]),
                "max_case_duration_sec": float(row["max_case_duration_sec"]),
                "max_case_duration_text": format_duration_text(row["max_case_duration_sec"]),
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
                "avg_duration_text": format_duration_text(row["avg_duration_sec"]),
                "max_duration_sec": float(row["max_duration_sec"]),
                "max_duration_text": format_duration_text(row["max_duration_sec"]),
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
            **{key_column: row[key_column] for key_column in key_columns},
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
        "activity_heatmap": build_heatmap(activity_bottlenecks, "activity"),
        "transition_heatmap": build_heatmap(transition_bottlenecks, "transition_key"),
    }


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
        case_pattern_df = build_case_pattern_table(prepared_df)
        case_duration_series = (
            prepared_df.groupby("case_id")["duration_sec"]
            .sum()
            .astype(float)
        )
        avg_case_duration_sec = round(float(case_duration_series.mean()), 2)
        median_case_duration_sec = round(float(case_duration_series.median()), 2)
        max_case_duration_sec = round(float(case_duration_series.max()), 2)
    else:
        case_pattern_df = pd.DataFrame(columns=["case_id", "pattern"])
        avg_case_duration_sec = 0.0
        median_case_duration_sec = 0.0
        max_case_duration_sec = 0.0

    top_variant_items = list(variant_items or [])[: max(0, int(coverage_limit))]
    if variant_items is None:
        case_duration_df = (
            prepared_df.groupby("case_id", as_index=False)
            .agg(case_duration_sec=("duration_sec", "sum"))
        )
        case_pattern_df = case_pattern_df.merge(case_duration_df, on="case_id", how="left")
        variant_summary_df = (
            case_pattern_df.groupby("pattern", as_index=False)
            .agg(
                count=("case_id", "count"),
                avg_case_duration_sec=("case_duration_sec", "mean"),
            )
            .sort_values(["count", "pattern"], ascending=[False, True])
            .reset_index(drop=True)
        )
        if coverage_limit is not None:
            variant_summary_df = variant_summary_df.head(max(0, int(coverage_limit))).reset_index(drop=True)
        top_variant_items = [
            {
                "pattern": row["pattern"],
                "count": int(row["count"]),
                "avg_case_duration_sec": float(round(row["avg_case_duration_sec"], 2)),
            }
            for row in variant_summary_df.to_dict(orient="records")
        ]

    covered_case_count = sum(int(variant_item["count"]) for variant_item in top_variant_items)
    top10_variant_coverage_ratio = round(covered_case_count / total_cases, 4) if total_cases else 0.0

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
        "avg_case_duration_text": format_duration_text(avg_case_duration_sec),
        "median_case_duration_sec": median_case_duration_sec,
        "median_case_duration_text": format_duration_text(median_case_duration_sec),
        "max_case_duration_sec": max_case_duration_sec,
        "max_case_duration_text": format_duration_text(max_case_duration_sec),
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
            format_duration_text(top_bottleneck_avg_wait_sec)
            if top_transition_bottleneck
            else ""
        ),
    }


def create_pattern_bottleneck_details(prepared_df, pattern):
    pattern_df = filter_prepared_df_by_pattern(prepared_df, pattern)

    if pattern_df.empty:
        raise ValueError("パターンが見つかりません。")

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
            step_metrics_df["activity"] + f" {FLOW_PATH_SEPARATOR} " + step_metrics_df["next_activity"]
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
