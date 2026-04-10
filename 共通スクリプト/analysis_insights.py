import pandas as pd

from 共通スクリプト.analysis_constants import (
    FILTER_SLOT_KEYS,
    DEFAULT_FILTER_LABELS,
    FLOW_PATH_SEPARATOR,
    FLOW_FREQUENCY_ACTIVITY_COLUMN,
    FLOW_FREQUENCY_EVENT_COUNT_COLUMN,
    FLOW_FREQUENCY_CASE_COUNT_COLUMN,
    FLOW_FREQUENCY_AVG_DURATION_COLUMN,
    FLOW_FREQUENCY_RATIO_COLUMN,
    FLOW_TRANSITION_FROM_COLUMN,
    FLOW_TRANSITION_TO_COLUMN,
    FLOW_TRANSITION_COUNT_COLUMN,
    FLOW_TRANSITION_AVG_WAIT_COLUMN,
    FLOW_TRANSITION_RATIO_COLUMN,
    FLOW_PATTERN_CASE_COUNT_COLUMN,
    FLOW_PATTERN_COLUMN,
    FLOW_PATTERN_CASE_RATIO_COLUMN,
    FLOW_PATTERN_AVG_CASE_DURATION_COLUMN,
    INSIGHT_ATTENTION_ACTIVITY_KEYWORDS,
)
from 共通スクリプト.analysis_core import (
    build_case_pattern_table,
    build_transition_key,
    create_variant_summary,
)
from 共通スクリプト.analysis_filters import normalize_filter_column_settings


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
    if prepared_df is None or prepared_df.empty or "activity" not in prepared_df.columns:
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
        f"最も件数が多いアクティビティは「{top_activity}」で、{top_event_count:,} 件（全イベントの {top_ratio:.1f}%）を占めています。",
        ["analysis"],
    )

    top3_ratio = sum(float(row.get(FLOW_FREQUENCY_RATIO_COLUMN) or 0.0) for row in analysis_rows[:3])
    _append_insight(
        items,
        max_items,
        "event_distribution",
        f"上位3アクティビティで全イベントの {top3_ratio:.1f}% を占めており、イベント分布の偏りを確認できます。",
        ["analysis"],
    )

    if top_ratio >= 40.0:
        _append_insight(
            items,
            max_items,
            "activity_concentration",
            f"「{top_activity}」への集中度が高く、単一アクティビティへの依存が大きい構成です。",
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
        f"平均処理時間が最も長いアクティビティは「{longest_activity}」で、平均 {longest_avg_duration:.2f} 分です。",
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
            f"平均所要時間が最も長い遷移は「{transition_label}」で、平均 {longest_wait_value:.2f} 分です。",
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
        insights_payload["description"] = "頻度分析の上位アクティビティとイベント分布の偏りを要約しています。"
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
                    f"平均所要時間 {top_impact_row['avg_duration_text']}、"
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
                    f"平均所要時間が最大のアクティビティボトルネックは「{top_activity_bottleneck['activity']}」で、"
                    f"平均所要時間 {_format_duration_text(top_activity_bottleneck['avg_duration_sec'])} です。"
                ),
                ["bottleneck"],
            )
        if top_transition_bottleneck:
            _append_insight(
                insights_payload["items"],
                safe_max_items,
                "top_transition_bottleneck",
                (
                    f"平均所要時間が最大の遷移ボトルネックは"
                    f"「{top_transition_bottleneck['from_activity']} {FLOW_PATH_SEPARATOR} {top_transition_bottleneck['to_activity']}」で、"
                    f"平均所要時間 {_format_duration_text(top_transition_bottleneck['avg_duration_sec'])} です。"
                ),
                ["bottleneck"],
            )

    if len(insights_payload["items"]) < safe_max_items:
        _append_insight(
            insights_payload["items"],
            safe_max_items,
            "top10_variant_coverage",
            f"上位10バリアントで全ケースの {float(resolved_dashboard_summary['top10_variant_coverage_pct']):.1f}% をカバーしています。",
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
                f"「{display_names}」{suffix} のアクティビティが含まれており、差戻しや再提出が発生している可能性があります。",
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
        raise ValueError("ケースIDが必要です。")

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
