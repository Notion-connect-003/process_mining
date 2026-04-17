import pandas as pd

from core.analysis_constants import (
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
from core.analysis_core import (
    build_duration_interval_table,
    format_duration_text,
)

# -----------------------------------------------------------------------------
# Root cause, bottleneck, and case trace helpers
# -----------------------------------------------------------------------------


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
    # pandas版 fallback を削除。summary は呼び出し元が必ず渡す前提。
    resolved_dashboard_summary = dashboard_summary or {}
    resolved_bottleneck_summary = bottleneck_summary or {
        "activity_bottlenecks": [],
        "transition_bottlenecks": [],
        "activity_heatmap": {},
        "transition_heatmap": {},
    }
    resolved_impact_summary = impact_summary or {"has_data": False, "rows": []}
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
                    f"平均所要時間 {format_duration_text(top_activity_bottleneck['avg_duration_sec'])} です。"
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
                    f"平均所要時間 {format_duration_text(top_transition_bottleneck['avg_duration_sec'])} です。"
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
            "duration_text": format_duration_text(row["duration_sec"]),
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
            "duration_text": format_duration_text(row["duration_sec"]),
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
            "total_duration_text": format_duration_text(total_duration_sec),
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
                    format_duration_text(row["duration_sec"])
                    if isinstance(row["next_activity"], str) and row["next_activity"]
                    else ""
                ),
            }
            for row in case_df.to_dict(orient="records")
        ],
    }
