from datetime import datetime, timezone
import json
import re

import httpx
import pandas as pd

from web_services.analysis_queries import (
    build_filter_cache_key,
    get_analysis_data,
    get_bottleneck_summary,
    get_dashboard_summary,
    get_impact_summary,
    get_root_cause_summary,
    get_rule_based_insights_summary,
    get_variant_items,
)
from web_reports.detail_report import build_transition_display_label
from web_reports.excel_common import REPORT_SHEET_NAMES, normalize_excel_cell_value
from web_services.llm_helpers import request_ollama_insights_text

from 共通スクリプト.analysis_service import get_available_analysis_definitions
from 共通スクリプト.duckdb_service import (
    query_bottleneck_summary,
    query_period_text,
)


def serialize_ai_prompt_rows(rows, max_items=5):
    serialized_rows = []

    for row in list(rows or [])[: max(0, int(max_items or 0))]:
        if isinstance(row, dict):
            serialized_rows.append(
                {
                    str(key): normalize_excel_cell_value(value)
                    for key, value in row.items()
                }
            )
        else:
            serialized_rows.append(normalize_excel_cell_value(row))

    return json.dumps(serialized_rows, ensure_ascii=False, indent=2)


def build_ai_recommended_actions(ai_context):
    """ルールベースで推奨アクションのリストを生成して返す。"""
    analysis_key = str(ai_context["analysis_key"]).strip().lower()
    bottleneck_summary = ai_context["bottleneck_summary"]
    analysis_rows = ai_context["analysis_rows"]
    impact_summary = ai_context["impact_summary"]

    top_activity_bottleneck = (
        bottleneck_summary["activity_bottlenecks"][0]
        if bottleneck_summary.get("activity_bottlenecks")
        else None
    )
    top_impact_row = impact_summary["rows"][0] if impact_summary.get("rows") else None

    actions = []

    if top_activity_bottleneck:
        activity_name = top_activity_bottleneck.get("activity", "\u4e0d\u660e")
        std_min = top_activity_bottleneck.get("std_duration_min", 0)
        avg_min = top_activity_bottleneck.get("avg_duration_min", 0)
        if std_min and avg_min and float(std_min) > float(avg_min) * 0.5:
            actions.append(
                f"\u300c{activity_name}\u300d\u306e\u51e6\u7406\u6642\u9593\u306b\u3070\u3089\u3064\u304d\u304c\u3042\u308a\u307e\u3059\u3002"
                f"\u30b1\u30fc\u30b9\u3054\u3068\u306e\u30c9\u30ea\u30eb\u30c0\u30a6\u30f3\u3067\u539f\u56e0\u3092\u78ba\u8a8d\u3059\u308b\u3053\u3068\u3092\u63a8\u5968\u3057\u307e\u3059\u3002"
            )

    if analysis_key == "frequency" and len(analysis_rows) >= 3:
        top3_ratio_sum = sum(
            float(row.get("\u30a4\u30d9\u30f3\u30c8\u6bd4\u7387(%)", row.get("event_ratio_pct", 0)) or 0)
            for row in analysis_rows[:3]
        )
        if top3_ratio_sum >= 80:
            actions.append(
                "\u4e0a\u4f4d\u30a2\u30af\u30c6\u30a3\u30d3\u30c6\u30a3\u3078\u306e\u96c6\u4e2d\u5ea6\u304c\u9ad8\u3044\u305f\u3081\u3001"
                "\u3053\u308c\u3089\u306e\u52b9\u7387\u5316\u304c\u5168\u4f53\u6539\u5584\u306b\u76f4\u7d50\u3057\u307e\u3059\u3002"
                "\u524d\u5f8c\u51e6\u7406\u5206\u6790\u3067\u9077\u79fb\u30d1\u30bf\u30fc\u30f3\u3092\u78ba\u8a8d\u3057\u3066\u304f\u3060\u3055\u3044\u3002"
            )

    if top_activity_bottleneck:
        activity_name = top_activity_bottleneck.get("activity", "\u4e0d\u660e")
        avg_min = float(top_activity_bottleneck.get("avg_duration_min", 0) or 0)
        dashboard_avg = float(
            ai_context["dashboard_summary"].get("avg_case_duration_min", 0) or 0
        )
        if dashboard_avg > 0 and avg_min >= dashboard_avg * 3:
            actions.append(
                f"\u300c{activity_name}\u300d\u306e\u51e6\u7406\u6642\u9593\u304c\u7a81\u51fa\u3057\u3066\u3044\u307e\u3059\u3002"
                f"\u5f53\u8a72\u30a2\u30af\u30c6\u30a3\u30d3\u30c6\u30a3\u306e\u30b1\u30fc\u30b9\u660e\u7d30\u3092\u78ba\u8a8d\u3057\u3001"
                f"\u5916\u308c\u5024\u3084\u7279\u6b8a\u30b1\u30fc\u30b9\u306e\u6709\u7121\u3092\u8abf\u67fb\u3057\u3066\u304f\u3060\u3055\u3044\u3002"
            )

    if top_impact_row and analysis_key != "frequency":
        transition_label = top_impact_row.get("transition_label", "\u4e0d\u660e")
        actions.append(
            f"\u6539\u5584\u30a4\u30f3\u30d1\u30af\u30c8\u304c\u9ad8\u3044\u9077\u79fb\u300c{transition_label}\u300d\u3092\u512a\u5148\u3057\u3066\u3001"
            f"\u627f\u8a8d\u5f85\u3061\u30fb\u5dee\u623b\u3057\u7b49\u306e\u5185\u8a33\u3092\u78ba\u8a8d\u3057\u3066\u304f\u3060\u3055\u3044\u3002"
        )

    group_summary = ai_context.get("group_summary", [])
    group_rows = [
        row
        for row in group_summary
        if str(row.get("value") or row.get("\u5024") or "") != "\u5168\u4f53"
    ]
    if len(group_rows) >= 2:
        value_key = next(
            (key for key in ["value", "\u5024"] if key in group_rows[0]),
            "value",
        )
        avg_duration_key = next(
            (
                key
                for key in ["avg_case_duration_min", "\u5e73\u5747\u51e6\u7406\u6642\u9593(\u5206)", "avg_duration_min"]
                if key in group_rows[0]
            ),
            None,
        )
        if avg_duration_key:
            sorted_groups = sorted(
                group_rows,
                key=lambda row: float(row.get(avg_duration_key, 0) or 0),
                reverse=True,
            )
            max_group = sorted_groups[0]
            min_group = sorted_groups[-1]
            max_dur = float(max_group.get(avg_duration_key, 0) or 0)
            min_dur = float(min_group.get(avg_duration_key, 0) or 0)
            if min_dur > 0 and max_dur / min_dur >= 1.3:
                actions.append(
                    f"\u30b0\u30eb\u30fc\u30d7\u300c{max_group.get(value_key, '\u4e0d\u660e')}\u300d\u306e\u5e73\u5747\u51e6\u7406\u6642\u9593\u304c"
                    f"\u4ed6\u30b0\u30eb\u30fc\u30d7\u3088\u308a\u9577\u3044\u305f\u3081\u3001\u5f53\u8a72\u30b0\u30eb\u30fc\u30d7\u306e\u30dc\u30c8\u30eb\u30cd\u30c3\u30af\u8981\u56e0\u3092"
                    f"\u30c9\u30ea\u30eb\u30c0\u30a6\u30f3\u3067\u78ba\u8a8d\u3057\u3066\u304f\u3060\u3055\u3044\u3002"
                )

    actions = actions[:3]

    if not actions:
        actions.append(
            "\u524d\u5f8c\u51e6\u7406\u5206\u6790\u3084\u30d0\u30ea\u30a2\u30f3\u30c8\u5206\u6790\u3067\u3001"
            "\u30d7\u30ed\u30bb\u30b9\u5168\u4f53\u306e\u6d41\u308c\u3092\u78ba\u8a8d\u3059\u308b\u3053\u3068\u3092\u63a8\u5968\u3057\u307e\u3059\u3002"
        )

    return actions

def _build_group_comparison_lines(group_summary):
    """group_summary からグループ間比較テキスト行を生成して返す。"""
    if not group_summary:
        return []

    group_rows = []
    for row in group_summary:
        value = row.get("value") or row.get("\u5024") or ""
        if str(value) == "\u5168\u4f53":
            continue
        group_rows.append(row)

    if len(group_rows) < 2:
        return []

    case_key = next(
        (key for key in ["case_count", "\u30b1\u30fc\u30b9\u6570"] if key in group_rows[0]),
        None,
    )
    avg_duration_key = next(
        (
            key
            for key in ["avg_case_duration_min", "\u5e73\u5747\u51e6\u7406\u6642\u9593(\u5206)", "avg_duration_min"]
            if key in group_rows[0]
        ),
        None,
    )
    value_key = next(
        (key for key in ["value", "\u5024"] if key in group_rows[0]),
        "value",
    )

    lines = [
        "",
        "【\u30b0\u30eb\u30fc\u30d7\u9593\u6bd4\u8f03】",
        f"{len(group_rows)}\u30b0\u30eb\u30fc\u30d7\u3092\u6bd4\u8f03\u3057\u305f\u7d50\u679c:",
    ]

    if case_key:
        sorted_by_case = sorted(
            group_rows,
            key=lambda row: float(row.get(case_key, 0) or 0),
            reverse=True,
        )
        max_group = sorted_by_case[0]
        min_group = sorted_by_case[-1]
        lines.append(
            f"- \u30b1\u30fc\u30b9\u6570\u304c\u6700\u3082\u591a\u3044\u306e\u306f\u300c{max_group.get(value_key, '\u4e0d\u660e')}\u300d"
            f"\uff08{int(float(max_group.get(case_key, 0) or 0)):,}\u4ef6\uff09\u3001"
            f"\u6700\u3082\u5c11\u306a\u3044\u306e\u306f\u300c{min_group.get(value_key, '\u4e0d\u660e')}\u300d"
            f"\uff08{int(float(min_group.get(case_key, 0) or 0)):,}\u4ef6\uff09\u3067\u3059\u3002"
        )

    if avg_duration_key:
        sorted_by_duration = sorted(
            group_rows,
            key=lambda row: float(row.get(avg_duration_key, 0) or 0),
            reverse=True,
        )
        max_dur_group = sorted_by_duration[0]
        min_dur_group = sorted_by_duration[-1]
        max_dur = float(max_dur_group.get(avg_duration_key, 0) or 0)
        min_dur = float(min_dur_group.get(avg_duration_key, 0) or 0)
        if max_dur > 0:
            lines.append(
                f"- \u5e73\u5747\u51e6\u7406\u6642\u9593\u304c\u6700\u3082\u9577\u3044\u30b0\u30eb\u30fc\u30d7\u306f\u300c{max_dur_group.get(value_key, '\u4e0d\u660e')}\u300d"
                f"\uff08{max_dur:.1f}\u5206\uff09\u3001\u6700\u3082\u77ed\u3044\u306e\u306f\u300c{min_dur_group.get(value_key, '\u4e0d\u660e')}\u300d"
                f"\uff08{min_dur:.1f}\u5206\uff09\u3067\u3059\u3002"
            )
            if min_dur > 0 and max_dur / min_dur >= 1.5:
                lines.append(
                    f"- \u4e21\u8005\u306e\u5dee\u306f\u7d04{max_dur - min_dur:.1f}\u5206\uff08{max_dur / min_dur:.1f}\u500d\uff09\u3042\u308a\u3001"
                    f"\u30b0\u30eb\u30fc\u30d7\u56fa\u6709\u306e\u8981\u56e0\u3092\u78ba\u8a8d\u3059\u308b\u3053\u3068\u3092\u63a8\u5968\u3057\u307e\u3059\u3002"
                )

    if not any(line.startswith("- ") for line in lines):
        lines.append("- \u30b0\u30eb\u30fc\u30d7\u9593\u3067\u9855\u8457\u306a\u5dee\u306f\u691c\u51fa\u3055\u308c\u307e\u305b\u3093\u3067\u3057\u305f\u3002")

    return lines

def build_ai_fallback_text(ai_context):
    analysis_key = str(ai_context["analysis_key"]).strip().lower()
    analysis_name = ai_context["analysis_name"]
    dashboard_summary = ai_context["dashboard_summary"]
    period_text = ai_context["period_text"]
    impact_summary = ai_context["impact_summary"]
    bottleneck_summary = ai_context["bottleneck_summary"]
    analysis_rows = ai_context["analysis_rows"]

    top_impact_row = impact_summary["rows"][0] if impact_summary.get("rows") else None
    top_transition_bottleneck = (
        bottleneck_summary["transition_bottlenecks"][0]
        if bottleneck_summary.get("transition_bottlenecks")
        else None
    )
    top_activity_bottleneck = (
        bottleneck_summary["activity_bottlenecks"][0]
        if bottleneck_summary.get("activity_bottlenecks")
        else None
    )
    top_transition_bottleneck_label = build_transition_display_label(
        top_transition_bottleneck
    )
    top_row = analysis_rows[0] if analysis_rows else {}
    group_comparison_lines = _build_group_comparison_lines(
        ai_context.get("group_summary", [])
    )

    if analysis_key == "frequency":
        top_activity_name = (
            top_row.get("アクティビティ")
            or top_row.get("アクティビティ名")
            or "不明"
        )
        top_event_count = top_row.get("イベント件数", top_row.get("イベント数", 0))
        if top_row:
            overall_trend_text = (
                f"最もイベント件数が多いアクティビティは「{top_activity_name}」"
                f"（{normalize_excel_cell_value(top_event_count)} 件）"
                f"で、比率 {normalize_excel_cell_value(top_row.get('イベント比率(%)', 0))}%を占めます。"
            )
        else:
            overall_trend_text = "対象データが少ないため、傾向の把握が困難です。"

        attention_lines = []
        if top_row:
            attention_lines.append(
                f"- 「{top_activity_name}」のイベント件数が突出"
                f"しており、業務負荷が集中している可能性があります。"
            )
        if top_activity_bottleneck:
            attention_lines.append(
                f"- 「{top_activity_bottleneck['activity']}」の処理時間が長く、"
                f"ボトルネックの可能性があります。"
            )
        if not attention_lines:
            attention_lines.append("- 特に顕著な偏りは検出されませんでした。")

        bottleneck_lines = []
        if top_activity_bottleneck:
            bottleneck_lines.append(
                f"- アクティビティ「{top_activity_bottleneck['activity']}」の"
                f"平均処理時間が他と比較して長く、"
                f"ケース明細のドリルダウンで確認を推奨します。"
            )
        if top_transition_bottleneck_label:
            bottleneck_lines.append(
                f"- 遷移「{top_transition_bottleneck_label}」の所要時間が長く、"
                f"前後アクティビティの関連を確認してください。"
            )
        if not bottleneck_lines:
            bottleneck_lines.append(
                "- 明確なボトルネックは検出されませんでした。"
                "前後処理分析で詳細を確認してください。"
            )

    elif analysis_key == "transition":
        if top_transition_bottleneck_label:
            overall_trend_text = (
                f"最もボトルネックとなっている遷移は「{top_transition_bottleneck_label}」です。"
            )
        else:
            overall_trend_text = "遷移間の所要時間に大きな偏りはありません。"

        attention_lines = []
        if top_transition_bottleneck_label:
            attention_lines.append(
                f"- 「{top_transition_bottleneck_label}」の遷移が"
                f"所要時間の観点で最も注目すべきポイントです。"
            )
        if top_impact_row:
            attention_lines.append(
                f"- 改善インパクトが最も高い遷移は「{top_impact_row['transition_label']}」で"
                f"優先的に確認が必要です。"
            )
        if not attention_lines:
            attention_lines.append("- 特に顕著なボトルネック遷移はありません。")

        bottleneck_lines = []
        if top_transition_bottleneck_label:
            bottleneck_lines.append(
                f"- 遷移「{top_transition_bottleneck_label}」の所要時間が突出しています。"
                f"承認待ち・差戻し等の内訳をドリルダウンで確認してください。"
            )
        if top_impact_row:
            bottleneck_lines.append(
                f"- 改善インパクトが高い遷移「{top_impact_row['transition_label']}」の前後工程を"
                f"確認し、待ち時間の要因を特定してください。"
            )
        if not bottleneck_lines:
            bottleneck_lines.append(
                "- 明確なボトルネック遷移は検出されませんでした。"
                "パターン分析で全体の流れを確認してください。"
            )

    elif analysis_key == "pattern":
        if top_row:
            overall_trend_text = (
                f"最も多いパターンは「{top_row.get('処理順パターン', top_row.get('パターン', '不明'))}」です。"
            )
        else:
            overall_trend_text = "パターンデータが不足しています。"

        attention_lines = [
            "- 主要パターンと例外パターンの処理時間差に注目してください。",
            "- 繰り返しが発生しているパターンは改善候補となります。",
        ]

        bottleneck_lines = []
        if top_activity_bottleneck:
            bottleneck_lines.append(
                f"- アクティビティ「{top_activity_bottleneck['activity']}」の"
                f"処理時間がパターン全体に影響しています。"
            )
        bottleneck_lines.append(
            "- パターン間の処理時間差を確認し、"
            "例外ルートの原因特定を進めてください。"
        )

    else:
        if top_impact_row:
            overall_trend_text = (
                f"最も改善インパクトが高い遷移は「{top_impact_row['transition_label']}」で、"
                f"平均所要時間 {top_impact_row['avg_duration_text']} です。"
            )
        else:
            overall_trend_text = "データに基づく全体傾向を算出中です。"

        attention_lines = []
        if top_transition_bottleneck_label:
            attention_lines.append(
                f"- 「{top_transition_bottleneck_label}」の遷移が"
                f"所要時間の観点で最も注目すべきポイントです。"
            )
        if top_activity_bottleneck:
            attention_lines.append(
                f"- アクティビティ「{top_activity_bottleneck['activity']}」の処理時間が"
                f"他のアクティビティと比較して長くなっています。"
            )
        if not attention_lines:
            attention_lines.append(
                "- 現時点で特に顕著な偏りやボトルネックは検出されていません。"
            )

        bottleneck_lines = []
        if top_transition_bottleneck_label:
            bottleneck_lines.append(
                f"- 遷移「{top_transition_bottleneck_label}」の所要時間が突出しています。"
            )
        if top_activity_bottleneck:
            bottleneck_lines.append(
                f"- アクティビティ「{top_activity_bottleneck['activity']}」のケース明細を確認してください。"
            )
        if not bottleneck_lines:
            bottleneck_lines.append(
                "- 明確なボトルネックは検出されませんでした。詳細を確認してください。"
            )

    return "\n".join(
        [
            "【全体傾向】",
            (
                f"{analysis_name} のデータセットには "
                f"{int(dashboard_summary.get('total_cases', 0)):,} ケース / "
                f"{int(dashboard_summary.get('total_records', 0)):,} イベントが含まれます。"
            ),
            f"分析期間 {period_text} です。",
            overall_trend_text,
            "",
            "【注目ポイント】",
            *attention_lines,
            "",
            "【ボトルネック示唆】",
            *bottleneck_lines,
            *group_comparison_lines,
        ]
    )

def get_cached_ai_summary(run_data, analysis_key, filter_params=None):
    cache_key = (
        str(analysis_key or "").strip().lower(),
        build_filter_cache_key(filter_params),
        None,
    )
    cached_payload = run_data.setdefault("ai_insights_cache", {}).get(cache_key)
    if cached_payload is None:
        return None
    cached = {
        **cached_payload,
        "generated": True,
        "cached": True,
    }
    if cached.get("text"):
        cached["text"] = normalize_ai_generated_text(cached["text"])
    return cached


def build_ai_context_summary(
    run_data,
    analysis_key,
    filter_params=None,
    prepared_df=None,
    variant_pattern=None,
    analysis=None,
    dashboard_summary=None,
    impact_summary=None,
    bottleneck_summary=None,
    root_cause_summary=None,
    insights_summary=None,
    variant_items=None,
    analysis_name=None,
    group_columns=None,
    group_summary=None,
):
    normalized_analysis_key = str(analysis_key or "").strip().lower()
    analysis_definitions = get_available_analysis_definitions()
    resolved_analysis = analysis or get_analysis_data(
        run_data,
        normalized_analysis_key,
        filter_params=filter_params,
        variant_pattern=variant_pattern,
    )
    resolved_prepared_df = prepared_df
    if resolved_prepared_df is None:
        resolved_prepared_df = pd.DataFrame(
            columns=[
                "case_id",
                "activity",
                "duration_sec",
                "start_time",
                "next_time",
                "timestamp",
            ]
        )
    resolved_dashboard_summary = dashboard_summary or get_dashboard_summary(
        run_data,
        filter_params=filter_params,
        prepared_df=resolved_prepared_df,
        variant_pattern=variant_pattern,
    )
    resolved_impact_summary = impact_summary or get_impact_summary(
        run_data,
        filter_params=filter_params,
        prepared_df=resolved_prepared_df,
        variant_pattern=variant_pattern,
    )
    resolved_bottleneck_summary = bottleneck_summary
    if resolved_bottleneck_summary is None:
        if variant_pattern:
            resolved_bottleneck_summary = query_bottleneck_summary(
                run_data["prepared_parquet_path"],
                filter_params=filter_params,
                filter_column_settings=run_data.get("column_settings"),
                variant_pattern=variant_pattern,
                limit=None,
            )
        else:
            resolved_bottleneck_summary = get_bottleneck_summary(
                run_data,
                filter_params=filter_params,
            )
    resolved_root_cause_summary = root_cause_summary or get_root_cause_summary(
        run_data,
        filter_params=filter_params,
        prepared_df=resolved_prepared_df,
        variant_pattern=variant_pattern,
    )
    resolved_analysis_name = (
        analysis_name
        or resolved_analysis.get("analysis_name")
        or analysis_definitions.get(normalized_analysis_key, {})
        .get("config", {})
        .get("analysis_name", analysis_key)
    )
    period_text = query_period_text(
        run_data["prepared_parquet_path"],
        filter_params=filter_params,
        filter_column_settings=run_data.get("column_settings"),
        variant_pattern=variant_pattern,
    )
    resolved_insights_summary = insights_summary or get_rule_based_insights_summary(
        run_data,
        normalized_analysis_key,
        analysis_rows=resolved_analysis.get("rows"),
        filter_params=filter_params,
        prepared_df=resolved_prepared_df,
        variant_pattern=variant_pattern,
        dashboard_summary=resolved_dashboard_summary,
        impact_summary=resolved_impact_summary,
    )

    return {
        "analysis_key": normalized_analysis_key,
        "analysis_name": resolved_analysis_name,
        "analysis_rows": list(resolved_analysis.get("rows", []))[:10],
        "dashboard_summary": resolved_dashboard_summary,
        "impact_summary": resolved_impact_summary,
        "bottleneck_summary": resolved_bottleneck_summary,
        "root_cause_summary": resolved_root_cause_summary,
        "variant_items": (
            list(variant_items)[:5]
            if variant_items is not None
            else list(
                get_variant_items(
                    run_data,
                    filter_params=filter_params,
                    variant_pattern=variant_pattern,
                )
            )[:5]
        ),
        "period_text": period_text,
        "insights_summary": resolved_insights_summary,
        "group_columns": group_columns or [],
        "group_summary": group_summary or [],
    }


def build_excel_ai_summary(
    run_data,
    analysis_key,
    analysis_name,
    filter_params,
    prepared_df,
    variant_pattern,
    dashboard_summary,
    impact_summary,
    bottleneck_summary,
    analysis=None,
    root_cause_summary=None,
    insights_summary=None,
    variant_items=None,
    use_cache=True,
    generate_text=None,
    group_columns=None,
    group_summary=None,
):
    return build_ai_insights_summary(
        run_data=run_data,
        analysis_key=analysis_key,
        filter_params=filter_params,
        prepared_df=prepared_df,
        variant_pattern=variant_pattern,
        analysis=analysis,
        dashboard_summary=dashboard_summary,
        impact_summary=impact_summary,
        bottleneck_summary=bottleneck_summary,
        root_cause_summary=root_cause_summary,
        insights_summary=insights_summary,
        variant_items=variant_items,
        analysis_name=analysis_name,
        force_refresh=False,
        use_cache=use_cache,
        generate_text=generate_text,
        group_columns=group_columns,
        group_summary=group_summary,
    )


def build_empty_ai_summary(analysis_key, analysis_name):
    return {
        "title": REPORT_SHEET_NAMES["ai_insights"],
        "analysis_key": analysis_key,
        "analysis_name": analysis_name,
        "generated": False,
        "cached": False,
        "mode": "idle",
        "provider": "",
        "generated_at": "",
        "period": "",
        "text": "",
        "highlights": [],
        "recommended_actions": [],
        "note": "",
    }


def build_analysis_ai_prompt(ai_context):
    analysis_key = str(ai_context["analysis_key"]).strip().lower()
    analysis_name = ai_context["analysis_name"]
    focus_map = {
        "frequency": {
            "focus": "件数が集中しているアクティビティと、平均処理時間が長いアクティビティを特定してください。",
            "priority": "頻度の偏り、処理時間、ばらつきの3点を組み合わせて、改善余地を説明してください。",
            "actions": "入口処理の見直し、重複作業の削減、滞留ケースの確認につながる示唆を重視してください。",
            "constraint": "頻度分析では「バリアント」「パターン」という用語は使わず、アクティビティ単位の傾向に焦点を当ててください。",
            "variant_label": "主要処理経路",
            "coverage_label": "上位10処理経路カバー率",
        },
        "transition": {
            "focus": "ボトルネックになっている遷移と、その前後の処理のつながりを説明してください。",
            "priority": "遷移時間の長さ、改善インパクト、関連アクティビティの偏りを重視してください。",
            "actions": "前後工程の引き継ぎ、待ち時間、承認経路の見直しにつながる示唆を重視してください。",
            "constraint": "前後処理分析では「バリアント」「パターン」という用語は使わず、遷移（前後のアクティビティのつながり）に焦点を当ててください。",
            "variant_label": "主要処理経路",
            "coverage_label": "上位10処理経路カバー率",
        },
        "pattern": {
            "focus": "主要パターンと例外パターンの差を明確にし、どの順序に改善余地があるか説明してください。",
            "priority": "出現頻度、処理時間、分岐の違いから改善余地を説明してください。",
            "actions": "例外ルートの原因特定や標準ルートへの集約につながる示唆を重視してください。",
            "constraint": "",
            "variant_label": "主要バリアント",
            "coverage_label": "上位10バリアントカバー率",
        },
    }
    focus_config = focus_map.get(
        analysis_key,
        {
            "focus": "主要な傾向と、そこから見える課題を説明してください。",
            "priority": "処理時間、件数、ボトルネックの観点を整理してください。",
            "actions": "現場で次に確認すべきアクションにつながる示唆を重視してください。",
            "constraint": "",
            "variant_label": "主要処理経路",
            "coverage_label": "上位10処理経路カバー率",
        },
    )

    group_columns = ai_context.get("group_columns", [])
    group_summary = ai_context.get("group_summary", [])
    comparable_group_rows = [
        row
        for row in group_summary
        if str(row.get("value") or row.get("値") or "") != "全体"
    ]

    group_prompt_section = ""
    group_instruction = ""
    if len(comparable_group_rows) >= 2:
        group_prompt_section = f"""

## グループ別集計（グルーピング軸: {"、".join(group_columns)}）
{serialize_ai_prompt_rows(group_summary, max_items=10)}
"""
        group_instruction = """
【グループ間比較】
グループ別集計データに基づき、グループ間の特徴的な違いを2〜3点指摘してください。
以下の観点を重視してください:
・ケース数・イベント数の偏りがあるグループ
・グループ間で平均処理時間に差があるポイント
・特定のグループで顕著な特徴（処理時間が突出して長い/短い等）

"""

    constraint_line = f"- {focus_config['constraint']}" if focus_config.get("constraint") else ""
    return f"""あなたはプロセスマイニングの分析結果を要約するアナリストです。以下の分析結果に基づき、簡潔で実務に使える解説を日本語で作成してください。

## この分析で重視する視点
- {focus_config['focus']}
- {focus_config['priority']}
- {focus_config['actions']}
{constraint_line}

## 基本情報
- 分析名: {analysis_name}
- ケース数: {int(ai_context['dashboard_summary'].get('total_cases', 0)):,}
- イベント数: {int(ai_context['dashboard_summary'].get('total_records', 0)):,}
- 分析期間: {ai_context['period_text']}
- 平均ケース処理時間: {ai_context['dashboard_summary'].get('avg_case_duration_text', '0s')}
- {focus_config['coverage_label']}: {float(ai_context['dashboard_summary'].get('top10_variant_coverage_pct', 0.0)):.2f}%

## 現在の分析結果上位
{serialize_ai_prompt_rows(ai_context['analysis_rows'], max_items=7)}

## アクティビティボトルネック
{serialize_ai_prompt_rows(ai_context['bottleneck_summary'].get('activity_bottlenecks', []), max_items=5)}

## 遷移ボトルネック
{serialize_ai_prompt_rows(ai_context['bottleneck_summary'].get('transition_bottlenecks', []), max_items=5)}

## 改善インパクト上位
{serialize_ai_prompt_rows(ai_context['impact_summary'].get('rows', []), max_items=5)}

## Root Cause 候補
{serialize_ai_prompt_rows(ai_context['root_cause_summary'].get('rows', []), max_items=5)}

## {focus_config['variant_label']}
{serialize_ai_prompt_rows(ai_context['variant_items'], max_items=5)}

## ルールベース要点
{serialize_ai_prompt_rows([item.get('text', '') for item in ai_context['insights_summary'].get('items', [])], max_items=5)}{group_prompt_section}

## 回答形式（必ず以下の4セクションに分けて出力してください）
【全体傾向】
全体の傾向を2〜3文で要約してください。数値をそのまま並べるのではなく、意味のある解釈を含めてください。

【注目ポイント】
この分析で特に注目すべきポイントを2〜3点、箇条書きで整理してください。単なる数値の列挙ではなく、なぜ重要かも簡潔に述べてください。

【ボトルネック示唆】
ボトルネック候補を1〜2点に絞って説明してください。分析結果に基づく根拠を明示し、確認すべき要因を示してください。

{group_instruction}【推奨アクション】
分析結果を踏まえ、次に確認すべきアクションや追加調査を1〜3点、箇条書きで提案してください。実行可能で具体的な内容にしてください。

マークダウン記法（**、##、- のリスト記号、▸ など）は使わず、プレーンテキストで出力してください。
ただしセクション見出しは必ず【】で囲んでください（例: 【全体傾向】【注目ポイント】【ボトルネック示唆】【推奨アクション】）。
強調したい箇所は「」で囲んでください。箇条書きには「・」を使用してください。
分単位の大きな数値（1440分以上）は日単位に換算して表示し、分単位の生数値は省略してください（例: ×「30685分」→ ○「約21日」）。
"""


def extract_recommended_actions_from_text(text):
    """LLMの出力テキストから推奨アクションセクションを分離して返す。"""
    normalized_text = str(text or "").strip()
    if not normalized_text:
        return "", []

    lines = normalized_text.splitlines()
    marker_index = next(
        (index for index, line in enumerate(lines) if line.strip() == "【推奨アクション】"),
        None,
    )
    if marker_index is None:
        marker_index = next(
            (
                index
                for index, line in enumerate(lines)
                if "推奨アクション" in line.strip() and len(line.strip()) <= 20
            ),
            None,
        )
    if marker_index is None:
        return normalized_text, []

    main_text = "\n".join(lines[:marker_index]).rstrip()
    actions = []
    for line in lines[marker_index + 1 :]:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("【") and stripped.endswith("】"):
            break
        if stripped[0] in "-・*":
            actions.append(stripped.lstrip("-・*").strip())
        elif len(stripped) > 1 and stripped[0].isdigit() and stripped[1] in ".)":
            actions.append(stripped[2:].strip())
        elif len(stripped) > 2 and stripped[:2].isdigit() and stripped[2] in ".)":
            actions.append(stripped[3:].strip())
        else:
            actions.append(stripped)

    return main_text, [action for action in actions if action]


def normalize_ai_generated_text(text):
    normalized_text = str(text or "").strip()
    if not normalized_text:
        return ""
    return re.sub(
        r"^[▸►▹▶]\s*(.+)$",
        lambda match: f"【{match.group(1).strip()}】",
        normalized_text,
        flags=re.MULTILINE,
    )


def build_ai_insights_summary(
    run_data,
    analysis_key,
    filter_params=None,
    prepared_df=None,
    variant_pattern=None,
    analysis=None,
    dashboard_summary=None,
    impact_summary=None,
    bottleneck_summary=None,
    root_cause_summary=None,
    insights_summary=None,
    variant_items=None,
    analysis_name=None,
    force_refresh=False,
    use_cache=True,
    generate_text=None,
    group_columns=None,
    group_summary=None,
):
    cache_key = (
        str(analysis_key or "").strip().lower(),
        build_filter_cache_key(filter_params),
        str(variant_pattern or "").strip() or None,
    )
    cache = run_data.setdefault("ai_insights_cache", {})

    if use_cache and not force_refresh and cache_key in cache:
        cached = {**cache[cache_key], "generated": True, "cached": True}
        if cached.get("text"):
            cached["text"] = normalize_ai_generated_text(cached["text"])
        return cached

    ai_context = build_ai_context_summary(
        run_data=run_data,
        analysis_key=analysis_key,
        filter_params=filter_params,
        prepared_df=prepared_df,
        variant_pattern=variant_pattern,
        analysis=analysis,
        dashboard_summary=dashboard_summary,
        impact_summary=impact_summary,
        bottleneck_summary=bottleneck_summary,
        root_cause_summary=root_cause_summary,
        insights_summary=insights_summary,
        variant_items=variant_items,
        analysis_name=analysis_name,
        group_columns=group_columns,
        group_summary=group_summary,
    )
    generated_at = datetime.now(timezone.utc).isoformat()
    fallback_text = build_ai_fallback_text(ai_context)

    if not ai_context["dashboard_summary"].get("has_data"):
        fallback_actions = build_ai_recommended_actions(ai_context)
        payload = {
            "title": REPORT_SHEET_NAMES["ai_insights"],
            "analysis_key": ai_context["analysis_key"],
            "analysis_name": ai_context["analysis_name"],
            "mode": "rule_based",
            "provider": "",
            "generated_at": generated_at,
            "period": ai_context["period_text"],
            "text": fallback_text,
            "highlights": [
                item["text"] for item in ai_context["insights_summary"].get("items", [])
            ],
            "recommended_actions": fallback_actions,
            "note": "既存集計からの要約を掲載しています。",
        }
        if use_cache:
            cache[cache_key] = payload
        return {
            **payload,
            "generated": True,
            "cached": False,
        }

    request_ai_text = generate_text or request_ollama_insights_text

    try:
        ai_text = request_ai_text(build_analysis_ai_prompt(ai_context))
        if ai_text:
            normalized_ai_text = normalize_ai_generated_text(ai_text)
            main_text, llm_actions = extract_recommended_actions_from_text(normalized_ai_text)
            if not llm_actions:
                llm_actions = build_ai_recommended_actions(ai_context)
            payload = {
                "title": REPORT_SHEET_NAMES["ai_insights"],
                "analysis_key": ai_context["analysis_key"],
                "analysis_name": ai_context["analysis_name"],
                "mode": "ollama",
                "provider": "",
                "generated_at": generated_at,
                "period": ai_context["period_text"],
                "text": main_text,
                "highlights": [
                    item["text"]
                    for item in ai_context["insights_summary"].get("items", [])
                ],
                "recommended_actions": llm_actions,
                "note": "",
            }
            if use_cache:
                cache[cache_key] = payload
            return {
                **payload,
                "generated": True,
                "cached": False,
            }
    except httpx.ConnectError:
        error_message = "既存集計からの要約を掲載しています。"
    except Exception:
        error_message = "既存集計からの要約を掲載しています。"
    else:
        error_message = "既存集計からの要約を掲載しています。"

    fallback_actions = build_ai_recommended_actions(ai_context)
    payload = {
        "title": REPORT_SHEET_NAMES["ai_insights"],
        "analysis_key": ai_context["analysis_key"],
        "analysis_name": ai_context["analysis_name"],
        "mode": "rule_based",
        "provider": "",
        "generated_at": generated_at,
        "period": ai_context["period_text"],
        "text": fallback_text,
        "highlights": [item["text"] for item in ai_context["insights_summary"].get("items", [])],
        "recommended_actions": fallback_actions,
        "note": error_message,
    }
    if use_cache:
        cache[cache_key] = payload
    return {
        **payload,
        "generated": True,
        "cached": False,
    }
