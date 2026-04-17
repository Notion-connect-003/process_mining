from excel.detail.report import build_transition_display_label


def _build_group_comparison_lines(group_summary):
    if not group_summary:
        return []

    group_rows = []
    for row in group_summary:
        value = row.get("value") or row.get("値") or ""
        if str(value) == "全体":
            continue
        group_rows.append(row)

    if len(group_rows) < 2:
        return []

    case_key = next((key for key in ["case_count", "ケース数"] if key in group_rows[0]), None)
    avg_duration_key = next(
        (
            key
            for key in ["avg_case_duration_min", "平均処理時間(分)", "avg_duration_min"]
            if key in group_rows[0]
        ),
        None,
    )
    value_key = next((key for key in ["value", "値"] if key in group_rows[0]), "value")

    lines = ["", "グループ間比較", f"{len(group_rows)}グループを比較した結果:"]

    if case_key:
        sorted_by_case = sorted(
            group_rows,
            key=lambda row: float(row.get(case_key, 0) or 0),
            reverse=True,
        )
        max_group = sorted_by_case[0]
        min_group = sorted_by_case[-1]
        lines.append(
            f"- ケース数が最も多いのは「{max_group.get(value_key, '不明')}」"
            f"（{int(float(max_group.get(case_key, 0) or 0)):,}件）、"
            f"最も少ないのは「{min_group.get(value_key, '不明')}」"
            f"（{int(float(min_group.get(case_key, 0) or 0)):,}件）です。"
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
                f"- 平均処理時間が最も長いグループは「{max_dur_group.get(value_key, '不明')}」"
                f"（{max_dur:.1f}分）、最も短いのは「{min_dur_group.get(value_key, '不明')}」"
                f"（{min_dur:.1f}分）です。"
            )
            if min_dur > 0 and max_dur / min_dur >= 1.5:
                lines.append(
                    f"- 両者の差は約{max_dur - min_dur:.1f}分（{max_dur / min_dur:.1f}倍）あり、"
                    "グループ固有の要因を確認することを推奨します。"
                )

    if not any(line.startswith("- ") for line in lines):
        lines.append("- グループ間で顕著な差は検出されませんでした。")

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
    top_transition_bottleneck_label = build_transition_display_label(top_transition_bottleneck)
    top_row = analysis_rows[0] if analysis_rows else {}
    group_comparison_lines = _build_group_comparison_lines(ai_context.get("group_summary", []))

    if analysis_key == "frequency":
        top_activity_name = top_row.get("アクティビティ") or top_row.get("activity") or "不明"
        top_event_count = int(
            float(
                top_row.get("イベント数", top_row.get("event_count", top_row.get("件数", top_row.get("count", 0))))
                or 0
            )
        )
        if top_event_count <= 0:
            event_ratio_pct = float(
                top_row.get("イベント比率(%)", top_row.get("event_ratio_pct", 0)) or 0
            )
            if event_ratio_pct > 0:
                top_event_count = int(round(float(dashboard_summary.get("total_records", 0) or 0) * event_ratio_pct / 100))
        lines = [
            f"{analysis_name}の結果、{period_text}の期間で"
            f"{int(dashboard_summary.get('total_cases', 0)):,}件のケース / "
            f"{int(dashboard_summary.get('total_records', 0)):,}件のイベントを確認しました。",
            f"最も出現回数が多いアクティビティは「{top_activity_name}」で、{top_event_count:,} 件です。"
            if top_activity_name
            else "主要なアクティビティは特定できませんでした。",
        ]
    elif analysis_key == "transition":
        transition_label = (
            top_row.get("遷移") or top_row.get("transition_label") or top_row.get("transition") or "不明"
        )
        transition_count = int(float(top_row.get("件数", top_row.get("count", 0)) or 0))
        lines = [
            f"{analysis_name}の結果、{period_text}の期間で"
            f"{int(dashboard_summary.get('total_cases', 0)):,}件のケース / "
            f"{int(dashboard_summary.get('total_records', 0)):,}件のイベントを確認しました。",
            f"代表的な遷移は「{transition_label}」で、{transition_count:,}件です。"
            if transition_label
            else "代表的な遷移は特定できませんでした。",
        ]
    else:
        pattern_label = top_row.get("処理順パターン") or top_row.get("pattern") or "不明"
        pattern_count = int(float(top_row.get("ケース数", top_row.get("case_count", 0)) or 0))
        lines = [
            f"{analysis_name}の結果、{period_text}の期間で"
            f"{int(dashboard_summary.get('total_cases', 0)):,}件のケース / "
            f"{int(dashboard_summary.get('total_records', 0)):,}件のイベントを確認しました。",
            f"代表的な処理順は「{pattern_label}」で、{pattern_count:,}件です。"
            if pattern_label
            else "代表的な処理順は特定できませんでした。",
        ]

    if top_activity_bottleneck:
        lines.append(f"ボトルネック候補のアクティビティは「{top_activity_bottleneck.get('activity', '不明')}」です。")
    if top_transition_bottleneck_label:
        lines.append(f"遷移ボトルネック候補は「{top_transition_bottleneck_label}」です。")
    if top_impact_row:
        lines.append(f"改善インパクトの高い箇所は「{top_impact_row.get('transition_label', '不明')}」です。")
    lines.extend(group_comparison_lines)
    return "\n".join([line for line in lines if line])
