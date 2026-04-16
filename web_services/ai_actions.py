def build_ai_recommended_actions(ai_context):
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

