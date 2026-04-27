from reports.excel.common import (
    REPORT_SHEET_NAMES,
    append_bullet_rows,
    append_key_value_rows,
    append_table_to_worksheet,
    initialize_excel_worksheet,
    sanitize_workbook_sheet_name,
)
from reports.excel.exports.detail_report_helpers import *
def _create_report_sheet(workbook, title, *, use_active=False):
    worksheet = workbook.active if use_active else workbook.create_sheet()
    worksheet.title = sanitize_workbook_sheet_name(title)
    initialize_excel_worksheet(worksheet)
    return worksheet

def _append_pattern_export_sheets(workbook, context, run_data, filter_params, pattern_display_limit="10"):
    from reports.excel.exports.detail_report_sections import (
        append_pattern_conclusion_charts,
        append_pattern_detail_sheet,
        build_excel_anchor,
    )
    analysis_definitions = context["analysis_definitions"]
    selected_analysis = context["selected_analysis"]
    export_variant_items = context["export_variant_items"]
    pattern_config = analysis_definitions.get("pattern", {}).get("config", {})
    pattern_display_columns = pattern_config.get("display_columns", {})
    pattern_column_label = pattern_display_columns.get("pattern", "処理順パターン")
    pattern_summary = build_pattern_export_summary(
        selected_analysis["rows"],
        pattern_display_columns,
    )
    pattern_detail_count = resolve_pattern_detail_sheet_count(
        pattern_display_limit,
        len(selected_analysis.get("rows", [])),
    )
    attach_pattern_detail_links(pattern_summary, pattern_detail_count)
    pattern_conclusion = build_pattern_conclusion_summary(pattern_summary)
    pattern_dashboard = build_pattern_dashboard_summary(pattern_summary, pattern_conclusion)

    conclusion_sheet = _create_report_sheet(workbook, REPORT_SHEET_NAMES["pattern_conclusion"])
    conclusion_next_row = append_key_value_rows(
        conclusion_sheet,
        REPORT_SHEET_NAMES["pattern_conclusion"],
        [
            ("全体要約", pattern_conclusion["overall_summary"]),
            ("改善候補パターン数", pattern_conclusion["improvement_candidate_count"]),
            ("改善検討時間規模", pattern_conclusion["total_impact_text"]),
            ("改善検討時間規模(分)", pattern_conclusion["total_impact_minutes"]),
            ("改善検討時間規模(時間)", pattern_conclusion["total_impact_hours"]),
            ("改善検討時間規模(日)", pattern_conclusion["total_impact_days"]),
            ("最短処理パターン", pattern_conclusion["fastest_pattern"].get("パターン", "該当なし")),
            ("最短処理パターン平均所要時間(分)", pattern_conclusion["fastest_pattern"].get("平均所要時間(分)", 0)),
        ],
        description="処理順パターン分析の結論、改善優先度、優先確認のための集計上の時間規模をまとめています。確定した短縮効果ではありません。",
    )
    conclusion_next_row = append_table_to_worksheet(
        conclusion_sheet,
        "問題点3つ",
        pattern_conclusion["issue_rows"],
        ["問題点", "原因", "改善案", "対象ケース数", "平均所要時間差分(分)", "改善検討時間規模", "改善検討時間規模(分)", "改善検討時間規模(時間)", "改善検討時間規模(日)", "対象パターン", "詳細リンク"],
        start_row=conclusion_next_row,
        description="改善対象パターンTOP3を中心に、問題点・原因・改善案・改善検討時間規模を整理しています。改善検討時間規模は対象ケース数と平均所要時間差分から算出した確認優先度です。",
        no_wrap_headers=["対象パターン", "詳細リンク"],
        min_column_widths={"問題点": 40, "原因": 38, "改善案": 42, "対象パターン": 56, "詳細リンク": 18},
    )
    append_pattern_conclusion_charts(
        workbook,
        conclusion_sheet,
        pattern_summary["comparison_rows"],
        pie_anchor=build_excel_anchor("A", conclusion_next_row + 1),
        bar_anchor=build_excel_anchor("C", conclusion_next_row + 1),
    )

    dashboard_sheet = _create_report_sheet(workbook, REPORT_SHEET_NAMES["pattern_dashboard"])
    dashboard_next_row = append_key_value_rows(
        dashboard_sheet,
        REPORT_SHEET_NAMES["pattern_dashboard"],
        [
            ("全体要約", pattern_dashboard["overall_summary"]),
            ("上位10パターン累積カバー率(%)", pattern_dashboard["top10_coverage_pct"]),
            ("改善候補パターン数", pattern_dashboard["improvement_candidate_count"]),
            ("改善検討時間規模", pattern_dashboard["total_impact_text"]),
            ("改善検討時間規模(分)", pattern_dashboard["total_impact_minutes"]),
        ],
        description="処理順パターン分析の主要サマリーをダッシュボード形式でまとめています。改善検討時間規模は確定効果ではなく、優先確認のための集計値です。",
    )
    dashboard_next_row = append_table_to_worksheet(
        dashboard_sheet,
        "改善優先TOP3",
        pattern_dashboard["top3_rows"],
        ["順位", "詳細リンク", "パターン", "改善優先度スコア", "全体影響度(%)", "繰り返し率(%)", "平均所要時間差分(分)", "簡易コメント"],
        start_row=dashboard_next_row,
        description="改善優先度スコアが高い上位3パターンです。",
        no_wrap_headers=["詳細リンク", "パターン", "簡易コメント"],
        min_column_widths={"詳細リンク": 18, "パターン": 56, "簡易コメント": 36},
    )
    append_bullet_rows(
        dashboard_sheet,
        "問題点",
        pattern_dashboard["problem_points"],
        start_row=dashboard_next_row,
        column_count=6,
        empty_text="抽出できる問題点はありません。",
    )

    if "pattern_summary" in context["export_sheet_keys"]:
        pattern_summary_sheet = _create_report_sheet(workbook, REPORT_SHEET_NAMES["pattern_summary"])
        next_row = append_key_value_rows(
            pattern_summary_sheet,
            REPORT_SHEET_NAMES["pattern_summary"],
            [
                ("上位3パターン累積カバー率(%)", pattern_summary["top3_coverage_pct"]),
                ("上位10パターン累積カバー率(%)", pattern_summary["top10_coverage_pct"]),
                ("カバー率要約", pattern_summary["coverage_summary_text"]),
                ("最短処理パターン", pattern_summary["fastest_pattern"].get("パターン", "該当なし")),
                ("最短処理パターン平均所要時間(分)", pattern_summary["fastest_pattern"].get("平均所要時間(分)", 0)),
                ("要確認パターン数", len(pattern_summary["repeated_patterns"])),
                ("要確認パターン影響比率(%)", pattern_summary["repeated_case_ratio_pct"]),
                ("改善候補パターン数", pattern_summary["improvement_candidate_count"]),
                ("要確認判定基準", "繰り返し率 30%以上"),
                ("改善対象抽出基準", "繰り返し率 10%以上 かつ 平均所要時間差分がプラス"),
            ],
            description="処理順パターンの上位カバー率、要確認パターン、改善候補パターンをまとめています。サマリー上の処理順は読みやすさのため短縮表示しています。",
        )
        next_row = append_table_to_worksheet(
            pattern_summary_sheet,
            "上位10パターン",
            pattern_summary["comparison_rows"],
            ["順位", "詳細リンク", "繰り返し", "繰り返し回数", "繰り返し率(%)", "繰り返し率区分", "件数", "全体比率(%)", "平均所要時間(分)", "平均所要時間差分(分)", "改善優先度スコア", "全体影響度(%)", "最短処理", "最短所要時間(分)", "最長所要時間(分)", "確認区分", "簡易コメント", "パターン"],
            start_row=next_row,
            description="件数上位10パターンの比率・所要時間・繰り返し率を比較できます。",
            no_wrap_headers=["詳細リンク", "パターン", "簡易コメント"],
            min_column_widths={"詳細リンク": 18, "パターン": 56, "簡易コメント": 48},
        )
        next_row = append_table_to_worksheet(
            pattern_summary_sheet,
            "要確認パターン一覧",
            pattern_summary["repeated_patterns"],
            ["順位", "詳細リンク", "繰り返し", "繰り返し回数", "繰り返し率(%)", "繰り返し率区分", "件数", "全体比率(%)", "平均所要時間(分)", "平均所要時間差分(分)", "改善優先度スコア", "全体影響度(%)", "最短処理", "最短所要時間(分)", "最長所要時間(分)", "確認区分", "簡易コメント", "パターン"],
            start_row=next_row,
            description="繰り返し率が高く、確認を優先したいパターンを一覧化しています。0件の場合でも、別枠の改善候補パターンを確認してください。",
            no_wrap_headers=["詳細リンク", "パターン", "簡易コメント"],
            min_column_widths={"詳細リンク": 18, "パターン": 56, "簡易コメント": 48},
        )
        append_table_to_worksheet(
            pattern_summary_sheet,
            "改善対象パターンTOP3",
            pattern_summary["improvement_targets"],
            ["順位", "詳細リンク", "繰り返し", "繰り返し回数", "繰り返し率(%)", "繰り返し率区分", "件数", "全体比率(%)", "平均所要時間(分)", "平均所要時間差分(分)", "改善優先度スコア", "全体影響度(%)", "最短処理", "確認区分", "簡易コメント", "パターン"],
            start_row=next_row,
            description="繰り返し率が一定以上で、平均所要時間も全体平均より長い改善候補パターンです。",
            no_wrap_headers=["詳細リンク", "パターン", "簡易コメント"],
            min_column_widths={"詳細リンク": 18, "パターン": 56, "簡易コメント": 48},
        )

    pattern_sheet = _create_report_sheet(workbook, REPORT_SHEET_NAMES["pattern"])
    pattern_rows, pattern_headers = localize_report_rows(
        build_pattern_overview_rows(
            selected_analysis["rows"],
            export_variant_items,
            pattern_column_label,
            analysis_definitions,
        ),
        [
            "rank",
            "pattern_variant",
            "repeat_flag",
            "repeat_count",
            "repeat_rate_pct",
            "repeat_rate_band",
            "review_flag",
            "avg_case_duration_diff_min",
            "improvement_priority_score",
            "overall_impact_pct",
            "fastest_pattern_flag",
            "simple_comment",
            "step_count",
            "repeated_activities",
            "count",
            "ratio",
            "cumulative_case_ratio_pct",
            "avg_case_duration_min",
            "std_case_duration_min",
            "min_case_duration_min",
            "max_case_duration_min",
            "p75_case_duration_min",
            "p90_case_duration_min",
            "p95_case_duration_min",
            "pattern",
        ],
    )
    append_table_to_worksheet(
        pattern_sheet,
        REPORT_SHEET_NAMES["pattern"],
        pattern_rows,
        pattern_headers,
        description="パターン / バリアントを 1 つの一覧にまとめ、繰り返し回数・繰り返し率・確認区分・件数・比率・累積カバー率・所要時間の代表値と代表ルートを比較できます。",
        no_wrap_headers=["パターン", "簡易コメント"],
        min_column_widths={"パターン": 80, "簡易コメント": 48},
    )

    variant_by_pattern = {
        str(variant_item.get("pattern") or "").strip(): variant_item
        for variant_item in export_variant_items
    }
    for pattern_rank, pattern_row in enumerate(selected_analysis.get("rows", [])[:pattern_detail_count], start=1):
        pattern_text = str(pattern_row.get(pattern_column_label) or "").strip()
        pattern_detail = None
        if pattern_text:
            pattern_detail = query_pattern_bottleneck_details(
                run_data["prepared_parquet_path"],
                pattern_text,
                filter_params=filter_params,
                filter_column_settings=run_data.get("column_settings"),
                scope_variant_pattern=context["variant_pattern"],
            )
        append_pattern_detail_sheet(
            workbook,
            None,
            pattern_row,
            pattern_rank,
            pattern_column_label,
            analysis_definitions,
            variant_item=variant_by_pattern.get(str(pattern_row.get(pattern_column_label) or "").strip()),
            pattern_detail=pattern_detail,
        )
