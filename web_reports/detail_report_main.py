from io import BytesIO
from datetime import datetime, timezone
import duckdb

from openpyxl import Workbook
from openpyxl.chart import BarChart, PieChart, Reference
from openpyxl.chart.data_source import AxDataSource, StrRef
from openpyxl.chart.label import DataLabelList
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter

from web_reports.excel_common import (
    ANALYSIS_PRECONDITIONS_TEXT,
    APPLIED_FILTERS_NOTE_TEXT,
    EXCEL_ALT_ROW_FILL,
    EXCEL_ASSUMPTION_SECTION_FILL,
    EXCEL_BODY_FONT,
    EXCEL_BOLD_FONT,
    EXCEL_GROUP_SECTION_FILL,
    EXCEL_GROUP_SECTION_FONT,
    EXCEL_HEADER_FILL,
    EXCEL_LABEL_FILL,
    EXCEL_MUTED_FONT,
    EXCEL_MUTED_SECTION_FILL,
    EXCEL_NOTE_FONT,
    EXCEL_SECTION_FILL,
    EXCEL_SUBTITLE_FILL,
    EXCEL_TEXT_BLOCK_FILL,
    EXCEL_THIN_BORDER,
    EXCEL_TITLE_BORDER,
    EXCEL_TITLE_FILL,
    EXCEL_TITLE_FONT,
    GROUPING_CONDITION_NOTE_TEXT,
    LOG_DIAGNOSTIC_SHEET_NAMES,
    REPORT_HEADER_LABELS,
    REPORT_SHEET_NAMES,
    TERMINOLOGY_ROWS,
    append_bullet_rows,
    append_custom_text_section_to_worksheet,
    append_definition_table_to_worksheet,
    append_key_value_rows,
    append_table_to_worksheet,
    append_text_block_to_worksheet,
    autosize_worksheet_columns,
    build_analysis_excel_file_name,
    estimate_wrapped_row_height,
    initialize_excel_worksheet,
    merge_excel_row,
    normalize_excel_cell_value,
    resolve_analysis_display_name,
    sanitize_workbook_sheet_name,
    style_excel_cell,
)

from web_reports.detail_report_helpers import *
from web_reports.detail_report_sections import *

def _create_report_sheet(workbook, title, *, use_active=False):
    worksheet = workbook.active if use_active else workbook.create_sheet()
    worksheet.title = sanitize_workbook_sheet_name(title)
    initialize_excel_worksheet(worksheet)
    return worksheet

def _serialize_workbook_bytes(workbook):
    for worksheet in workbook.worksheets:
        autosize_worksheet_columns(worksheet)

    output_buffer = BytesIO()
    workbook.save(output_buffer)
    return output_buffer.getvalue()

def _build_detail_export_summary_rows(
    context,
    run_data,
    analysis_key,
    variant_id=None,
    selected_activity="",
    case_id="",
):
    group_columns = context["group_columns"]
    grouping_text = "、".join(group_columns) if group_columns else "なし"
    summary_rows = [
        (REPORT_HEADER_LABELS["analysis_key"], analysis_key),
        (REPORT_HEADER_LABELS["analysis_name"], context["analysis_name"]),
        (REPORT_HEADER_LABELS["source_file_name"], run_data["source_file_name"]),
        (REPORT_HEADER_LABELS["analysis_executed_at"], run_data.get("created_at", "")),
        (REPORT_HEADER_LABELS["exported_at"], datetime.now(timezone.utc).isoformat()),
        (REPORT_HEADER_LABELS["case_count"], context["filtered_meta"]["case_count"]),
        (REPORT_HEADER_LABELS["event_count"], context["filtered_meta"]["event_count"]),
        (REPORT_HEADER_LABELS["applied_filters"], context.get("applied_filter_summary_text", "未適用")),
        {"label": "", "value": APPLIED_FILTERS_NOTE_TEXT, "style": "note"},
        ("分析期間", context["period_text"]),
        ("グルーピング条件", grouping_text),
        {"label": "", "value": GROUPING_CONDITION_NOTE_TEXT, "style": "note"},
    ]
    if variant_id is not None:
        summary_rows.append((REPORT_HEADER_LABELS["selected_variant"], f"#{variant_id}"))
    if analysis_key != "frequency":
        summary_rows.extend(
            [
                (REPORT_HEADER_LABELS["selected_activity"], selected_activity or "未選択"),
                (REPORT_HEADER_LABELS["selected_transition"], context["selected_transition_label"] or "未選択"),
                (REPORT_HEADER_LABELS["selected_case_id"], case_id or "未選択"),
            ]
        )
    return summary_rows

def _append_detail_export_summary_sheet(
    summary_sheet,
    context,
    run_data,
    analysis_key,
    filter_params,
    variant_id=None,
    selected_activity="",
    case_id="",
):
    summary_rows = _build_detail_export_summary_rows(
        context,
        run_data,
        analysis_key,
        variant_id=variant_id,
        selected_activity=selected_activity,
        case_id=case_id,
    )
    next_row = append_key_value_rows(
        summary_sheet,
        REPORT_SHEET_NAMES["summary"],
        summary_rows,
        description="対象範囲、選択条件、出力時点の情報をまとめています。",
    )
    kpi_rows = build_detail_summary_kpi_rows(
        analysis_key,
        context["selected_analysis"].get("rows", []),
        context["dashboard_summary"],
        context["impact_summary"],
        context["bottleneck_summary"],
        prepared_df=None,
        variant_items=context["export_variant_items"],
    )
    next_row = append_key_value_rows(
        summary_sheet,
        "主要KPI",
        kpi_rows,
        start_row=next_row,
        description=f"{context['analysis_name']} で優先して見たい代表値をまとめています。",
    )
    next_row = append_bullet_rows(
        summary_sheet,
        "分析ハイライト",
        context["ai_summary"].get("highlights", []),
        start_row=next_row,
        column_count=4,
    )

    if not context["group_columns"]:
        return

    group_summary = query_group_summary(
        run_data["prepared_parquet_path"],
        context["group_columns"],
        filter_params=filter_params,
        filter_column_settings=run_data.get("column_settings"),
        variant_pattern=context["variant_pattern"],
    )
    if not group_summary:
        return

    group_summary_df = build_summary_sheet_df(group_summary, context["group_columns"])
    append_table_to_worksheet(
        summary_sheet,
        "グループ別比較",
        group_summary_df.to_dict(orient="records"),
        list(group_summary_df.columns),
        start_row=next_row + 2,
        description="グルーピング条件ごとのケース数・処理時間の比較です。",
    )

def _append_detail_export_ai_sheet(workbook, context):
    ai_sheet = _create_report_sheet(workbook, REPORT_SHEET_NAMES["ai_insights"])
    ai_meta_rows = [
        ("対象分析", context["analysis_name"]),
        ("分析期間", context["ai_summary"].get("period", "不明")),
        ("出力時刻", context["ai_summary"].get("generated_at", "")),
    ]
    next_row = append_key_value_rows(
        ai_sheet,
        REPORT_SHEET_NAMES["ai_insights"],
        ai_meta_rows,
        description="現在の分析条件に対応する分析コメント、または既存集計からの要約を掲載します。",
    )
    next_row = append_custom_text_section_to_worksheet(
        ai_sheet,
        "分析前提",
        ANALYSIS_PRECONDITIONS_TEXT,
        start_row=next_row,
        column_count=6,
        header_fill=EXCEL_ASSUMPTION_SECTION_FILL,
    )
    next_row = append_text_block_to_worksheet(
        ai_sheet,
        "解説本文",
        context["ai_summary"].get("text", ""),
        start_row=next_row,
        column_count=6,
    )
    next_row = append_definition_table_to_worksheet(
        ai_sheet,
        "用語説明",
        TERMINOLOGY_ROWS,
        start_row=next_row,
        column_count=6,
        header_fill=EXCEL_MUTED_SECTION_FILL,
    )
    append_custom_text_section_to_worksheet(
        ai_sheet,
        "補足・免責事項",
        context["ai_summary"].get("note", ""),
        start_row=next_row,
        column_count=6,
        header_fill=EXCEL_MUTED_SECTION_FILL,
        body_fill=EXCEL_LABEL_FILL,
    )

def _append_frequency_export_sheet(workbook, context, run_data, filter_params):
    frequency_sheet = _create_report_sheet(workbook, REPORT_SHEET_NAMES["frequency"])
    group_columns = context["group_columns"]
    selected_analysis = context["selected_analysis"]
    variant_pattern = context["variant_pattern"]

    if not group_columns:
        frequency_rows = build_ranked_rows(selected_analysis["rows"], rank_key=REPORT_HEADER_LABELS["rank"])
        frequency_headers = list(frequency_rows[0].keys()) if frequency_rows else [REPORT_HEADER_LABELS["rank"]]
        append_table_to_worksheet(
            frequency_sheet,
            REPORT_SHEET_NAMES["frequency"],
            frequency_rows,
            frequency_headers,
            description="アクティビティごとの件数、ケース数、処理時間の代表値を確認できます。",
        )
        return

    overall_frequency_rows = query_analysis_records(
        run_data["prepared_parquet_path"],
        "frequency",
        filter_params=filter_params,
        filter_column_settings=run_data.get("column_settings"),
        variant_pattern=variant_pattern,
    )["rows"]

    current_row = 1
    max_frequency_columns = max(10, len(overall_frequency_rows[0]) + 1 if overall_frequency_rows else 10)
    current_row = _write_section_header(
        frequency_sheet,
        current_row,
        "全体",
        column_count=max_frequency_columns,
    )
    current_row = _write_frequency_data(
        frequency_sheet,
        overall_frequency_rows,
        current_row,
    )

    for group_name, group_frequency_rows in _iter_groups_from_parquet(
        run_data["prepared_parquet_path"],
        group_columns,
        filter_params=filter_params,
        filter_column_settings=run_data.get("column_settings"),
        variant_pattern=variant_pattern,
    ) or []:
        current_row += 3
        group_column_count = max(10, len(group_frequency_rows[0]) + 1 if group_frequency_rows else max_frequency_columns)
        current_row = _write_section_header(
            frequency_sheet,
            current_row,
            f"グループ: {group_name}",
            column_count=group_column_count,
        )
        current_row = _write_frequency_data(
            frequency_sheet,
            group_frequency_rows,
            current_row,
        )

def _append_transition_export_sheet(workbook, context):
    transition_sheet = _create_report_sheet(workbook, REPORT_SHEET_NAMES["transition"])
    transition_rows = build_ranked_rows(context["selected_analysis"]["rows"], rank_key=REPORT_HEADER_LABELS["rank"])
    transition_headers = list(transition_rows[0].keys()) if transition_rows else [REPORT_HEADER_LABELS["rank"]]
    append_table_to_worksheet(
        transition_sheet,
        REPORT_SHEET_NAMES["transition"],
        transition_rows,
        transition_headers,
        description="前後遷移ごとの件数、ケース数、平均所要時間を確認できます。",
    )

def build_detail_export_workbook_bytes(
    run_data,
    analysis_key,
    context,
    filter_params,
    pattern_display_limit="10",
    variant_id=None,
    selected_activity="",
    case_id="",
    drilldown_limit=20,
):
    workbook = Workbook()
    summary_sheet = _create_report_sheet(
        workbook,
        REPORT_SHEET_NAMES["summary"],
        use_active=True,
    )
    _append_detail_export_summary_sheet(
        summary_sheet,
        context,
        run_data,
        analysis_key,
        filter_params=filter_params,
        variant_id=variant_id,
        selected_activity=selected_activity,
        case_id=case_id,
    )
    _append_detail_export_ai_sheet(workbook, context)
    _append_detail_export_analysis_sheets(
        workbook,
        context,
        run_data,
        filter_params,
        pattern_display_limit=pattern_display_limit,
    )
    _append_drilldown_export_sheet(
        workbook,
        context,
        run_data,
        filter_params,
        selected_activity=selected_activity,
        drilldown_limit=drilldown_limit,
    )
    _append_case_trace_export_sheet(workbook, run_data, case_id=case_id)
    return _serialize_workbook_bytes(workbook)

def _append_pattern_export_sheets(workbook, context, run_data, filter_params, pattern_display_limit="10"):
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
    pattern_conclusion = build_pattern_conclusion_summary(pattern_summary)
    pattern_dashboard = build_pattern_dashboard_summary(pattern_summary, pattern_conclusion)

    conclusion_sheet = _create_report_sheet(workbook, REPORT_SHEET_NAMES["pattern_conclusion"])
    conclusion_next_row = append_key_value_rows(
        conclusion_sheet,
        REPORT_SHEET_NAMES["pattern_conclusion"],
        [
            ("全体要約", pattern_conclusion["overall_summary"]),
            ("改善による時間インパクト(分)", pattern_conclusion["total_impact_minutes"]),
            ("改善による時間インパクト(時間)", pattern_conclusion["total_impact_hours"]),
            ("最短処理パターン", pattern_conclusion["fastest_pattern"].get("パターン", "該当なし")),
            ("最短処理パターン平均処理時間(分)", pattern_conclusion["fastest_pattern"].get("平均処理時間(分)", 0)),
        ],
        description="処理順パターン分析の結論、改善優先度、想定時間効果をまとめています。",
    )
    conclusion_next_row = append_table_to_worksheet(
        conclusion_sheet,
        "問題点3つ",
        pattern_conclusion["issue_rows"],
        ["問題点", "原因", "改善案", "期待効果（時間短縮・分）", "対象パターン"],
        start_row=conclusion_next_row,
        description="改善対象パターンTOP3を中心に、問題点・原因・改善案・期待効果を整理しています。",
        no_wrap_headers=["対象パターン"],
        min_column_widths={"問題点": 40, "原因": 38, "改善案": 42, "対象パターン": 72},
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
            ("改善による時間インパクト(分)", pattern_dashboard["total_impact_minutes"]),
        ],
        description="処理順パターン分析の主要サマリーをダッシュボード形式でまとめています。",
    )
    dashboard_next_row = append_table_to_worksheet(
        dashboard_sheet,
        "改善優先TOP3",
        pattern_dashboard["top3_rows"],
        ["順位", "パターン", "改善優先度スコア", "全体影響度(%)", "繰り返し率(%)", "平均処理時間差分(分)", "簡易コメント"],
        start_row=dashboard_next_row,
        description="改善優先度スコアが高い上位3パターンです。",
        no_wrap_headers=["パターン", "簡易コメント"],
        min_column_widths={"パターン": 72, "簡易コメント": 36},
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
                ("最短処理パターン平均処理時間(分)", pattern_summary["fastest_pattern"].get("平均処理時間(分)", 0)),
                ("要確認パターン数", len(pattern_summary["repeated_patterns"])),
                ("要確認パターン影響比率(%)", pattern_summary["repeated_case_ratio_pct"]),
                ("要確認判定基準", "繰り返し率 30%以上"),
                ("改善対象抽出基準", "繰り返し率 10%以上 かつ 平均処理時間差分がプラス"),
            ],
            description="処理順パターンの上位カバー率と、繰り返し率が高いパターンをまとめています。",
        )
        next_row = append_table_to_worksheet(
            pattern_summary_sheet,
            "上位10パターン",
            pattern_summary["comparison_rows"],
            ["順位", "繰り返し", "繰り返し回数", "繰り返し率(%)", "繰り返し率区分", "件数", "全体比率(%)", "平均処理時間(分)", "平均処理時間差分(分)", "改善優先度スコア", "全体影響度(%)", "最短処理", "最短処理時間(分)", "最長処理時間(分)", "確認区分", "簡易コメント", "パターン"],
            start_row=next_row,
            description="件数上位10パターンの比率・処理時間・繰り返し率を比較できます。",
            no_wrap_headers=["パターン", "簡易コメント"],
            min_column_widths={"パターン": 72, "簡易コメント": 48},
        )
        next_row = append_table_to_worksheet(
            pattern_summary_sheet,
            "要確認パターン一覧",
            pattern_summary["repeated_patterns"],
            ["順位", "繰り返し", "繰り返し回数", "繰り返し率(%)", "繰り返し率区分", "件数", "全体比率(%)", "平均処理時間(分)", "平均処理時間差分(分)", "改善優先度スコア", "全体影響度(%)", "最短処理", "最短処理時間(分)", "最長処理時間(分)", "確認区分", "簡易コメント", "パターン"],
            start_row=next_row,
            description="繰り返し率が高く、確認を優先したいパターンを一覧化しています。",
            no_wrap_headers=["パターン", "簡易コメント"],
            min_column_widths={"パターン": 72, "簡易コメント": 48},
        )
        append_table_to_worksheet(
            pattern_summary_sheet,
            "改善対象パターンTOP3",
            pattern_summary["improvement_targets"],
            ["順位", "繰り返し", "繰り返し回数", "繰り返し率(%)", "繰り返し率区分", "件数", "全体比率(%)", "平均処理時間(分)", "平均処理時間差分(分)", "改善優先度スコア", "全体影響度(%)", "最短処理", "確認区分", "簡易コメント", "パターン"],
            start_row=next_row,
            description="繰り返し率が一定以上で、平均処理時間も全体平均より長い改善候補パターンです。",
            no_wrap_headers=["パターン", "簡易コメント"],
            min_column_widths={"パターン": 72, "簡易コメント": 48},
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
            "count",
            "ratio",
            "cumulative_case_ratio_pct",
            "avg_case_duration_min",
            "avg_case_duration_diff_min",
            "improvement_priority_score",
            "overall_impact_pct",
            "fastest_pattern_flag",
            "std_case_duration_min",
            "min_case_duration_min",
            "max_case_duration_min",
            "p75_case_duration_min",
            "p90_case_duration_min",
            "p95_case_duration_min",
            "simple_comment",
            "pattern",
        ],
    )
    append_table_to_worksheet(
        pattern_sheet,
        REPORT_SHEET_NAMES["pattern"],
        pattern_rows,
        pattern_headers,
        description="パターン / バリアントを 1 つの一覧にまとめ、繰り返し回数・繰り返し率・確認区分・件数・比率・累積カバー率・処理時間の代表値と代表ルートを比較できます。",
        no_wrap_headers=["パターン", "簡易コメント"],
        min_column_widths={"パターン": 80, "簡易コメント": 48},
    )

    variant_by_pattern = {
        str(variant_item.get("pattern") or "").strip(): variant_item
        for variant_item in export_variant_items
    }
    pattern_detail_count = resolve_pattern_detail_sheet_count(
        pattern_display_limit,
        len(selected_analysis.get("rows", [])),
    )
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

def _append_bottleneck_export_sheet(workbook, context):
    bottleneck_sheet = _create_report_sheet(workbook, REPORT_SHEET_NAMES["bottleneck"])
    activity_bottleneck_rows, activity_bottleneck_headers = localize_report_rows(
        build_bottleneck_export_rows(
            context["bottleneck_summary"]["activity_bottlenecks"],
            "activity",
        ),
        ["rank", "activity", "count", "case_count", "avg_duration_text", "median_duration_text", "max_duration_text"],
    )
    next_row = append_table_to_worksheet(
        bottleneck_sheet,
        "アクティビティボトルネック",
        activity_bottleneck_rows,
        activity_bottleneck_headers,
        description="アクティビティ単位で所要時間が大きい箇所を並べています。",
    )
    transition_bottleneck_rows, transition_bottleneck_headers = localize_report_rows(
        build_bottleneck_export_rows(
            context["bottleneck_summary"]["transition_bottlenecks"],
            "transition_label",
        ),
        ["rank", "transition_label", "count", "case_count", "avg_duration_text", "median_duration_text", "max_duration_text"],
    )
    append_table_to_worksheet(
        bottleneck_sheet,
        "遷移ボトルネック",
        transition_bottleneck_rows,
        transition_bottleneck_headers,
        start_row=next_row,
        description="前後遷移ごとの平均所要時間・中央値・最大値を比較できます。",
    )

def _append_impact_export_sheet(workbook, context):
    impact_sheet = _create_report_sheet(workbook, REPORT_SHEET_NAMES["impact"])
    impact_rows, impact_headers = localize_report_rows(
        [
            {
                "rank": impact_row["rank"],
                "transition": impact_row["transition_label"],
                "case_count": impact_row["case_count"],
                "avg_duration": impact_row["avg_duration_text"],
                "max_duration": impact_row["max_duration_text"],
                "impact_score": impact_row["impact_score"],
                "impact_share_pct": impact_row["impact_share_pct"],
            }
            for impact_row in context["impact_summary"]["rows"]
        ],
        ["rank", "transition", "case_count", "avg_duration", "max_duration", "impact_score", "impact_share_pct"],
    )
    append_table_to_worksheet(
        impact_sheet,
        REPORT_SHEET_NAMES["impact"],
        impact_rows,
        impact_headers,
        description="改善インパクトが高い遷移を優先順位付きで確認できます。",
    )

def _append_drilldown_export_sheet(
    workbook,
    context,
    run_data,
    filter_params,
    selected_activity="",
    drilldown_limit=20,
):
    selected_activity_name = str(selected_activity or "").strip()
    drilldown_rows = []
    drilldown_title = REPORT_SHEET_NAMES["drilldown"]

    if context["from_activity"] and context["to_activity"]:
        drilldown_title = f"遷移ドリルダウン: {context['from_activity']} → {context['to_activity']}"
        drilldown_rows = query_transition_case_drilldown(
            run_data["prepared_parquet_path"],
            from_activity=context["from_activity"],
            to_activity=context["to_activity"],
            limit=max(0, int(drilldown_limit)),
            filter_params=filter_params,
            filter_column_settings=run_data.get("column_settings"),
            variant_pattern=context["variant_pattern"],
        )
    elif selected_activity_name:
        drilldown_title = f"アクティビティドリルダウン: {selected_activity_name}"
        drilldown_rows = query_activity_case_drilldown(
            run_data["prepared_parquet_path"],
            activity=selected_activity_name,
            limit=max(0, int(drilldown_limit)),
            filter_params=filter_params,
            filter_column_settings=run_data.get("column_settings"),
            variant_pattern=context["variant_pattern"],
        )

    if not drilldown_rows:
        return

    drilldown_sheet = _create_report_sheet(workbook, REPORT_SHEET_NAMES["drilldown"])
    drilldown_rows, drilldown_headers = localize_report_rows(
        drilldown_rows,
        ["case_id", "activity", "next_activity", "duration_text", "from_time", "to_time"],
    )
    append_table_to_worksheet(
        drilldown_sheet,
        drilldown_title,
        drilldown_rows,
        drilldown_headers,
        description="選択中アクティビティ / 遷移に該当するケースの明細です。",
    )

def _append_case_trace_export_sheet(workbook, run_data, case_id=""):
    normalized_case_id = str(case_id or "").strip()
    if not normalized_case_id:
        return

    case_trace = query_case_trace_details(
        run_data["prepared_parquet_path"],
        normalized_case_id,
    )
    if not case_trace.get("found"):
        return

    case_trace_sheet = _create_report_sheet(workbook, REPORT_SHEET_NAMES["case_trace"])
    next_row = append_key_value_rows(
        case_trace_sheet,
        "ケース概要",
        [
            (REPORT_HEADER_LABELS["case_id"], case_trace["case_id"]),
            (REPORT_HEADER_LABELS["event_count"], case_trace["summary"]["event_count"]),
            (REPORT_HEADER_LABELS["total_duration"], case_trace["summary"]["total_duration_text"]),
            (REPORT_HEADER_LABELS["start_time"], case_trace["summary"]["start_time"]),
            (REPORT_HEADER_LABELS["end_time"], case_trace["summary"]["end_time"]),
        ],
        description="指定したケースの概要情報です。",
    )
    case_trace_event_rows, case_trace_event_headers = localize_report_rows(
        case_trace["events"],
        ["case_id", "activity", "next_activity", "start_time", "end_time", "duration_text"],
    )
    append_table_to_worksheet(
        case_trace_sheet,
        "通過イベント",
        case_trace_event_rows,
        case_trace_event_headers,
        start_row=next_row,
        description="ケース内で通過したイベントを時系列順に並べています。",
    )

def _append_detail_export_analysis_sheets(workbook, context, run_data, filter_params, pattern_display_limit="10"):
    export_sheet_keys = context["export_sheet_keys"]
    if "frequency" in export_sheet_keys:
        _append_frequency_export_sheet(workbook, context, run_data, filter_params)
    if "transition" in export_sheet_keys:
        _append_transition_export_sheet(workbook, context)
    if "pattern" in export_sheet_keys:
        _append_pattern_export_sheets(
            workbook,
            context,
            run_data,
            filter_params,
            pattern_display_limit=pattern_display_limit,
        )
    if "bottleneck" in export_sheet_keys:
        _append_bottleneck_export_sheet(workbook, context)
    if "impact" in export_sheet_keys:
        _append_impact_export_sheet(workbook, context)
