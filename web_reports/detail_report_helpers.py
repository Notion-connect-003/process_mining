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

from 共通スクリプト.Excel出力.excel_exporter import (
    build_excel_bytes,
    build_summary_sheet_df,
    convert_analysis_result_to_records,
)

from 共通スクリプト.analysis_service import (
    DEFAULT_ANALYSIS_KEYS,
    FLOW_PATTERN_CASE_COUNT_COLUMN,
    FLOW_PATTERN_COLUMN,
    analyze_prepared_event_log,
    build_group_summary,
    detect_group_columns,
    create_analysis_records,
    create_bottleneck_summary,
    create_dashboard_summary,
    create_impact_summary,
    create_log_diagnostics,
    create_rule_based_insights,
    create_root_cause_summary,
    filter_prepared_df,
    filter_prepared_df_by_pattern,
    get_filter_options,
    create_variant_summary,
    create_pattern_flow_snapshot,
    create_pattern_bottleneck_details,
    get_available_analysis_definitions,
    load_prepared_event_log,
    merge_filter_params,
    normalize_filter_params,
    normalize_filter_column_settings,
    select_pattern_rows_for_flow,
)

from 共通スクリプト.duckdb_core import (
    _build_scoped_relation_cte,
    _format_stddev_column,
    _get_parquet_column_names,
    _quote_identifier,
    persist_prepared_parquet,
)
from 共通スクリプト.duckdb_analysis_queries import (
    query_analysis_records,
    query_filter_options,
    query_filtered_meta,
    query_group_summary,
    query_period_text,
    query_transition_records_for_patterns,
    query_variant_summary,
)
from 共通スクリプト.duckdb_detail_queries import (
    query_activity_case_drilldown,
    query_bottleneck_summary,
    query_case_trace_details,
    query_dashboard_summary,
    query_impact_summary,
    query_pattern_bottleneck_details,
    query_root_cause_summary,
    query_transition_case_drilldown,
)

def build_ranked_rows(rows, rank_key="rank"):
    ranked_rows = []
    for index, row in enumerate(rows, start=1):
        ranked_rows.append({
            rank_key: index,
            **row,
        })
    return ranked_rows

def localize_report_headers(headers):
    return [REPORT_HEADER_LABELS.get(header, header) for header in headers]

def localize_report_rows(rows, headers):
    localized_headers = localize_report_headers(headers)
    localized_rows = []

    for row in rows:
        localized_rows.append(
            {
                localized_header: row.get(header)
                for header, localized_header in zip(headers, localized_headers)
            }
        )

    return localized_rows, localized_headers

def build_transition_display_label(row):
    if not row:
        return ""

    transition_label = str(row.get("transition_label") or row.get("transition") or "").strip()
    if transition_label:
        return transition_label

    from_activity = str(row.get("from_activity") or row.get("activity") or "").strip()
    to_activity = str(row.get("to_activity") or row.get("next_activity") or "").strip()
    if from_activity and to_activity:
        return f"{from_activity} → {to_activity}"
    return from_activity or to_activity

def format_duration_text_for_report(duration_sec):
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

def build_bottleneck_export_rows(rows, label_key):
    export_rows = []

    for index, row in enumerate(rows or [], start=1):
        export_rows.append(
            {
                "rank": index,
                label_key: (
                    build_transition_display_label(row)
                    if label_key == "transition_label"
                    else str(row.get(label_key) or "").strip()
                ),
                "count": int(row.get("count") or 0),
                "case_count": int(row.get("case_count") or 0),
                "avg_duration_text": format_duration_text_for_report(row.get("avg_duration_sec")),
                "median_duration_text": format_duration_text_for_report(row.get("median_duration_sec")),
                "max_duration_text": format_duration_text_for_report(row.get("max_duration_sec")),
            }
        )

    return export_rows

def resolve_pattern_detail_sheet_count(pattern_display_limit, available_count):
    normalized_limit = str(pattern_display_limit or "10").strip().lower()

    if normalized_limit == "all":
        requested_count = available_count
    else:
        try:
            requested_count = int(normalized_limit)
        except (TypeError, ValueError):
            requested_count = 10

    requested_count = max(0, requested_count)
    if requested_count > 20:
        requested_count = 20

    return min(available_count, requested_count)

def build_pattern_overview_rows(pattern_rows, variant_items, pattern_column_label, analysis_definitions):
    pattern_config = analysis_definitions.get("pattern", {}).get("config", {})
    pattern_display_columns = pattern_config.get("display_columns", {})
    repeat_flag_label = pattern_display_columns.get("repeat_flag", "繰り返し")
    repeat_count_label = pattern_display_columns.get("repeat_count", "繰り返し回数")
    repeat_rate_label = pattern_display_columns.get("repeat_rate_pct", "繰り返し率(%)")
    repeat_rate_band_label = pattern_display_columns.get("repeat_rate_band", "繰り返し率区分")
    review_flag_label = pattern_display_columns.get("review_flag", "確認区分")
    avg_case_duration_diff_label = pattern_display_columns.get("avg_case_duration_diff_min", "平均処理時間差分(分)")
    improvement_priority_score_label = pattern_display_columns.get("improvement_priority_score", "改善優先度スコア")
    overall_impact_pct_label = pattern_display_columns.get("overall_impact_pct", "全体影響度(%)")
    fastest_pattern_flag_label = pattern_display_columns.get("fastest_pattern_flag", "最短処理")
    simple_comment_label = pattern_display_columns.get("simple_comment", "簡易コメント")
    case_count_label = pattern_display_columns.get("case_count", "ケース数")
    case_ratio_label = pattern_display_columns.get("case_ratio_pct", "ケース比率(%)")
    cumulative_case_ratio_label = pattern_display_columns.get("cumulative_case_ratio_pct", "累積カバー率(%)")
    avg_case_duration_label = pattern_display_columns.get("avg_case_duration_min", "平均ケース処理時間(分)")
    std_case_duration_label = pattern_display_columns.get("std_case_duration_min", "標準偏差ケース処理時間(分)")
    min_case_duration_label = pattern_display_columns.get("min_case_duration_min", "最小ケース処理時間(分)")
    max_case_duration_label = pattern_display_columns.get("max_case_duration_min", "最大ケース処理時間(分)")
    p75_case_duration_label = pattern_display_columns.get("p75_case_duration_min", "75%点ケース処理時間(分)")
    p90_case_duration_label = pattern_display_columns.get("p90_case_duration_min", "90%点ケース処理時間(分)")
    p95_case_duration_label = pattern_display_columns.get("p95_case_duration_min", "95%点ケース処理時間(分)")
    variant_by_pattern = {
        str(variant_item.get("pattern") or "").strip(): variant_item
        for variant_item in (variant_items or [])
    }
    overview_rows = []

    for index, pattern_row in enumerate(pattern_rows or [], start=1):
        pattern_text = str(pattern_row.get(pattern_column_label) or "").strip()
        matched_variant = variant_by_pattern.get(pattern_text, {})
        variant_id = matched_variant.get("variant_id")
        overview_rows.append(
            {
                "rank": index,
                "pattern_variant": (
                    f"Pattern #{index} / Variant #{variant_id}"
                    if variant_id
                    else f"Pattern #{index}"
                ),
                "repeat_flag": pattern_row.get(repeat_flag_label, ""),
                "repeat_count": pattern_row.get(repeat_count_label, 0),
                "repeat_rate_pct": pattern_row.get(repeat_rate_label, 0),
                "repeat_rate_band": pattern_row.get(repeat_rate_band_label, ""),
                "review_flag": pattern_row.get(review_flag_label, ""),
                "avg_case_duration_diff_min": pattern_row.get(avg_case_duration_diff_label, 0),
                "improvement_priority_score": pattern_row.get(improvement_priority_score_label, 0),
                "overall_impact_pct": pattern_row.get(overall_impact_pct_label, 0),
                "fastest_pattern_flag": pattern_row.get(fastest_pattern_flag_label, ""),
                "simple_comment": pattern_row.get(simple_comment_label, ""),
                "count": pattern_row.get(case_count_label, 0),
                "ratio": pattern_row.get(case_ratio_label, 0),
                "cumulative_case_ratio_pct": pattern_row.get(cumulative_case_ratio_label, 0),
                "avg_case_duration_min": pattern_row.get(avg_case_duration_label, 0),
                "std_case_duration_min": pattern_row.get(std_case_duration_label, 0),
                "min_case_duration_min": pattern_row.get(min_case_duration_label, 0),
                "max_case_duration_min": pattern_row.get(max_case_duration_label, 0),
                "p75_case_duration_min": pattern_row.get(p75_case_duration_label, 0),
                "p90_case_duration_min": pattern_row.get(p90_case_duration_label, 0),
                "p95_case_duration_min": pattern_row.get(p95_case_duration_label, 0),
                "pattern": pattern_text,
            }
        )

    return overview_rows

def coerce_report_number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)

def build_pattern_export_summary(pattern_rows, pattern_display_columns):
    repeat_flag_label = pattern_display_columns.get("repeat_flag", "繰り返し")
    repeat_count_label = pattern_display_columns.get("repeat_count", "繰り返し回数")
    repeat_rate_label = pattern_display_columns.get("repeat_rate_pct", "繰り返し率(%)")
    repeat_rate_band_label = pattern_display_columns.get("repeat_rate_band", "繰り返し率区分")
    review_flag_label = pattern_display_columns.get("review_flag", "確認区分")
    avg_case_duration_diff_label = pattern_display_columns.get("avg_case_duration_diff_min", "平均処理時間差分(分)")
    improvement_priority_score_label = pattern_display_columns.get("improvement_priority_score", "改善優先度スコア")
    overall_impact_pct_label = pattern_display_columns.get("overall_impact_pct", "全体影響度(%)")
    fastest_pattern_flag_label = pattern_display_columns.get("fastest_pattern_flag", "最短処理")
    simple_comment_label = pattern_display_columns.get("simple_comment", "簡易コメント")
    case_count_label = pattern_display_columns.get("case_count", "ケース数")
    case_ratio_label = pattern_display_columns.get("case_ratio_pct", "ケース比率(%)")
    cumulative_case_ratio_label = pattern_display_columns.get("cumulative_case_ratio_pct", "累積カバー率(%)")
    avg_case_duration_label = pattern_display_columns.get("avg_case_duration_min", "平均ケース処理時間(分)")
    min_case_duration_label = pattern_display_columns.get("min_case_duration_min", "最小ケース処理時間(分)")
    max_case_duration_label = pattern_display_columns.get("max_case_duration_min", "最大ケース処理時間(分)")
    pattern_label = pattern_display_columns.get("pattern", "処理順パターン")

    comparison_rows = []
    repeated_patterns = []
    improvement_targets = []
    for index, pattern_row in enumerate(pattern_rows or [], start=1):
        comparison_row = {
            "順位": index,
            "繰り返し": pattern_row.get(repeat_flag_label, ""),
            "繰り返し回数": pattern_row.get(repeat_count_label, 0),
            "繰り返し率(%)": pattern_row.get(repeat_rate_label, 0),
            "繰り返し率区分": pattern_row.get(repeat_rate_band_label, ""),
            "件数": pattern_row.get(case_count_label, 0),
            "全体比率(%)": pattern_row.get(case_ratio_label, 0),
            "平均処理時間(分)": pattern_row.get(avg_case_duration_label, 0),
            "平均処理時間差分(分)": pattern_row.get(avg_case_duration_diff_label, 0),
            "改善優先度スコア": pattern_row.get(improvement_priority_score_label, 0),
            "全体影響度(%)": pattern_row.get(overall_impact_pct_label, 0),
            "最短処理": pattern_row.get(fastest_pattern_flag_label, ""),
            "最短処理時間(分)": pattern_row.get(min_case_duration_label, 0),
            "最長処理時間(分)": pattern_row.get(max_case_duration_label, 0),
            "確認区分": pattern_row.get(review_flag_label, ""),
            "簡易コメント": pattern_row.get(simple_comment_label, ""),
            "パターン": pattern_row.get(pattern_label, ""),
        }
        comparison_rows.append(comparison_row)
        if str(pattern_row.get(review_flag_label, "")).strip() != "要確認":
            pass
        else:
            repeated_patterns.append(comparison_row)
        if (
            coerce_report_number(pattern_row.get(repeat_rate_label, 0)) >= 10.0
            and coerce_report_number(pattern_row.get(avg_case_duration_diff_label, 0)) > 0
        ):
            improvement_targets.append(comparison_row)

    top3_rows = comparison_rows[:3]
    top3_coverage_pct = (
        coerce_report_number((pattern_rows or [])[2].get(cumulative_case_ratio_label, 0))
        if len(pattern_rows or []) >= 3
        else (
            coerce_report_number((pattern_rows or [])[-1].get(cumulative_case_ratio_label, 0))
            if pattern_rows
            else 0.0
        )
    )
    top10_coverage_pct = (
        coerce_report_number((pattern_rows or [])[9].get(cumulative_case_ratio_label, 0))
        if len(pattern_rows or []) >= 10
        else (
            coerce_report_number((pattern_rows or [])[-1].get(cumulative_case_ratio_label, 0))
            if pattern_rows
            else 0.0
        )
    )
    repeated_case_ratio_pct = round(
        sum(coerce_report_number(problem_row["全体比率(%)"]) for problem_row in repeated_patterns),
        2,
    )
    improvement_targets = sorted(
        improvement_targets,
        key=lambda row: (
            -coerce_report_number(row.get("改善優先度スコア"), 0),
            -coerce_report_number(row.get("繰り返し率(%)"), 0),
            -coerce_report_number(row.get("平均処理時間差分(分)"), 0),
            -coerce_report_number(row.get("件数"), 0),
            row.get("パターン", ""),
        ),
    )[:3]
    fastest_pattern = min(
        comparison_rows,
        key=lambda row: (
            coerce_report_number(row.get("平均処理時間(分)"), float("inf")),
            row.get("順位", 0),
        ),
        default=None,
    )
    coverage_summary_text = (
        f"上位3パターンで {round(top3_coverage_pct, 2):.2f}%、"
        f"上位10パターンで {round(top10_coverage_pct, 2):.2f}% をカバーしています。"
    )

    return {
        "top_patterns": top3_rows,
        "comparison_rows": comparison_rows[:10],
        "repeated_patterns": repeated_patterns,
        "top3_coverage_pct": round(top3_coverage_pct, 2),
        "top10_coverage_pct": round(top10_coverage_pct, 2),
        "coverage_summary_text": coverage_summary_text,
        "repeated_case_ratio_pct": repeated_case_ratio_pct,
        "fastest_pattern": fastest_pattern or {},
        "improvement_targets": improvement_targets,
    }

def calculate_pattern_time_impact_minutes(pattern_row):
    return round(
        max(0.0, coerce_report_number(pattern_row.get("平均処理時間差分(分)"), 0.0))
        * max(0.0, coerce_report_number(pattern_row.get("件数"), 0.0)),
        2,
    )

def build_pattern_issue_row(pattern_row):
    repeat_rate_pct = coerce_report_number(pattern_row.get("繰り返し率(%)"), 0.0)
    duration_diff_min = coerce_report_number(pattern_row.get("平均処理時間差分(分)"), 0.0)
    pattern_text = str(pattern_row.get("パターン") or "").strip()

    if repeat_rate_pct >= 30:
        issue_text = f"繰り返し率が {repeat_rate_pct:.2f}% と高く、手戻りが多いパターンです。"
        cause_text = "差戻しや再確認が発生しやすく、同じ工程を複数回通過している可能性があります。"
        action_text = "差戻し発生条件を洗い出し、一次判定や入力チェックの前倒しを検討してください。"
    elif repeat_rate_pct >= 10:
        issue_text = f"繰り返し率が {repeat_rate_pct:.2f}% あり、再作業が混在しています。"
        cause_text = "一部ケースで確認や承認のやり直しが発生し、処理が伸びている可能性があります。"
        action_text = "繰り返しが起きる工程の条件分岐を整理し、再実行の発生源を減らしてください。"
    else:
        issue_text = f"繰り返しは少ないものの、平均処理時間が全体平均より {duration_diff_min:.2f} 分長いパターンです。"
        cause_text = "特定工程の待ちや滞留により、パターン全体の処理時間が長くなっている可能性があります。"
        action_text = "ボトルネック工程の担当・承認・待機条件を見直し、滞留時間の短縮を優先してください。"

    return {
        "問題点": issue_text,
        "原因": cause_text,
        "改善案": action_text,
        "期待効果（時間短縮・分）": calculate_pattern_time_impact_minutes(pattern_row),
        "対象パターン": pattern_text,
    }

def build_pattern_conclusion_summary(pattern_summary):
    comparison_rows = pattern_summary.get("comparison_rows", [])
    improvement_targets = pattern_summary.get("improvement_targets", [])
    repeated_patterns = pattern_summary.get("repeated_patterns", [])
    fastest_pattern = pattern_summary.get("fastest_pattern", {}) or {}
    top_issue_candidates = improvement_targets[:3] if improvement_targets else comparison_rows[:3]
    issue_rows = [build_pattern_issue_row(row) for row in top_issue_candidates]
    total_impact_minutes = round(
        sum(calculate_pattern_time_impact_minutes(row) for row in improvement_targets),
        2,
    )
    overall_summary = (
        f"上位10パターンで {coerce_report_number(pattern_summary.get('top10_coverage_pct', 0.0)):.2f}% をカバーし、"
        f"要確認パターンは {len(repeated_patterns)} 件、改善対象TOP3で約 "
        f"{round(sum(calculate_pattern_time_impact_minutes(row) for row in improvement_targets[:3]), 2):.2f} 分の短縮余地があります。"
    )

    return {
        "overall_summary": overall_summary,
        "issue_rows": issue_rows,
        "total_impact_minutes": total_impact_minutes,
        "total_impact_hours": round(total_impact_minutes / 60.0, 2),
        "fastest_pattern": fastest_pattern,
        "improvement_targets": improvement_targets[:3],
    }

def build_pattern_dashboard_summary(pattern_summary, pattern_conclusion):
    top3_rows = pattern_summary.get("improvement_targets") or pattern_summary.get("comparison_rows", [])[:3]
    problem_points = [
        issue_row.get("問題点")
        for issue_row in pattern_conclusion.get("issue_rows", [])
        if str(issue_row.get("問題点") or "").strip()
    ]
    if not problem_points and pattern_summary.get("comparison_rows"):
        problem_points = [
            str(row.get("簡易コメント") or "").strip()
            for row in pattern_summary.get("comparison_rows", [])[:3]
            if str(row.get("簡易コメント") or "").strip()
        ]

    return {
        "overall_summary": pattern_conclusion.get("overall_summary", ""),
        "top3_rows": top3_rows,
        "problem_points": problem_points[:3],
        "top10_coverage_pct": pattern_summary.get("top10_coverage_pct", 0),
        "total_impact_minutes": pattern_conclusion.get("total_impact_minutes", 0),
    }
