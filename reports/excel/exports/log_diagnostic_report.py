from io import BytesIO

from openpyxl import Workbook

from reports.excel.common import (
    LOG_DIAGNOSTIC_SHEET_NAMES,
    append_key_value_rows,
    append_table_to_worksheet,
    autosize_worksheet_columns,
    initialize_excel_worksheet,
    sanitize_workbook_sheet_name,
)


DEFAULT_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT = 3000
MAX_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT = 50000


def resolve_log_diagnostic_sample_row_limit(raw_value):
    try:
        sample_row_limit = int(str(raw_value or "").strip() or DEFAULT_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT)
    except (TypeError, ValueError):
        sample_row_limit = DEFAULT_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT

    if sample_row_limit <= 0:
        sample_row_limit = DEFAULT_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT

    return min(MAX_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT, sample_row_limit)


def build_log_diagnostic_period_text(diagnostics):
    time_range = (diagnostics or {}).get("time_range") or {}
    min_time = str(time_range.get("min") or "").strip()
    max_time = str(time_range.get("max") or "").strip()
    if not min_time or not max_time:
        return "ケースID / アクティビティ / タイムスタンプ列を選択すると表示します。"
    return f"{min_time} 〜 {max_time}"


def build_log_diagnostic_missing_count_text(diagnostics):
    missing_counts = (diagnostics or {}).get("missing_counts") or {}
    return (
        f"ケースID {missing_counts.get('case_id', '-') if missing_counts.get('case_id') is not None else '-'}"
        f" / アクティビティ {missing_counts.get('activity', '-') if missing_counts.get('activity') is not None else '-'}"
        f" / タイムスタンプ {missing_counts.get('timestamp', '-') if missing_counts.get('timestamp') is not None else '-'}"
    )


def build_log_diagnostic_duplicate_rate_text(diagnostics):
    duplicate_rate = float((diagnostics or {}).get("duplicate_rate") or 0.0)
    return f"{duplicate_rate * 100:.1f}%"


def build_log_diagnostic_filter_rows(profile_payload, preview_limit=30):
    diagnostics = (profile_payload or {}).get("diagnostics") or {}
    diagnostics_filter_rows = {
        str(row.get("slot") or ""): row
        for row in (diagnostics.get("filters") or [])
        if str(row.get("slot") or "").strip()
    }
    column_settings = (profile_payload or {}).get("column_settings") or {}
    filter_definitions = column_settings.get("filters") or []
    rows = []

    for filter_definition in filter_definitions:
        slot = str(filter_definition.get("slot") or "").strip()
        diagnostics_row = diagnostics_filter_rows.get(slot, {})
        options = diagnostics_row.get("options") or []
        option_preview_values = [str(option) for option in options[:preview_limit]]
        option_preview = ", ".join(option_preview_values) if option_preview_values else "-"
        if len(options) > preview_limit:
            option_preview = f"{option_preview} ... 他 {len(options) - preview_limit} 件"

        rows.append(
            {
                "スロット": slot or "-",
                "表示名": str(filter_definition.get("label") or "").strip() or "-",
                "対象列": str(filter_definition.get("column_name") or "").strip() or "未設定",
                "候補数": int(len(options)),
                "候補一覧": option_preview,
            }
        )

    return rows


def build_log_diagnostic_sample_rows(raw_df, sample_row_limit):
    headers = [str(column_name) for column_name in raw_df.columns.tolist()]
    sampled_df = raw_df.head(max(0, int(sample_row_limit))).copy()
    sample_rows = []

    for _, (original_index, row) in enumerate(sampled_df.iterrows(), start=1):
        row_payload = {
            "レコード順": int(original_index) + 1,
        }
        for header in headers:
            row_payload[header] = row.get(header, "")
        sample_rows.append(row_payload)

    return sample_rows, ["レコード順", *headers]


def build_log_diagnostic_workbook_bytes(profile_payload, raw_df, sample_row_limit):
    diagnostics = (profile_payload or {}).get("diagnostics") or {}
    column_settings = (profile_payload or {}).get("column_settings") or {}
    sample_rows, sample_headers = build_log_diagnostic_sample_rows(raw_df, sample_row_limit)
    sample_row_count = len(sample_rows)
    total_record_count = int((diagnostics or {}).get("record_count") or len(raw_df))
    omitted_row_count = max(0, total_record_count - sample_row_count)

    workbook = Workbook()
    summary_sheet = workbook.active
    summary_sheet.title = sanitize_workbook_sheet_name(LOG_DIAGNOSTIC_SHEET_NAMES["summary"])
    initialize_excel_worksheet(summary_sheet)

    summary_rows = [
        ("元ファイル名", profile_payload.get("source_file_name") or ""),
        ("ケースID列", column_settings.get("case_id_column") or "未設定"),
        ("アクティビティ列", column_settings.get("activity_column") or "未設定"),
        ("タイムスタンプ列", column_settings.get("timestamp_column") or "未設定"),
        ("ログレコード数", diagnostics.get("record_count", "-")),
        ("総ケース数", diagnostics.get("case_count", "-")),
        ("アクティビティ種類数", diagnostics.get("activity_type_count", "-")),
        ("ログ期間", build_log_diagnostic_period_text(diagnostics)),
        ("欠損件数", build_log_diagnostic_missing_count_text(diagnostics)),
        ("重複行数", diagnostics.get("duplicate_row_count", 0)),
        ("重複あり/なし", diagnostics.get("duplicate_status", "なし")),
        ("重複除外後レコード数", diagnostics.get("deduplicated_record_count", "-")),
        ("重複率", build_log_diagnostic_duplicate_rate_text(diagnostics)),
        ("ヘッダー一覧", ", ".join(diagnostics.get("headers") or [])),
        ("ログサンプル出力上限", int(sample_row_limit)),
        ("ログサンプル出力件数", sample_row_count),
    ]
    next_row = append_key_value_rows(
        summary_sheet,
        "ログ診断サマリー",
        summary_rows,
        description="トップ画面のログ診断に表示する件数・期間・欠損・重複をまとめています。",
    )

    column_summary_rows = [
        {
            "列名": str(column.get("name") or ""),
            "サンプル値": ", ".join(column.get("sample_values") or []) or "-",
            "ユニーク件数": int(column.get("unique_count") or 0),
            "欠損件数": int(column.get("missing_count") or 0),
        }
        for column in (diagnostics.get("columns") or [])
    ]
    next_row = append_table_to_worksheet(
        summary_sheet,
        "列サマリー",
        column_summary_rows,
        ["列名", "サンプル値", "ユニーク件数", "欠損件数"],
        start_row=next_row,
        description="トップ画面に表示する列ごとのサンプル値・ユニーク件数・欠損件数です。",
        min_column_widths={"列名": 24, "サンプル値": 48},
    )

    sample_sheet = workbook.create_sheet(sanitize_workbook_sheet_name(LOG_DIAGNOSTIC_SHEET_NAMES["sample"]))
    initialize_excel_worksheet(sample_sheet)
    sample_description = (
        f"ログサンプルとして先頭 {sample_row_count} 件を掲載しています。"
        if omitted_row_count <= 0
        else f"ログサンプルとして先頭 {sample_row_count} 件を掲載しています。残り {omitted_row_count} 件は省略しています。"
    )
    append_table_to_worksheet(
        sample_sheet,
        "ログサンプル",
        sample_rows,
        sample_headers,
        description=sample_description,
        min_column_widths={"レコード順": 14},
    )

    for worksheet in workbook.worksheets:
        autosize_worksheet_columns(worksheet)

    output_buffer = BytesIO()
    workbook.save(output_buffer)
    return output_buffer.getvalue()
