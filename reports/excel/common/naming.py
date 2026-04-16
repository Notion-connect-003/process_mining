from pathlib import Path

from reports.excel.common.display_names import resolve_analysis_display_name


def sanitize_workbook_sheet_name(sheet_name):
    invalid_characters = set('[]:*?/\\')
    normalized_name = "".join(
        "_" if character in invalid_characters else character
        for character in str(sheet_name or "").strip()
    )
    normalized_name = normalized_name or "Sheet"
    return normalized_name[:31]


def sanitize_file_name_component(value):
    invalid_characters = set('<>:"/\\|?*')
    normalized_value = "".join(
        "_" if character in invalid_characters else character
        for character in str(value or "").strip()
    )
    return normalized_value.strip(" .") or "analysis"


def build_analysis_excel_file_name(source_file_name, analysis_key, analysis_name="", suffix=""):
    source_stem = sanitize_file_name_component(Path(str(source_file_name or "analysis")).stem)
    display_name = sanitize_file_name_component(
        resolve_analysis_display_name(analysis_key, analysis_name)
    )
    normalized_suffix = sanitize_file_name_component(suffix) if str(suffix or "").strip() else ""
    return f"{source_stem}_{display_name}{normalized_suffix}.xlsx"
