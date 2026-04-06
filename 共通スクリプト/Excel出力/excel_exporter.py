from io import BytesIO
from pathlib import Path

import pandas as pd
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter


HEADER_FILL = PatternFill(fill_type="solid", fgColor="EDF2F7")
ALT_ROW_FILL = PatternFill(fill_type="solid", fgColor="FBFDFF")
THIN_BORDER = Border(
    left=Side(style="thin", color="D6DEE8"),
    right=Side(style="thin", color="D6DEE8"),
    top=Side(style="thin", color="D6DEE8"),
    bottom=Side(style="thin", color="D6DEE8"),
)


def _autosize_worksheet_columns(worksheet, min_width=10, max_width=40):
    for column_index in range(1, worksheet.max_column + 1):
        column_letter = get_column_letter(column_index)
        measured_width = min_width
        for row_index in range(1, worksheet.max_row + 1):
            cell_value = worksheet.cell(row=row_index, column=column_index).value
            if cell_value in (None, ""):
                continue
            measured_width = max(measured_width, len(str(cell_value)))
        worksheet.column_dimensions[column_letter].width = max(
            min_width,
            min(max_width, measured_width + 2),
        )


def _style_export_worksheet(worksheet):
    worksheet.sheet_view.showGridLines = False
    worksheet.sheet_view.zoomScale = 90

    if worksheet.max_row >= 1:
        for column_index in range(1, worksheet.max_column + 1):
            header_cell = worksheet.cell(row=1, column=column_index)
            header_cell.font = Font(bold=True, color="1F2937")
            header_cell.fill = HEADER_FILL
            header_cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            header_cell.border = THIN_BORDER
        worksheet.row_dimensions[1].height = 22
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = f"A1:{get_column_letter(worksheet.max_column)}{worksheet.max_row}"

    for row_index in range(2, worksheet.max_row + 1):
        row_fill = ALT_ROW_FILL if row_index % 2 == 0 else None
        for column_index in range(1, worksheet.max_column + 1):
            body_cell = worksheet.cell(row=row_index, column=column_index)
            body_cell.alignment = Alignment(vertical="top", wrap_text=True)
            body_cell.border = THIN_BORDER
            if row_fill is not None:
                body_cell.fill = row_fill

    _autosize_worksheet_columns(worksheet)


def format_analysis_result(df, display_columns=None):
    if display_columns is None:
        return df.copy()

    ordered_columns = [
        column_name
        for column_name in display_columns.keys()
        if column_name in df.columns
    ]
    formatted_df = df.loc[:, ordered_columns].copy() if ordered_columns else df.copy()
    return formatted_df.rename(columns=display_columns)


def build_excel_bytes(
    df,
    sheet_name,
):
    buffer = BytesIO()

    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        _style_export_worksheet(writer.sheets[sheet_name])

    return buffer.getvalue()


def export_dataframe_to_excel(
    df,
    output_file,
    sheet_name,
):
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        output_path.write_bytes(build_excel_bytes(df, sheet_name))
    except PermissionError as exc:
        raise PermissionError(
            f"{output_path} に書き込めません。Excel で開いている場合は閉じてから再実行してください。"
        ) from exc

    return output_path


def export_analysis_to_excel(
    df,
    output_root_dir,
    analysis_name,
    output_file_name,
    sheet_name,
    display_columns=None,
):
    output_file = Path(output_root_dir) / analysis_name / output_file_name
    excel_df = format_analysis_result(df, display_columns)
    return export_dataframe_to_excel(excel_df, output_file, sheet_name)


def convert_analysis_result_to_records(
    df,
    display_columns=None,
):
    api_df = format_analysis_result(df, display_columns)
    return api_df.to_dict(orient="records")
