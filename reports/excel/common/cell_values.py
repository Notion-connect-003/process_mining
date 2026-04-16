import re

import pandas as pd
from openpyxl.cell.rich_text import CellRichText, TextBlock
from openpyxl.cell.text import InlineFont


def normalize_excel_cell_value(value):
    if value is None:
        return ""

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if isinstance(value, (list, tuple, set)):
        return " / ".join(str(item) for item in value)

    if isinstance(value, dict):
        return ", ".join(
            f"{key}={normalize_excel_cell_value(item_value)}"
            for key, item_value in value.items()
        )

    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass

    return value


def build_rich_text_with_bold_numbers(text, base_color="1F2937", base_size=10):
    safe_text = str(text or "").strip()
    if not safe_text:
        return safe_text

    number_pattern = re.compile(
        r"(\d[\d,.]*\d?)"
        r"(%|件|分|秒|時間|日|月|年|回|個|ケース|イベント|種類|パターン|s|ms|min|h)?"
    )

    parts = []
    last_end = 0
    for match in number_pattern.finditer(safe_text):
        if match.start() > last_end:
            parts.append(("text", safe_text[last_end:match.start()]))
        parts.append(("number", match.group(0)))
        last_end = match.end()
    if last_end < len(safe_text):
        parts.append(("text", safe_text[last_end:]))

    if not any(kind == "number" for kind, _ in parts):
        return safe_text

    normal_font = InlineFont(sz=base_size, color=base_color)
    bold_font = InlineFont(b=True, sz=base_size, color=base_color)

    rich_parts = []
    for kind, value in parts:
        if not value:
            continue
        rich_parts.append(TextBlock(bold_font if kind == "number" else normal_font, value))

    if not rich_parts:
        return safe_text

    return CellRichText(*rich_parts)
