from datetime import datetime, timezone
import json
import re

import httpx
import pandas as pd

from app.services.analysis_queries import (
    get_analysis_data,
    get_bottleneck_summary,
    get_dashboard_summary,
    get_impact_summary,
    get_root_cause_summary,
    get_rule_based_insights_summary,
    get_variant_items,
)
from excel.detail.report import build_transition_display_label
from excel.common import REPORT_SHEET_NAMES, normalize_excel_cell_value
from app.services.cache_keys import build_filter_cache_key
from app.services.ai_actions import build_ai_recommended_actions
from app.services.ai_context import (
    build_ai_context_summary as build_ai_context_summary_impl,
    serialize_ai_prompt_rows as serialize_ai_prompt_rows_impl,
)
from app.services.ai_fallback import build_ai_fallback_text
from app.services.llm_helpers import request_ollama_insights_text

serialize_ai_prompt_rows = serialize_ai_prompt_rows_impl

AI_MAX_TEXT_LENGTH = 12000
AI_PROMPT_FIELD_MAX_LENGTH = 220
AI_PATTERN_ROUTE_MAX_STEPS = 10
AI_PATTERN_ROUTE_HEAD_STEPS = 5
AI_PATTERN_ROUTE_TAIL_STEPS = 4
AI_FALLBACK_CACHE_MODES = {"fallback", "rule_based"}
DURATION_UNIT_VALIDATION_TOLERANCE = 0.75
DURATION_MINUTE_FIELD_CANDIDATES = (
    ("avg_duration_min", "平均所要時間"),
    ("平均所要時間(分)", "平均所要時間"),
    ("平均処理時間(分)", "平均所要時間"),
    ("median_duration_min", "中央値所要時間"),
    ("中央値所要時間(分)", "中央値所要時間"),
    ("中央値処理時間(分)", "中央値所要時間"),
    ("avg_case_duration_min", "平均ケース所要時間"),
    ("平均ケース所要時間(分)", "平均ケース所要時間"),
    ("平均ケース処理時間(分)", "平均ケース所要時間"),
    ("avg_case_duration_diff_min", "平均所要時間差分"),
    ("平均所要時間差分(分)", "平均所要時間差分"),
    ("平均処理時間差分(分)", "平均所要時間差分"),
    ("total_duration_min", "合計所要時間"),
    ("合計所要時間(分)", "合計所要時間"),
    ("改善検討時間規模(分)", "改善検討時間規模"),
)
DURATION_UNIT_PATTERN = re.compile(r"(?<![\d.])([0-9][0-9,]*(?:\.\d+)?)\s*(時間|日)")

PATTERN_AI_ROW_KEYS = (
    "順位",
    "rank",
    "処理順パターン",
    "パターン",
    "pattern",
    "ケース数",
    "case_count",
    "件数",
    "ケース比率(%)",
    "case_ratio_pct",
    "全体比率(%)",
    "累積カバー率(%)",
    "cumulative_case_ratio_pct",
    "平均ケース処理時間(分)",
    "平均ケース所要時間(分)",
    "avg_case_duration_min",
    "平均処理時間差分(分)",
    "平均所要時間差分(分)",
    "avg_case_duration_diff_min",
    "改善優先度スコア",
    "improvement_priority_score",
    "全体影響度(%)",
    "overall_impact_pct",
    "繰り返し",
    "repeat_flag",
    "繰り返し回数",
    "repeat_count",
    "繰り返し率(%)",
    "repeat_rate_pct",
    "確認区分",
    "review_flag",
    "簡易コメント",
    "simple_comment",
)

VARIANT_AI_ROW_KEYS = (
    "variant_id",
    "activities",
    "activity_count",
    "pattern",
    "count",
    "ratio",
    "avg_case_duration_text",
    "repeat_flag",
)


def _coerce_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_minutes_for_ai(minutes):
    safe_minutes = _coerce_float(minutes)
    if safe_minutes is None:
        return ""

    if safe_minutes >= 1440:
        return f"{safe_minutes:.2f}分（約{safe_minutes / 60:.2f}時間 / 約{safe_minutes / 1440:.2f}日）"
    if safe_minutes >= 60:
        return f"{safe_minutes:.2f}分（約{safe_minutes / 60:.2f}時間）"
    return f"{safe_minutes:.2f}分"


def _is_near_number(value, expected, tolerance=DURATION_UNIT_VALIDATION_TOLERANCE):
    try:
        numeric_value = float(value)
        numeric_expected = float(expected)
    except (TypeError, ValueError):
        return False
    return abs(numeric_value - numeric_expected) <= max(float(tolerance), abs(numeric_expected) * 0.02)


def _parse_ai_number(value):
    try:
        return float(str(value or "").replace(",", ""))
    except (TypeError, ValueError):
        return None


def _is_duration_minute_key(key):
    normalized_key = str(key or "").strip().lower()
    if not normalized_key:
        return False
    if normalized_key.endswith("_min") and ("duration" in normalized_key or "impact" in normalized_key):
        return True
    if "(分)" not in normalized_key:
        return False
    return any(
        token in normalized_key
        for token in ("所要時間", "処理時間", "時間差分", "検討時間規模", "時間インパクト")
    )


def _collect_duration_minute_values(value, collected=None):
    if collected is None:
        collected = []
    if isinstance(value, dict):
        for key, child_value in value.items():
            if _is_duration_minute_key(key):
                safe_value = _coerce_float(child_value)
                if safe_value is not None and safe_value > 0:
                    collected.append(safe_value)
            elif isinstance(child_value, (dict, list, tuple)):
                _collect_duration_minute_values(child_value, collected)
    elif isinstance(value, (list, tuple)):
        for child_value in value:
            _collect_duration_minute_values(child_value, collected)
    return collected


def find_ai_duration_unit_issue(text, ai_context):
    """Return a validation error when AI text clearly treats minute values as hours/days."""
    normalized_text = str(text or "")
    if not normalized_text:
        return ""

    duration_minutes = _collect_duration_minute_values(
        {
            "analysis_rows": ai_context.get("analysis_rows", []),
            "variant_items": ai_context.get("variant_items", []),
            "dashboard_summary": ai_context.get("dashboard_summary", {}),
            "bottleneck_summary": ai_context.get("bottleneck_summary", {}),
            "impact_summary": ai_context.get("impact_summary", {}),
            "root_cause_summary": ai_context.get("root_cause_summary", {}),
        }
    )
    duration_minutes = sorted({round(value, 2) for value in duration_minutes if value >= 60.0})
    if not duration_minutes:
        return ""

    for match in DURATION_UNIT_PATTERN.finditer(normalized_text):
        number = _parse_ai_number(match.group(1))
        unit = match.group(2)
        if number is None:
            continue
        for minutes in duration_minutes[:200]:
            if unit == "時間" and _is_near_number(number, minutes):
                return f"{minutes:g}分を{number:g}時間として表現している可能性があります。"
            if unit == "日" and (
                _is_near_number(number, minutes / 60.0)
                or _is_near_number(number, minutes / 24.0)
            ):
                return f"{minutes:g}分を{number:g}日として表現している可能性があります。"
    return ""


def build_duration_validation_retry_prompt(prompt, validation_issue):
    return f"""{prompt}

## 前回出力の修正指示
前回の解説には時間単位の誤変換が含まれる可能性があります: {validation_issue}
- 入力データの分単位の数値を、そのまま「時間」や「日」と書かないでください。
- 時間は 分 ÷ 60、日は 分 ÷ 1440 で換算してください。
- 「時間表示テキスト」がある場合は、その表記を優先してください。
- 迷う場合は「表の分単位の値を確認してください」と書き、誤った換算を断定しないでください。
"""


def _truncate_ai_text(value, max_length=AI_PROMPT_FIELD_MAX_LENGTH):
    text = str(value or "").strip()
    if len(text) <= max_length:
        return text
    keep = max(20, int(max_length) - 1)
    return f"{text[:keep]}…"


def _compact_pattern_route(value):
    text = str(value or "").strip()
    if not text:
        return ""

    steps = [step.strip() for step in text.split("→") if step.strip()]
    if len(steps) > AI_PATTERN_ROUTE_MAX_STEPS:
        text = "→".join(
            steps[:AI_PATTERN_ROUTE_HEAD_STEPS]
            + ["…"]
            + steps[-AI_PATTERN_ROUTE_TAIL_STEPS:]
        )
    return _truncate_ai_text(text)


def _normalize_ai_prompt_value(key, value):
    normalized_key = str(key or "")
    if normalized_key in {"処理順パターン", "パターン", "pattern"}:
        return _compact_pattern_route(value)
    if isinstance(value, str):
        return _truncate_ai_text(value)
    if isinstance(value, list):
        return [_truncate_ai_text(item, 80) for item in value[:12]]
    return normalize_excel_cell_value(value)


def _compact_ai_row(row, allowed_keys=None):
    if not isinstance(row, dict):
        return normalize_excel_cell_value(row)

    source_keys = allowed_keys or tuple(row.keys())
    compacted = {}
    for key in source_keys:
        if key not in row:
            continue
        value = row.get(key)
        if value in (None, ""):
            continue
        compacted[str(key)] = _normalize_ai_prompt_value(key, value)
    return compacted


def compact_analysis_rows_for_prompt(analysis_key, rows):
    normalized_analysis_key = str(analysis_key or "").strip().lower()
    if normalized_analysis_key == "pattern":
        return [_compact_ai_row(row, PATTERN_AI_ROW_KEYS) for row in list(rows or [])]
    return [_compact_ai_row(row) for row in list(rows or [])]


def compact_variant_rows_for_prompt(rows):
    return [_compact_ai_row(row, VARIANT_AI_ROW_KEYS) for row in list(rows or [])]


def serialize_compact_ai_prompt_rows(rows, max_items=5):
    compacted_rows = [
        _compact_ai_row(row)
        for row in list(rows or [])[: max(0, int(max_items or 0))]
    ]
    return json.dumps(compacted_rows, ensure_ascii=False, separators=(",", ":"))


def serialize_named_ai_prompt_rows(rows, max_items=5):
    return json.dumps(
        [
            normalize_excel_cell_value(row)
            for row in list(rows or [])[: max(0, int(max_items or 0))]
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def enrich_rows_with_duration_display(rows):
    enriched_rows = []
    for row in list(rows or []):
        if not isinstance(row, dict):
            enriched_rows.append(row)
            continue

        enriched_row = dict(row)
        display_parts = []
        for field_name, label in DURATION_MINUTE_FIELD_CANDIDATES:
            if field_name not in row:
                continue
            formatted_duration = _format_minutes_for_ai(row.get(field_name))
            if formatted_duration:
                display_parts.append(f"{label}: {formatted_duration}")
        if display_parts:
            enriched_row["時間表示テキスト"] = " / ".join(display_parts)
        enriched_rows.append(enriched_row)
    return enriched_rows





def _build_group_comparison_lines(group_summary):
    return []



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
    return build_ai_context_summary_impl(
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
            "focus": "頻度上位のアクティビティと全体の偏りを説明してください。",
            "priority": "件数の多い活動、所要時間の長い活動、重要なボトルネックを優先してください。",
            "actions": "次に見るべき遷移やケース確認ポイントを具体的に示してください。",
            "constraint": "頻度分析では「バリアント」「パターン」という用語は使わず、アクティビティ単位で説明してください。",
            "variant_label": "主要処理経路",
            "coverage_label": "上位10処理経路カバー率",
        },
        "transition": {
            "focus": "詰まりやすい遷移と前後のつながりを説明してください。",
            "priority": "遷移件数、所要時間、改善インパクトの高い遷移を優先してください。",
            "actions": "次に掘るべき遷移やケース確認ポイントを具体的に示してください。",
            "constraint": "前後処理分析では「バリアント」「パターン」という用語は使わず、遷移単位で説明してください。",
            "variant_label": "主要処理経路",
            "coverage_label": "上位10処理経路カバー率",
        },
        "pattern": {
            "focus": "主要な処理順と偏りの大きいパターンを説明してください。",
            "priority": "件数、所要時間、改善検討対象の時間規模が大きい順に説明してください。",
            "actions": "追加で確認すべきケースやルートの観点を具体的に示してください。",
            "constraint": "",
            "variant_label": "主要バリアント",
            "coverage_label": "上位10バリアントカバー率",
        },
    }
    focus_config = focus_map.get(analysis_key, focus_map["frequency"])
    group_columns = ai_context.get("group_columns", [])
    group_summary = ai_context.get("group_summary", [])
    comparable_group_rows = [row for row in group_summary if str(row.get("value") or row.get("値") or "") != "全体"]
    group_prompt_section = ""
    group_instruction = ""
    if len(comparable_group_rows) >= 2:
        group_prompt_section = f"\n\n## グループ比較（グルーピング軸: {"、".join(group_columns)}）\n{serialize_compact_ai_prompt_rows(group_summary, max_items=10)}"
        group_instruction = "\nグループ間比較に根拠がある場合のみ差分を自然文で説明してください。"
    constraint_line = f"- {focus_config['constraint']}" if focus_config.get("constraint") else ""
    analysis_rows_for_prompt = compact_analysis_rows_for_prompt(
        analysis_key,
        enrich_rows_with_duration_display(ai_context["analysis_rows"]),
    )
    variant_rows_for_prompt = compact_variant_rows_for_prompt(
        enrich_rows_with_duration_display(ai_context["variant_items"]),
    )
    return f"""あなたはプロセスマイニングの分析結果を要約するアナリストです。以下の情報に基づき、日本語で簡潔に解説してください。
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
- 平均ケース所要時間: {ai_context['dashboard_summary'].get('avg_case_duration_text', '0s')}
- {focus_config['coverage_label']}: {float(ai_context['dashboard_summary'].get('top10_variant_coverage_pct', 0.0)):.2f}%

## 現在の分析結果
{serialize_compact_ai_prompt_rows(analysis_rows_for_prompt, max_items=7)}

## アクティビティボトルネック
{serialize_compact_ai_prompt_rows(ai_context['bottleneck_summary'].get('activity_bottlenecks', []), max_items=5)}

## 遷移ボトルネック
{serialize_compact_ai_prompt_rows(ai_context['bottleneck_summary'].get('transition_bottlenecks', []), max_items=5)}

## 改善インパクト上位
{serialize_compact_ai_prompt_rows(ai_context['impact_summary'].get('rows', []), max_items=5)}

## Root Cause 候補
{serialize_compact_ai_prompt_rows(ai_context['root_cause_summary'].get('rows', []), max_items=5)}

## {focus_config['variant_label']}
{serialize_compact_ai_prompt_rows(variant_rows_for_prompt, max_items=5)}

## ルールベース要約
{serialize_named_ai_prompt_rows([item.get('text', '') for item in ai_context['insights_summary'].get('items', [])], max_items=5)}{group_prompt_section}

次の4セクションをこの順番で出力してください。
【全体傾向】
【注目ポイント】
【ボトルネック示唆】
【推奨アクション】
推奨アクションは1〜3件、各行を「・」で始めてください。
マークダウン記法（**、##、- のリスト記号、▸ など）は使わず、プレーンテキストで出力してください。
ただしセクション見出しは必ず【】で囲んでください。
強調したい箇所は「」で囲んでください。箇条書きには「・」を使用してください。
時間単位の扱い:
・入力データの分単位の数値は、60で割ると時間、1440で割ると日です。
・例: 1482分は約24.7時間（約1.03日）であり、25日相当ではありません。
・分単位の数値を文章化する場合は、必ず「時間表示テキスト」があればそれを優先してください。
・所要時間はタイムスタンプ1列から算出しており、休憩・待機・営業時間外などの非作業時間を含む可能性がある前提を必要に応じて明記してください。
{group_instruction}
"""


def extract_recommended_actions_from_text(text):
    """Extract recommended actions from AI-generated text."""
    normalized_text = str(text or "").strip()
    if not normalized_text:
        return "", []
    lines = normalized_text.splitlines()
    marker_index = next((index for index, line in enumerate(lines) if line.strip() == "【推奨アクション】"), None)
    if marker_index is None:
        marker_index = next((index for index, line in enumerate(lines) if "推奨アクション" in line.strip() and len(line.strip()) <= 20), None)
    if marker_index is None:
        return normalized_text, []
    main_text = "\n".join(lines[:marker_index]).rstrip()
    actions = []
    for line in lines[marker_index + 1:]:
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
    return re.sub(r"^[▸►▹▶]\s*(.+)$", lambda match: f"【{match.group(1).strip()}】", normalized_text, flags=re.MULTILINE)


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
        cached_mode = str(cached.get("mode") or "").strip().lower()
        if cached_mode not in AI_FALLBACK_CACHE_MODES:
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
    analysis_name = ai_context["analysis_name"]
    if not ai_context["analysis_rows"]:
        payload = {
            **build_empty_ai_summary(analysis_key, analysis_name),
            "generated": True,
            "cached": False,
            "mode": "fallback",
            "provider": "rule-based",
            "generated_at": generated_at,
            "period": ai_context["period_text"],
            "text": build_ai_fallback_text(ai_context),
            "highlights": [item.get("text", "") for item in ai_context["insights_summary"].get("items", []) if item.get("text")][:5],
            "recommended_actions": build_ai_recommended_actions(ai_context),
            "note": "既存集計からの要約を掲載しています。",
        }
        cache[cache_key] = {k: v for k, v in payload.items() if k not in {"generated", "cached"}}
        return payload
    prompt = build_analysis_ai_prompt(ai_context)
    text_generator = generate_text or request_ollama_insights_text
    try:
        validation_issue = ""
        extracted_actions = []
        response_text = ""
        for attempt_index in range(2):
            active_prompt = (
                build_duration_validation_retry_prompt(prompt, validation_issue)
                if attempt_index and validation_issue
                else prompt
            )
            ai_text = text_generator(active_prompt)
            normalized_ai_text = normalize_ai_generated_text(ai_text)
            if not normalized_ai_text or len(normalized_ai_text) > AI_MAX_TEXT_LENGTH:
                raise ValueError("AI response is empty or too long.")
            main_text, extracted_actions = extract_recommended_actions_from_text(normalized_ai_text)
            response_text = main_text or normalized_ai_text
            validation_issue = find_ai_duration_unit_issue(response_text, ai_context)
            if not validation_issue:
                break
        if validation_issue:
            raise ValueError(f"AI duration validation failed: {validation_issue}")
        comparable_group_rows = [
            row
            for row in ai_context.get("group_summary", [])
            if str(row.get("value") or row.get("値") or "") != "全体"
        ]
        if len(comparable_group_rows) >= 2 and "グループ間比較" not in response_text:
            response_text = f"{response_text}\n\nグループ間比較"
        payload = {
            "title": REPORT_SHEET_NAMES["ai_insights"],
            "analysis_key": analysis_key,
            "analysis_name": analysis_name,
            "generated": True,
            "cached": False,
            "mode": "ollama",
            "provider": "Ollama",
            "generated_at": generated_at,
            "period": ai_context["period_text"],
            "text": response_text,
            "highlights": [item.get("text", "") for item in ai_context["insights_summary"].get("items", []) if item.get("text")][:5],
            "recommended_actions": extracted_actions or build_ai_recommended_actions(ai_context),
            "note": "",
        }
    except (httpx.HTTPError, TimeoutError, ValueError, RuntimeError) as exc:
        payload = {
            "title": REPORT_SHEET_NAMES["ai_insights"],
            "analysis_key": analysis_key,
            "analysis_name": analysis_name,
            "generated": True,
            "cached": False,
            "mode": "fallback",
            "provider": f"rule-based ({type(exc).__name__})",
            "generated_at": generated_at,
            "period": ai_context["period_text"],
            "text": build_ai_fallback_text(ai_context),
            "highlights": [item.get("text", "") for item in ai_context["insights_summary"].get("items", []) if item.get("text")][:5],
            "recommended_actions": build_ai_recommended_actions(ai_context),
            "note": f"AI生成に失敗したため、既存集計からの要約を掲載しています（{type(exc).__name__}）。",
        }
    cache[cache_key] = {k: v for k, v in payload.items() if k not in {"generated", "cached"}}
    return payload

