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
from app.services.ai_actions import build_ai_recommended_actions as build_ai_recommended_actions_impl
from app.services.ai_context import (
    build_ai_context_summary as build_ai_context_summary_impl,
    serialize_ai_prompt_rows as serialize_ai_prompt_rows_impl,
)
from app.services.ai_fallback import build_ai_fallback_text as build_ai_fallback_text_impl
from app.services.llm_helpers import request_ollama_insights_text



def serialize_ai_prompt_rows(rows, max_items=5):
    return serialize_ai_prompt_rows_impl(rows, max_items=max_items)


def build_ai_recommended_actions(ai_context):
    """Build recommended actions from the AI context."""
    return build_ai_recommended_actions_impl(ai_context)


def _build_group_comparison_lines(group_summary):
    return []


def build_ai_fallback_text(ai_context):
    return build_ai_fallback_text_impl(ai_context)


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
            "priority": "件数の多い活動、処理時間の長い活動、重要なボトルネックを優先してください。",
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
            "priority": "件数、処理時間、改善余地の大きい順に説明してください。",
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
        group_prompt_section = f"\n\n## グループ比較（グルーピング軸: {"、".join(group_columns)}）\n{serialize_ai_prompt_rows(group_summary, max_items=10)}"
        group_instruction = "\nグループ間比較に根拠がある場合のみ差分を自然文で説明してください。"
    constraint_line = f"- {focus_config['constraint']}" if focus_config.get("constraint") else ""
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
- 平均ケース処理時間: {ai_context['dashboard_summary'].get('avg_case_duration_text', '0s')}
- {focus_config['coverage_label']}: {float(ai_context['dashboard_summary'].get('top10_variant_coverage_pct', 0.0)):.2f}%

## 現在の分析結果
{serialize_ai_prompt_rows(ai_context['analysis_rows'], max_items=7)}

## アクティビティボトルネック
{serialize_ai_prompt_rows(ai_context['bottleneck_summary'].get('activity_bottlenecks', []), max_items=5)}

## 遷移ボトルネック
{serialize_ai_prompt_rows(ai_context['bottleneck_summary'].get('transition_bottlenecks', []), max_items=5)}

## 改善インパクト上位
{serialize_ai_prompt_rows(ai_context['impact_summary'].get('rows', []), max_items=5)}

## Root Cause 候補
{serialize_ai_prompt_rows(ai_context['root_cause_summary'].get('rows', []), max_items=5)}

## {focus_config['variant_label']}
{serialize_ai_prompt_rows(ai_context['variant_items'], max_items=5)}

## ルールベース要約
{serialize_ai_prompt_rows([item.get('text', '') for item in ai_context['insights_summary'].get('items', [])], max_items=5)}{group_prompt_section}

次の4セクションをこの順番で出力してください。
【全体傾向】
【注目ポイント】
【ボトルネック示唆】
【推奨アクション】
推奨アクションは1〜3件、各行を「・」で始めてください。
マークダウン記法（**、##、- のリスト記号、▸ など）は使わず、プレーンテキストで出力してください。
ただしセクション見出しは必ず【】で囲んでください。
強調したい箇所は「」で囲んでください。箇条書きには「・」を使用してください。
分単位の大きな数値（1440分以上）は日単位に換算して表示し、分単位の生数値は省略してください。
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
        ai_text = text_generator(prompt)
        normalized_ai_text = normalize_ai_generated_text(ai_text)
        main_text, extracted_actions = extract_recommended_actions_from_text(normalized_ai_text)
        response_text = main_text or normalized_ai_text
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
    except (httpx.HTTPError, TimeoutError, ValueError) as exc:
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
            "note": "既存集計からの要約を掲載しています。",
        }
    cache[cache_key] = {k: v for k, v in payload.items() if k not in {"generated", "cached"}}
    return payload

