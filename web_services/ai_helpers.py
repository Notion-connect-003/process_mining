from datetime import datetime, timezone
import json

import httpx
import pandas as pd

from web_services.analysis_queries import (
    build_filter_cache_key,
    get_analysis_data,
    get_bottleneck_summary,
    get_dashboard_summary,
    get_impact_summary,
    get_root_cause_summary,
    get_rule_based_insights_summary,
    get_variant_items,
)
from web_reports.detail_report import build_transition_display_label
from web_reports.excel_common import REPORT_SHEET_NAMES, normalize_excel_cell_value

from 共通スクリプト.analysis_service import get_available_analysis_definitions
from 共通スクリプト.duckdb_service import (
    query_bottleneck_summary,
    query_period_text,
)


def _request_ollama_insights_text(prompt, model="qwen2.5:7b"):
    timeout = httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=5.0)
    with httpx.Client(timeout=timeout) as client:
        response = client.post(
            "http://localhost:11434/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
        )
        response.raise_for_status()
        payload = response.json()
        return str(payload.get("response") or "").strip()


def serialize_ai_prompt_rows(rows, max_items=5):
    serialized_rows = []

    for row in list(rows or [])[: max(0, int(max_items or 0))]:
        if isinstance(row, dict):
            serialized_rows.append(
                {
                    str(key): normalize_excel_cell_value(value)
                    for key, value in row.items()
                }
            )
        else:
            serialized_rows.append(normalize_excel_cell_value(row))

    return json.dumps(serialized_rows, ensure_ascii=False, indent=2)


def build_analysis_ai_prompt(ai_context):
    analysis_key = str(ai_context["analysis_key"]).strip().lower()
    analysis_name = ai_context["analysis_name"]
    focus_map = {
        "frequency": {
            "focus": "件数が集中しているアクティビティと、平均処理時間が長いアクティビティを分けて解釈してください。",
            "priority": "負荷集中、入力不備、担当偏り、差戻し起点を優先して原因仮説を述べてください。",
            "actions": "入口制御、担当割付、事前チェック、差戻し削減に直結する改善アクションを提案してください。",
        },
        "transition": {
            "focus": "前後遷移の処理時間、引き継ぎの詰まり、ループや差戻しを中心に解釈してください。",
            "priority": "最も遅い遷移と改善インパクトが大きい遷移を分けて説明してください。",
            "actions": "承認待ち、差戻し、バッチ処理、手作業の受け渡しを減らす改善アクションを提案してください。",
        },
        "pattern": {
            "focus": "標準ルートと例外ルートの違い、分岐の多さ、例外パターンの発生理由を解釈してください。",
            "priority": "最頻出パターンと、時間が長い例外パターンを比較して説明してください。",
            "actions": "標準化、例外削減、分岐条件の見直しに直結する改善アクションを提案してください。",
        },
    }
    focus_config = focus_map.get(
        analysis_key,
        {
            "focus": "主要な傾向、滞留箇所、改善優先度を解釈してください。",
            "priority": "影響が大きい箇所を数値付きで説明してください。",
            "actions": "現場で着手しやすい改善アクションを提案してください。",
        },
    )

    return f"""あなたはプロセスマイニング結果を業務改善に落とし込むアナリストです。
以下は「{analysis_name}」の詳細画面に対応する分析データです。現場担当者が画面を切り替えても同じ解釈を再利用できるよう、論点がぶれない説明にしてください。

## この分析で重視する視点
- {focus_config['focus']}
- {focus_config['priority']}
- {focus_config['actions']}

## 基本情報
- 分析名: {analysis_name}
- 総ケース数: {int(ai_context['dashboard_summary'].get('total_cases', 0)):,}
- 総イベント数: {int(ai_context['dashboard_summary'].get('total_records', 0)):,}
- 分析期間: {ai_context['period_text']}
- 平均ケース処理時間: {ai_context['dashboard_summary'].get('avg_case_duration_text', '0s')}
- 上位10バリアントカバー率: {float(ai_context['dashboard_summary'].get('top10_variant_coverage_pct', 0.0)):.2f}%

## 現在の分析結果上位
{serialize_ai_prompt_rows(ai_context['analysis_rows'], max_items=7)}

## アクティビティボトルネック
{serialize_ai_prompt_rows(ai_context['bottleneck_summary'].get('activity_bottlenecks', []), max_items=5)}

## 遷移ボトルネック
{serialize_ai_prompt_rows(ai_context['bottleneck_summary'].get('transition_bottlenecks', []), max_items=5)}

## 改善インパクト上位
{serialize_ai_prompt_rows(ai_context['impact_summary'].get('rows', []), max_items=5)}

## Root Cause 候補
{serialize_ai_prompt_rows(ai_context['root_cause_summary'].get('rows', []), max_items=5)}

## 主要バリアント
{serialize_ai_prompt_rows(ai_context['variant_items'], max_items=5)}

## ルールベース要点
{serialize_ai_prompt_rows([item.get('text', '') for item in ai_context['insights_summary'].get('items', [])], max_items=5)}

## 回答形式（必ず以下の3セクションに分けて出力してください）

【全体傾向】
データ全体の傾向を2〜3文で要約してください。具体的な数値（ケース数、イベント数、イベント比率、平均処理時間など）を必ず引用してください。

【注目ポイント】
この分析で特に注目すべきアクティビティや遷移を2〜3点ピックアップし、数値を交えて考察してください。なぜそれが注目に値するのか（高頻度、長時間、高ばらつき等）を明記してください。

【ボトルネック示唆】
処理時間・ばらつき・ボトルネックデータの観点からボトルネック候補を1〜2点示唆してください。現場で起こりやすい原因仮説を添え、詳細確認の推奨を含めてください。

専門用語は使いすぎず、現場担当者が読みやすい自然な日本語で書いてください。
"""


def build_ai_fallback_text(ai_context):
    analysis_key = str(ai_context["analysis_key"]).strip().lower()
    analysis_name = ai_context["analysis_name"]
    dashboard_summary = ai_context["dashboard_summary"]
    period_text = ai_context["period_text"]
    impact_summary = ai_context["impact_summary"]
    bottleneck_summary = ai_context["bottleneck_summary"]
    analysis_rows = ai_context["analysis_rows"]

    top_impact_row = impact_summary["rows"][0] if impact_summary.get("rows") else None
    top_transition_bottleneck = (
        bottleneck_summary["transition_bottlenecks"][0]
        if bottleneck_summary.get("transition_bottlenecks")
        else None
    )
    top_activity_bottleneck = (
        bottleneck_summary["activity_bottlenecks"][0]
        if bottleneck_summary.get("activity_bottlenecks")
        else None
    )
    top_transition_bottleneck_label = build_transition_display_label(
        top_transition_bottleneck
    )
    top_row = analysis_rows[0] if analysis_rows else {}

    if analysis_key == "frequency":
        # 全体傾向
        if top_row:
            overall_trend_text = (
                f"最も頻度の高いアクティビティは「{top_row.get('アクティビティ', '不明')}」"
                f"（{normalize_excel_cell_value(top_row.get('イベント件数', 0))} 件、"
                f"イベント比率 {normalize_excel_cell_value(top_row.get('イベント比率(%)', 0))}%）です。"
            )
        else:
            overall_trend_text = "件数集中の中心アクティビティは特定できませんでした。"

        # 注目ポイント
        attention_lines = []
        if top_row:
            attention_lines.append(
                f"- 「{top_row.get('アクティビティ', '不明')}」に件数が集中しており、"
                f"受付経路や担当者別件数の比較を推奨します。"
            )
        if top_activity_bottleneck:
            attention_lines.append(
                f"- 「{top_activity_bottleneck['activity']}」は平均処理時間が長く、"
                f"処理時間のばらつきにも注意が必要です。"
            )
        if not attention_lines:
            attention_lines.append("- 特に注目すべきアクティビティは検出されませんでした。")

        # ボトルネック示唆
        bottleneck_lines = []
        if top_activity_bottleneck:
            bottleneck_lines.append(
                f"- アクティビティ「{top_activity_bottleneck['activity']}」が"
                f"処理時間の観点でボトルネック候補です。"
                f"前後の入力不備や差戻しが原因の可能性があります。"
            )
        if top_transition_bottleneck_label:
            bottleneck_lines.append(
                f"- 遷移「{top_transition_bottleneck_label}」も滞留が見られます。"
                f"ケースごとのドリルダウンでの確認を推奨します。"
            )
        if not bottleneck_lines:
            bottleneck_lines.append(
                "- 明確なボトルネックは検出されませんでした。"
                "前後処理分析での確認を推奨します。"
            )

    elif analysis_key == "transition":
        # 全体傾向
        if top_transition_bottleneck_label:
            overall_trend_text = (
                f"最も優先度が高い遷移候補は「{top_transition_bottleneck_label}」です。"
            )
        else:
            overall_trend_text = "優先度が高い遷移候補は特定できませんでした。"

        # 注目ポイント
        attention_lines = []
        if top_transition_bottleneck_label:
            attention_lines.append(
                f"- 「{top_transition_bottleneck_label}」の前後で、"
                f"承認待ちや引き継ぎ待ちの内訳に注目してください。"
            )
        if top_impact_row:
            attention_lines.append(
                f"- 改善インパクトが高い「{top_impact_row['transition_label']}」は"
                f"優先的な改善対象です。"
            )
        if not attention_lines:
            attention_lines.append("- 特に注目すべき遷移は検出されませんでした。")

        # ボトルネック示唆
        bottleneck_lines = []
        if top_transition_bottleneck_label:
            bottleneck_lines.append(
                f"- 遷移「{top_transition_bottleneck_label}」が滞留のボトルネック候補です。"
                f"承認待ち・バッチ処理・手作業の受け渡しが原因の可能性があります。"
            )
        if top_impact_row:
            bottleneck_lines.append(
                f"- 改善インパクト分析では「{top_impact_row['transition_label']}」が上位です。"
                f"差戻しや再提出を含む遷移を優先して確認してください。"
            )
        if not bottleneck_lines:
            bottleneck_lines.append(
                "- 明確なボトルネック遷移は検出されませんでした。"
                "パターン分析での確認を推奨します。"
            )

    elif analysis_key == "pattern":
        # 全体傾向
        if top_row:
            overall_trend_text = (
                f"最頻出パターンは「{top_row.get('処理順パターン', top_row.get('パターン', '不明'))}」です。"
            )
        else:
            overall_trend_text = "標準ルートは特定できませんでした。"

        # 注目ポイント
        attention_lines = [
            "- 最頻出パターンと時間が長い例外パターンを比較し、分岐条件を整理してください。",
            "- 差戻しや再提出を含むパターンは、標準ルートへ寄せられるか確認を推奨します。",
        ]

        # ボトルネック示唆
        bottleneck_lines = []
        if top_activity_bottleneck:
            bottleneck_lines.append(
                f"- アクティビティ「{top_activity_bottleneck['activity']}」が"
                f"例外パターンのボトルネック候補です。"
            )
        bottleneck_lines.append(
            "- 繰り返し率が高いパターンは手戻りが多く、"
            "一次判定や入力チェックの前倒しを検討してください。"
        )

    else:
        # 全体傾向
        if top_impact_row:
            overall_trend_text = (
                f"改善インパクト最大の遷移は「{top_impact_row['transition_label']}」で、"
                f"平均所要時間は {top_impact_row['avg_duration_text']} です。"
            )
        else:
            overall_trend_text = "改善インパクト上位の遷移は検出されませんでした。"

        # 注目ポイント
        attention_lines = []
        if top_transition_bottleneck_label:
            attention_lines.append(
                f"- 「{top_transition_bottleneck_label}」の前後で、"
                f"担当待ち・承認待ち・差戻し理由の内訳を確認してください。"
            )
        if top_activity_bottleneck:
            attention_lines.append(
                f"- アクティビティ「{top_activity_bottleneck['activity']}」について、"
                f"担当者別の件数と平均所要時間を比較してください。"
            )
        if not attention_lines:
            attention_lines.append(
                "- 上位パターンと例外パターンを比較し、分岐ポイントを確認してください。"
            )

        # ボトルネック示唆
        bottleneck_lines = []
        if top_transition_bottleneck_label:
            bottleneck_lines.append(
                f"- 遷移「{top_transition_bottleneck_label}」が滞留のボトルネック候補です。"
            )
        if top_activity_bottleneck:
            bottleneck_lines.append(
                f"- アクティビティ「{top_activity_bottleneck['activity']}」も処理時間が長く注意が必要です。"
            )
        if not bottleneck_lines:
            bottleneck_lines.append(
                "- 改善インパクトが高い遷移を優先して、滞留の主因を確認してください。"
            )

    return "\n".join(
        [
            "【全体傾向】",
            (
                f"{analysis_name} を中心に確認すると、対象は "
                f"{int(dashboard_summary.get('total_cases', 0)):,} ケース / "
                f"{int(dashboard_summary.get('total_records', 0)):,} イベントです。"
            ),
            f"分析期間は {period_text} です。",
            overall_trend_text,
            "",
            "【注目ポイント】",
            *attention_lines,
            "",
            "【ボトルネック示唆】",
            *bottleneck_lines,
        ]
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
        "note": "まだ生成していません。現在の分析条件に対する分析コメントを生成すると、画面を切り替えても保持されます。",
    }


def get_cached_ai_summary(run_data, analysis_key, filter_params=None):
    cache_key = (
        str(analysis_key or "").strip().lower(),
        build_filter_cache_key(filter_params),
        None,
    )
    cached_payload = run_data.setdefault("ai_insights_cache", {}).get(cache_key)
    if cached_payload is None:
        return None
    return {
        **cached_payload,
        "generated": True,
        "cached": True,
    }


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
):
    normalized_analysis_key = str(analysis_key or "").strip().lower()
    analysis_definitions = get_available_analysis_definitions()
    resolved_analysis = analysis or get_analysis_data(
        run_data,
        normalized_analysis_key,
        filter_params=filter_params,
        variant_pattern=variant_pattern,
    )
    resolved_prepared_df = prepared_df
    if resolved_prepared_df is None:
        resolved_prepared_df = pd.DataFrame(
            columns=[
                "case_id",
                "activity",
                "duration_sec",
                "start_time",
                "next_time",
                "timestamp",
            ]
        )
    resolved_dashboard_summary = dashboard_summary or get_dashboard_summary(
        run_data,
        filter_params=filter_params,
        prepared_df=resolved_prepared_df,
        variant_pattern=variant_pattern,
    )
    resolved_impact_summary = impact_summary or get_impact_summary(
        run_data,
        filter_params=filter_params,
        prepared_df=resolved_prepared_df,
        variant_pattern=variant_pattern,
    )
    resolved_bottleneck_summary = bottleneck_summary
    if resolved_bottleneck_summary is None:
        if variant_pattern:
            resolved_bottleneck_summary = query_bottleneck_summary(
                run_data["prepared_parquet_path"],
                filter_params=filter_params,
                filter_column_settings=run_data.get("column_settings"),
                variant_pattern=variant_pattern,
                limit=None,
            )
        else:
            resolved_bottleneck_summary = get_bottleneck_summary(
                run_data,
                filter_params=filter_params,
            )
    resolved_root_cause_summary = root_cause_summary or get_root_cause_summary(
        run_data,
        filter_params=filter_params,
        prepared_df=resolved_prepared_df,
        variant_pattern=variant_pattern,
    )
    resolved_analysis_name = (
        analysis_name
        or resolved_analysis.get("analysis_name")
        or analysis_definitions.get(normalized_analysis_key, {})
        .get("config", {})
        .get("analysis_name", analysis_key)
    )
    period_text = query_period_text(
        run_data["prepared_parquet_path"],
        filter_params=filter_params,
        filter_column_settings=run_data.get("column_settings"),
        variant_pattern=variant_pattern,
    )
    resolved_insights_summary = insights_summary or get_rule_based_insights_summary(
        run_data,
        normalized_analysis_key,
        analysis_rows=resolved_analysis.get("rows"),
        filter_params=filter_params,
        prepared_df=resolved_prepared_df,
        variant_pattern=variant_pattern,
        dashboard_summary=resolved_dashboard_summary,
        impact_summary=resolved_impact_summary,
    )

    return {
        "analysis_key": normalized_analysis_key,
        "analysis_name": resolved_analysis_name,
        "analysis_rows": list(resolved_analysis.get("rows", []))[:10],
        "dashboard_summary": resolved_dashboard_summary,
        "impact_summary": resolved_impact_summary,
        "bottleneck_summary": resolved_bottleneck_summary,
        "root_cause_summary": resolved_root_cause_summary,
        "variant_items": (
            list(variant_items)[:5]
            if variant_items is not None
            else list(
                get_variant_items(
                    run_data,
                    filter_params=filter_params,
                    variant_pattern=variant_pattern,
                )
            )[:5]
        ),
        "period_text": period_text,
        "insights_summary": resolved_insights_summary,
    }


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
):
    cache_key = (
        str(analysis_key or "").strip().lower(),
        build_filter_cache_key(filter_params),
        str(variant_pattern or "").strip() or None,
    )
    cache = run_data.setdefault("ai_insights_cache", {})

    if use_cache and not force_refresh and cache_key in cache:
        return {
            **cache[cache_key],
            "generated": True,
            "cached": True,
        }

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
    )
    generated_at = datetime.now(timezone.utc).isoformat()
    fallback_text = build_ai_fallback_text(ai_context)

    if not ai_context["dashboard_summary"].get("has_data"):
        payload = {
            "title": REPORT_SHEET_NAMES["ai_insights"],
            "analysis_key": ai_context["analysis_key"],
            "analysis_name": ai_context["analysis_name"],
            "mode": "rule_based",
            "provider": "",
            "generated_at": generated_at,
            "period": ai_context["period_text"],
            "text": fallback_text,
            "highlights": [
                item["text"] for item in ai_context["insights_summary"].get("items", [])
            ],
            "note": "対象データがないため、既存集計からの要約のみを表示しています。",
        }
        if use_cache:
            cache[cache_key] = payload
        return {
            **payload,
            "generated": True,
            "cached": False,
        }

    request_ai_text = generate_text or _request_ollama_insights_text

    try:
        ai_text = request_ai_text(build_analysis_ai_prompt(ai_context))
        if ai_text:
            payload = {
                "title": REPORT_SHEET_NAMES["ai_insights"],
                "analysis_key": ai_context["analysis_key"],
                "analysis_name": ai_context["analysis_name"],
                "mode": "ollama",
                "provider": "",
                "generated_at": generated_at,
                "period": ai_context["period_text"],
                "text": ai_text,
                "highlights": [
                    item["text"]
                    for item in ai_context["insights_summary"].get("items", [])
                ],
                "note": "現在の分析条件に対応する分析コメントを保存しました。画面を切り替えても同じ条件なら再表示されます。",
            }
            if use_cache:
                cache[cache_key] = payload
            return {
                **payload,
                "generated": True,
                "cached": False,
            }
    except httpx.ConnectError:
        error_message = "分析コメントを生成できなかったため、既存集計からの要約を掲載しています。"
    except Exception as exc:
        error_message = (
            f"分析コメントの生成に失敗したため、ルールベース要約を掲載しています。({exc})"
        )
    else:
        error_message = "分析コメントを生成できなかったため、ルールベース要約を掲載しています。"

    payload = {
        "title": REPORT_SHEET_NAMES["ai_insights"],
        "analysis_key": ai_context["analysis_key"],
        "analysis_name": ai_context["analysis_name"],
        "mode": "rule_based",
        "provider": "",
        "generated_at": generated_at,
        "period": ai_context["period_text"],
        "text": fallback_text,
        "highlights": [item["text"] for item in ai_context["insights_summary"].get("items", [])],
        "note": error_message,
    }
    if use_cache:
        cache[cache_key] = payload
    return {
        **payload,
        "generated": True,
        "cached": False,
    }


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
    )
