REPORT_SHEET_NAMES = {
    "summary": "サマリー",
    "ai_insights": "分析コメント",
    "pattern_conclusion": "結論サマリー",
    "pattern_dashboard": "サマリーダッシュボード",
    "frequency": "頻度分析",
    "transition": "前後処理分析",
    "pattern": "処理順パターン分析",
    "pattern_summary": "パターンサマリー",
    "variant": "バリアント分析",
    "bottleneck": "ボトルネック分析",
    "impact": "改善インパクト分析",
    "drilldown": "ドリルダウン",
    "case_trace": "ケース追跡",
}

LOG_DIAGNOSTIC_SHEET_NAMES = {
    "summary": "ログ診断",
    "sample": "ログサンプル",
}

REPORT_HEADER_LABELS = {
    "run_id": "実行ID",
    "analysis_key": "分析種別",
    "analysis_name": "分析名",
    "source_file_name": "元ファイル名",
    "analysis_executed_at": "分析実行日時",
    "exported_at": "出力日時",
    "case_count": "対象ケース数",
    "event_count": "対象イベント数",
    "applied_filters": "適用条件",
    "selected_variant": "選択中バリアント",
    "selected_activity": "選択中アクティビティ",
    "selected_transition": "選択中遷移",
    "selected_case_id": "選択中ケースID",
    "rank": "順位",
    "pattern_variant": "パターン / バリアント",
    "repeat_flag": "繰り返し",
    "repeat_count": "繰り返し回数",
    "repeat_rate_pct": "繰り返し率(%)",
    "repeat_rate_band": "繰り返し率区分",
    "review_flag": "確認区分",
    "avg_case_duration_diff_min": "平均処理時間差分(分)",
    "improvement_priority_score": "改善優先度スコア",
    "overall_impact_pct": "全体影響度(%)",
    "fastest_pattern_flag": "最短処理",
    "simple_comment": "簡易コメント",
    "step_count": "ステップ数",
    "repeated_activities": "繰り返しアクティビティ",
    "variant_id": "バリアントID",
    "count": "件数",
    "case_ratio_pct": "ケース比率(%)",
    "ratio": "比率",
    "cumulative_case_ratio_pct": "累積カバー率(%)",
    "pattern": "パターン",
    "activity_count": "アクティビティ数",
    "avg_case_duration": "平均ケース処理時間",
    "avg_case_duration_min": "平均ケース処理時間(分)",
    "std_case_duration_min": "標準偏差ケース処理時間(分)",
    "min_case_duration_min": "最短処理時間(分)",
    "max_case_duration_min": "最長処理時間(分)",
    "p75_case_duration_min": "75%点ケース処理時間(分)",
    "p90_case_duration_min": "90%点ケース処理時間(分)",
    "p95_case_duration_min": "95%点ケース処理時間(分)",
    "avg_duration": "平均所要時間",
    "avg_duration_text": "平均所要時間",
    "avg_duration_min": "平均所要時間(分)",
    "from_avg_duration_min": "前処理平均時間(分)",
    "to_avg_duration_min": "後処理平均時間(分)",
    "median_duration_min": "中央値所要時間(分)",
    "std_duration_min": "標準偏差(分)",
    "min_duration_min": "最小所要時間(分)",
    "median_duration_text": "中央値所要時間",
    "max_duration": "最大所要時間",
    "max_duration_text": "最大所要時間",
    "max_duration_min": "最大所要時間(分)",
    "total_duration_min": "合計所要時間(分)",
    "p75_duration_min": "75%点(分)",
    "p90_duration_min": "90%点(分)",
    "p95_duration_min": "95%点(分)",
    "impact_score": "改善インパクト",
    "impact_share_pct": "改善インパクト比率(%)",
    "wait_share_pct": "構成比(%)",
    "case_id": "ケースID",
    "from_time": "開始時刻",
    "to_time": "終了時刻",
    "activity": "アクティビティ",
    "next_activity": "次アクティビティ",
    "sequence_no": "ステップ順",
    "transition": "遷移",
    "transition_label": "遷移",
    "duration_text": "所要時間",
    "total_duration": "総処理時間",
    "start_time": "開始時刻",
    "end_time": "終了時刻",
}

APPLIED_FILTERS_NOTE_TEXT = "\n".join(
    [
        "※ 適用条件の種類:",
        "  • 期間フィルター: 開始日 / 終了日",
        "  • グループ/カテゴリーフィルター①②③: CSVの任意カラムで絞り込み（例: 部署=営業部）",
        "  • アクティビティフィルター: 特定アクティビティを含む/除外",
        "  • 開始/終了アクティビティ: ケースの最初/最後のイベント名で絞り込み",
    ]
)

GROUPING_CONDITION_NOTE_TEXT = (
    "※ カラムを選択し値を未選択にすると、そのカラムがグルーピング軸（比較用）になります"
)

ANALYSIS_PRECONDITIONS_TEXT = "\n".join(
    [
        "• 処理時間は、同一ケース内で当該アクティビティの開始時刻から次のアクティビティの開始時刻までの差分として算出しています。",
        "• ケース内の最終アクティビティは、次のイベントが存在しないため処理時間が0分となります。",
        "• 処理時間が0分のイベントも集計対象に含まれています。統計値（平均・中央値等）に影響する点にご留意ください。",
        "• 本分析はフィルター適用後のデータに基づいています。適用条件の詳細はサマリーシートをご参照ください。",
    ]
)

_TERMINOLOGY_ROWS_COMMON = [
    {"用語": "ケース", "説明": "業務プロセスの1つの実行単位（例: 1件の注文、1件の申請）"},
    {"用語": "アクティビティ", "説明": "ケース内で実行される個々の作業ステップ（例: 申請、承認、支払）"},
    {"用語": "イベント", "説明": "特定のケースで特定のアクティビティが実行された1回の記録"},
    {"用語": "処理時間", "説明": "あるアクティビティの開始から次のアクティビティの開始までの所要時間"},
]

_TERMINOLOGY_ROWS_BY_ANALYSIS = {
    "frequency": _TERMINOLOGY_ROWS_COMMON
    + [
        {"用語": "イベント比率(%)", "説明": "全イベント数に対する当該アクティビティのイベント数の割合"},
        {"用語": "ケース比率(%)", "説明": "全ケース数に対する当該アクティビティを含むケース数の割合"},
    ],
    "transition": _TERMINOLOGY_ROWS_COMMON
    + [
        {"用語": "遷移", "説明": "前のアクティビティから次のアクティビティへの移行（前後関係）"},
        {"用語": "遷移比率(%)", "説明": "全遷移件数に対する当該遷移の割合"},
        {"用語": "ケース比率(%)", "説明": "全ケース数に対する当該遷移を含むケース数の割合"},
        {"用語": "前処理平均時間(分)", "説明": "遷移元アクティビティの平均処理時間"},
        {"用語": "後処理平均時間(分)", "説明": "遷移先アクティビティの平均処理時間"},
    ],
    "pattern": _TERMINOLOGY_ROWS_COMMON
    + [
        {"用語": "処理順パターン", "説明": "ケース内のアクティビティの実行順序"},
        {"用語": "ケース比率(%)", "説明": "全ケース数に対する当該パターンのケース数の割合"},
        {"用語": "累積カバー率(%)", "説明": "上位パターンから順に累積したケース数の全体に対する割合"},
        {"用語": "繰り返し率(%)", "説明": "パターン内のステップ数に対する繰り返しアクティビティの延べ回数の割合"},
        {"用語": "改善優先度スコア", "説明": "繰り返し率と処理時間差分を掛け合わせた改善優先度の指標"},
        {"用語": "全体影響度(%)", "説明": "全パターンの改善優先度スコア合計に対する当該パターンの割合"},
    ],
}

TERMINOLOGY_ROWS = _TERMINOLOGY_ROWS_BY_ANALYSIS["frequency"]


def get_terminology_rows(analysis_key=""):
    normalized_key = str(analysis_key or "").strip().lower()
    return _TERMINOLOGY_ROWS_BY_ANALYSIS.get(normalized_key, _TERMINOLOGY_ROWS_COMMON)


def resolve_analysis_display_name(analysis_key, analysis_name=""):
    normalized_analysis_key = str(analysis_key or "").strip().lower()
    if normalized_analysis_key in REPORT_SHEET_NAMES:
        return REPORT_SHEET_NAMES[normalized_analysis_key]
    return str(analysis_name or normalized_analysis_key or "分析").strip() or "分析"
