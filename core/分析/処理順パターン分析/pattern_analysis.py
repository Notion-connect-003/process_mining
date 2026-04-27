ANALYSIS_CONFIG = {
    "analysis_name": "処理順パターン分析",
    "sheet_name": "処理順パターン分析",
    "output_file_name": "処理順パターン分析.xlsx",
    "display_columns": {
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
        "case_count": "ケース数",
        "case_ratio_pct": "ケース比率(%)",
        "cumulative_case_ratio_pct": "累積カバー率(%)",
        "avg_case_duration_min": "平均ケース処理時間(分)",
        "median_case_duration_min": "中央値ケース処理時間(分)",
        "std_case_duration_min": "標準偏差ケース処理時間(分)",
        "min_case_duration_min": "最小ケース処理時間(分)",
        "max_case_duration_min": "最大ケース処理時間(分)",
        "p75_case_duration_min": "75%点ケース処理時間(分)",
        "p90_case_duration_min": "90%点ケース処理時間(分)",
        "p95_case_duration_min": "95%点ケース処理時間(分)",
        "pattern": "処理順パターン",
    },
}

FLOW_PATH_SEPARATOR = "→"
LOW_REPEAT_RATE_THRESHOLD_PCT = 10.0
HIGH_REPEAT_RATE_THRESHOLD_PCT = 30.0
IMPROVEMENT_TARGET_REPEAT_RATE_THRESHOLD_PCT = 10.0
PATTERN_ORDERED_COLUMNS = [
    "repeat_flag",
    "repeat_count",
    "repeat_rate_pct",
    "repeat_rate_band",
    "review_flag",
    "avg_case_duration_diff_min",
    "improvement_priority_score",
    "overall_impact_pct",
    "fastest_pattern_flag",
    "simple_comment",
    "step_count",
    "repeated_activities",
    "case_count",
    "case_ratio_pct",
    "cumulative_case_ratio_pct",
    "avg_case_duration_min",
    "median_case_duration_min",
    "std_case_duration_min",
    "min_case_duration_min",
    "max_case_duration_min",
    "p75_case_duration_min",
    "p90_case_duration_min",
    "p95_case_duration_min",
    "pattern",
]


def _split_pattern(pattern):
    return [
        step.strip()
        for step in str(pattern or "").split(FLOW_PATH_SEPARATOR)
        if str(step or "").strip()
    ]


def _has_repeated_step(pattern):
    steps = _split_pattern(pattern)
    return len(set(steps)) < len(steps)


def _count_repeated_steps(pattern):
    steps = _split_pattern(pattern)
    if not steps:
        return 0
    return max(0, len(steps) - len(set(steps)))


def _count_steps(pattern):
    """パターン内のステップ数（アクティビティ数）を返す。"""
    return len(_split_pattern(pattern))


def _find_repeated_activities(pattern):
    """パターン内で繰り返されているアクティビティ名をカンマ区切りで返す。"""
    steps = _split_pattern(pattern)
    if not steps:
        return ""
    from collections import Counter

    counts = Counter(steps)
    repeated = sorted(name for name, count in counts.items() if count >= 2)
    return ", ".join(repeated)


def _calculate_repeat_rate_pct(pattern):
    steps = _split_pattern(pattern)
    if not steps:
        return 0.0
    repeat_count = _count_repeated_steps(pattern)
    return round(repeat_count / len(steps) * 100, 2)


def _resolve_repeat_rate_band(repeat_rate_pct):
    safe_repeat_rate_pct = float(repeat_rate_pct or 0.0)
    if safe_repeat_rate_pct < LOW_REPEAT_RATE_THRESHOLD_PCT:
        return "0〜10%"
    if safe_repeat_rate_pct < HIGH_REPEAT_RATE_THRESHOLD_PCT:
        return "10〜30%"
    return "30%以上"


def _resolve_review_flag(repeat_rate_pct):
    if float(repeat_rate_pct or 0.0) >= HIGH_REPEAT_RATE_THRESHOLD_PCT:
        return "要確認"
    return ""


def _build_simple_comment(repeat_count, repeat_rate_pct, avg_duration_diff_min, fastest_pattern_flag):
    safe_repeat_count = int(repeat_count or 0)
    safe_repeat_rate_pct = float(repeat_rate_pct or 0.0)
    safe_avg_duration_diff_min = round(float(avg_duration_diff_min or 0.0), 2)
    is_fastest_pattern = str(fastest_pattern_flag or "").strip() == "○"

    if is_fastest_pattern and safe_repeat_rate_pct < LOW_REPEAT_RATE_THRESHOLD_PCT:
        return "最短・低繰り返しの安定パターン。"

    if safe_repeat_rate_pct >= HIGH_REPEAT_RATE_THRESHOLD_PCT and safe_avg_duration_diff_min > 0:
        return "繰り返し率高・所要時間長め。優先確認候補。"
    if safe_repeat_rate_pct >= LOW_REPEAT_RATE_THRESHOLD_PCT and safe_avg_duration_diff_min > 0:
        return "繰り返しあり・所要時間長め。改善候補。"
    if safe_repeat_count > 0:
        return "繰り返しあり。"
    if safe_avg_duration_diff_min > 0:
        return "所要時間が平均超過。"
    return "安定パターン。"


def enrich_pattern_analysis_result(result_df, group_columns=None):
    if result_df is None or result_df.empty:
        return result_df

    enriched_df = result_df.copy()
    valid_group_cols = [col for col in (group_columns or []) if col in enriched_df.columns]
    total_cases = float(enriched_df["case_count"].sum() or 0.0)
    weighted_avg_case_duration_min = round(
        (
            (
                enriched_df["avg_case_duration_min"].fillna(0)
                * enriched_df["case_count"].fillna(0)
            ).sum()
            / max(total_cases, 1.0)
        ),
        2,
    )
    fastest_case_duration_min = (
        enriched_df["avg_case_duration_min"].fillna(float("inf")).min()
        if "avg_case_duration_min" in enriched_df.columns
        else float("inf")
    )
    enriched_df["step_count"] = enriched_df["pattern"].apply(_count_steps)
    enriched_df["repeated_activities"] = enriched_df["pattern"].apply(
        _find_repeated_activities
    )
    enriched_df["repeat_flag"] = enriched_df["pattern"].apply(
        lambda pattern: "○" if _has_repeated_step(pattern) else ""
    )
    enriched_df["repeat_count"] = enriched_df["pattern"].apply(_count_repeated_steps)
    enriched_df["repeat_rate_pct"] = enriched_df["pattern"].apply(_calculate_repeat_rate_pct)
    enriched_df["repeat_rate_band"] = enriched_df["repeat_rate_pct"].apply(_resolve_repeat_rate_band)
    enriched_df["review_flag"] = enriched_df["repeat_rate_pct"].apply(_resolve_review_flag)
    enriched_df["avg_case_duration_diff_min"] = (
        enriched_df["avg_case_duration_min"].fillna(0) - weighted_avg_case_duration_min
    ).round(2)
    enriched_df["improvement_priority_score"] = (
        enriched_df["repeat_rate_pct"].fillna(0)
        * enriched_df["avg_case_duration_diff_min"].clip(lower=0).fillna(0)
    ).round(2)
    enriched_df["fastest_pattern_flag"] = enriched_df["avg_case_duration_min"].apply(
        lambda value: "○" if round(float(value or 0.0), 2) == round(float(fastest_case_duration_min or 0.0), 2) else ""
    )
    enriched_df["simple_comment"] = enriched_df.apply(
        lambda row: _build_simple_comment(
            row.get("repeat_count", 0),
            row.get("repeat_rate_pct", 0.0),
            row.get("avg_case_duration_diff_min", 0.0),
            row.get("fastest_pattern_flag", ""),
        ),
        axis=1,
    )
    enriched_df["cumulative_case_ratio_pct"] = (
        enriched_df["case_count"].cumsum() / max(total_cases, 1.0) * 100
    ).round(2)
    total_priority_score = float(enriched_df["improvement_priority_score"].sum() or 0.0)
    if total_priority_score > 0:
        enriched_df["overall_impact_pct"] = (
            enriched_df["improvement_priority_score"] / total_priority_score * 100
        ).round(2)
    else:
        enriched_df["overall_impact_pct"] = 0.0

    ordered_columns = (
        valid_group_cols
        + [
            column_name
            for column_name in PATTERN_ORDERED_COLUMNS
            if column_name in enriched_df.columns and column_name not in valid_group_cols
        ]
    )
    return enriched_df[ordered_columns]


def create_pattern_analysis(df, group_columns=None):
    valid_group_cols = [col for col in (group_columns or []) if col in df.columns]

    sorted_df = df.sort_values(["case_id", "sequence_no"])

    if valid_group_cols:
        # グルーピングモード：グループ列の値はケース内先頭行（sequence_no最小）を採用
        group_agg = {col: (col, "first") for col in valid_group_cols}
        case_path = (
            sorted_df
            .groupby("case_id")
            .agg(
                pattern=("activity", lambda s: FLOW_PATH_SEPARATOR.join(s.tolist())),
                **group_agg,
            )
            .reset_index()
        )
    else:
        case_path = (
            sorted_df
            .groupby("case_id")["activity"]
            .apply(lambda series: FLOW_PATH_SEPARATOR.join(series.tolist()))
            .reset_index(name="pattern")
        )

    case_duration = (
        df.groupby("case_id")
        .agg(start_time=("start_time", "min"), next_time=("next_time", "max"))
        .reset_index()
    )
    case_duration["case_total_duration_min"] = (
        (case_duration["next_time"] - case_duration["start_time"]).dt.total_seconds() / 60
    ).round(2)

    merged = case_path.merge(
        case_duration[["case_id", "case_total_duration_min"]],
        on="case_id",
        how="left",
    )

    if valid_group_cols:
        groupby_keys = valid_group_cols + ["pattern"]
    else:
        groupby_keys = ["pattern"]

    result = (
        merged.groupby(groupby_keys)
        .agg(
            case_count=("case_id", "count"),
            avg_case_duration_min=("case_total_duration_min", "mean"),
            median_case_duration_min=("case_total_duration_min", "median"),
            std_case_duration_min=("case_total_duration_min", "std"),
            min_case_duration_min=("case_total_duration_min", "min"),
            max_case_duration_min=("case_total_duration_min", "max"),
            p75_case_duration_min=("case_total_duration_min", lambda x: x.quantile(0.75)),
            p90_case_duration_min=("case_total_duration_min", lambda x: x.quantile(0.90)),
            p95_case_duration_min=("case_total_duration_min", lambda x: x.quantile(0.95)),
        )
        .reset_index()
    )

    total_cases = merged["case_id"].nunique()
    result["case_ratio_pct"] = (result["case_count"] / total_cases * 100).round(2)

    numeric_cols = [
        "avg_case_duration_min",
        "median_case_duration_min",
        "min_case_duration_min",
        "max_case_duration_min",
        "p75_case_duration_min",
        "p90_case_duration_min",
        "p95_case_duration_min",
    ]
    result[numeric_cols] = result[numeric_cols].round(2)
    result["std_case_duration_min"] = (
        result["std_case_duration_min"]
        .round(2)
        .where(result["std_case_duration_min"].notna(), other="-")
    )

    sort_keys = valid_group_cols + ["case_count", "pattern"]
    sort_ascending = [True] * len(valid_group_cols) + [False, True]
    result = result.sort_values(sort_keys, ascending=sort_ascending).reset_index(drop=True)
    return enrich_pattern_analysis_result(result, group_columns=valid_group_cols)
