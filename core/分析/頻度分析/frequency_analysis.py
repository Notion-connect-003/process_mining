ANALYSIS_CONFIG = {
    "analysis_name": "頻度分析",
    "sheet_name": "頻度分析",
    "output_file_name": "頻度分析.xlsx",
    "display_columns": {
        "activity": "アクティビティ",
        "event_count": "イベント件数",
        "case_count": "ケース数",
        "case_ratio_pct": "ケース比率(%)",
        "total_duration_min": "合計処理時間(分)",
        "avg_duration_min": "平均処理時間(分)",
        "median_duration_min": "中央値処理時間(分)",
        "std_duration_min": "標準偏差(分)",
        "min_duration_min": "最小処理時間(分)",
        "max_duration_min": "最大処理時間(分)",
        "p75_duration_min": "75%点(分)",
        "p90_duration_min": "90%点(分)",
        "p95_duration_min": "95%点(分)",
        "event_ratio_pct": "イベント比率(%)",
    },
}



def create_frequency_analysis(df, group_columns=None):
    # アクティビティごとの件数と処理時間を集計します。
    # group_columns が指定された場合はグルーピングモードとして集計します。
    if group_columns:
        groupby_keys = [col for col in group_columns if col in df.columns] + ["activity"]
    else:
        groupby_keys = ["activity"]

    result = (
        df.groupby(groupby_keys)
        .agg(
            event_count=("activity", "count"),
            case_count=("case_id", "nunique"),
            total_duration_min=("duration_min", "sum"),
            avg_duration_min=("duration_min", "mean"),
            median_duration_min=("duration_min", "median"),
            std_duration_min=("duration_min", "std"),
            min_duration_min=("duration_min", "min"),
            max_duration_min=("duration_min", "max"),
            p75_duration_min=("duration_min", lambda x: x.quantile(0.75)),
            p90_duration_min=("duration_min", lambda x: x.quantile(0.90)),
            p95_duration_min=("duration_min", lambda x: x.quantile(0.95)),
        )
        .reset_index()
    )

    total_events = len(df)
    total_cases = df["case_id"].nunique()
    result["event_ratio_pct"] = (result["event_count"] / total_events * 100).round(2)
    result["case_ratio_pct"] = (
        (result["case_count"] / total_cases * 100).round(2)
        if total_cases > 0
        else 0.0
    )

    numeric_cols = [
        "total_duration_min",
        "avg_duration_min",
        "median_duration_min",
        "min_duration_min",
        "max_duration_min",
        "p75_duration_min",
        "p90_duration_min",
        "p95_duration_min",
    ]
    result[numeric_cols] = result[numeric_cols].round(2)

    # 標準偏差はイベント1件のみの場合 NaN になるため "-" で表示する。
    result["std_duration_min"] = (
        result["std_duration_min"]
        .round(2)
        .where(result["std_duration_min"].notna(), other="-")
    )

    valid_group_cols = [col for col in (group_columns or []) if col in df.columns]
    sort_keys = valid_group_cols + ["event_count", "activity"]
    sort_ascending = [True] * len(valid_group_cols) + [False, True]
    return result.sort_values(sort_keys, ascending=sort_ascending).reset_index(drop=True)
