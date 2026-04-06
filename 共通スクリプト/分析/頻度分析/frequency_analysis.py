ANALYSIS_CONFIG = {
    "analysis_name": "頻度分析",
    "sheet_name": "頻度分析",
    "output_file_name": "頻度分析.xlsx",
    "display_columns": {
        "activity": "アクティビティ",
        "event_count": "イベント件数",
        "case_count": "ケース数",
        "total_duration_min": "合計時間(分)",
        "avg_duration_min": "平均時間(分)",
        "median_duration_min": "中央値時間(分)",
        "std_duration_min": "標準偏差(分)",
        "min_duration_min": "最小時間(分)",
        "max_duration_min": "最大時間(分)",
        "p75_duration_min": "75%点(分)",
        "p90_duration_min": "90%点(分)",
        "p95_duration_min": "95%点(分)",
        "event_ratio_pct": "イベント比率(%)",
    },
}



def create_frequency_analysis(df):
    # アクティビティごとの件数と処理時間を集計します。
    result = (
        df.groupby("activity")
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
    result["event_ratio_pct"] = (result["event_count"] / total_events * 100).round(2)

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

    return result.sort_values(["event_count", "activity"], ascending=[False, True]).reset_index(drop=True)
