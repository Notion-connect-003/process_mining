ANALYSIS_CONFIG = {
    "analysis_name": "処理順パターン分析",
    "sheet_name": "処理順パターン分析",
    "output_file_name": "処理順パターン分析.xlsx",
    "display_columns": {
        "case_count": "ケース数",
        "case_ratio_pct": "ケース比率(%)",
        "avg_case_duration_min": "平均ケース時間(分)",
        "median_case_duration_min": "中央値ケース時間(分)",
        "std_case_duration_min": "標準偏差ケース時間(分)",
        "min_case_duration_min": "最小ケース時間(分)",
        "max_case_duration_min": "最大ケース時間(分)",
        "p75_case_duration_min": "75%点ケース時間(分)",
        "p90_case_duration_min": "90%点ケース時間(分)",
        "p95_case_duration_min": "95%点ケース時間(分)",
        "pattern": "処理順パターン",
    },
}



def create_pattern_analysis(df):
    # ケースごとの処理順を1本のパターン文字列にまとめます。
    case_path = (
        df.sort_values(["case_id", "sequence_no"])
        .groupby("case_id")["activity"]
        .apply(lambda series: "→".join(series.tolist()))
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

    result = (
        merged.groupby("pattern")
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

    # 標準偏差はケースが1件のみのパターンで NaN になるため "-" で表示する。
    result["std_case_duration_min"] = (
        result["std_case_duration_min"]
        .round(2)
        .where(result["std_case_duration_min"].notna(), other="-")
    )

    result = result.sort_values(["case_count", "pattern"], ascending=[False, True]).reset_index(drop=True)

    ordered_columns = [
        "case_count",
        "case_ratio_pct",
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

    return result[ordered_columns]
