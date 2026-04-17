ANALYSIS_CONFIG = {
    "analysis_name": "前後処理分析",
    "sheet_name": "前後処理分析",
    "output_file_name": "前後処理分析.xlsx",
    "display_columns": {
        "from_activity": "前処理アクティビティ名",
        "to_activity": "後処理アクティビティ名",
        "transition_count": "遷移件数",
        "case_count": "ケース数",
        "case_ratio_pct": "ケース比率(%)",
        "total_duration_min": "合計所要時間(分)",
        "avg_duration_min": "平均所要時間(分)",
        "median_duration_min": "中央値所要時間(分)",
        "std_duration_min": "標準偏差(分)",
        "min_duration_min": "最小所要時間(分)",
        "max_duration_min": "最大所要時間(分)",
        "p75_duration_min": "75%点(分)",
        "p90_duration_min": "90%点(分)",
        "p95_duration_min": "95%点(分)",
        "from_avg_duration_min": "前処理平均時間(分)",
        "to_avg_duration_min": "後処理平均時間(分)",
        "transition_ratio_pct": "遷移比率(%)",
    },
}


def create_transition_analysis(df, group_columns=None):
    work = df.copy()

    # ケース内の次イベントを参照して前後遷移を集計します。
    work["next_activity"] = work.groupby("case_id")["activity"].shift(-1)
    work["next_start_time"] = work.groupby("case_id")["start_time"].shift(-1)
    work["next_duration_min"] = work.groupby("case_id")["duration_min"].shift(-1)
    work = work.dropna(subset=["next_activity"]).copy()
    work["waiting_time_min"] = (
        (work["next_start_time"] - work["next_time"]).dt.total_seconds() / 60
    )

    valid_group_cols = [col for col in (group_columns or []) if col in work.columns]
    if valid_group_cols:
        groupby_keys = valid_group_cols + ["activity", "next_activity"]
    else:
        groupby_keys = ["activity", "next_activity"]

    result = (
        work.groupby(groupby_keys)
        .agg(
            transition_count=("case_id", "count"),
            case_count=("case_id", "nunique"),
            from_total_duration_min=("duration_min", "sum"),
            from_avg_duration_min=("duration_min", "mean"),
            to_total_duration_min=("next_duration_min", "sum"),
            to_avg_duration_min=("next_duration_min", "mean"),
            total_waiting_time_min=("waiting_time_min", "sum"),
            avg_waiting_time_min=("waiting_time_min", "mean"),
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
        .rename(columns={"activity": "from_activity", "next_activity": "to_activity"})
    )

    total_transitions = result["transition_count"].sum()
    result["transition_ratio_pct"] = (
        result["transition_count"] / total_transitions * 100
    ).round(2)

    total_cases = work["case_id"].nunique()
    result["case_ratio_pct"] = (
        (result["case_count"] / total_cases * 100).round(2)
        if total_cases
        else 0.0
    )

    numeric_cols = [
        "from_total_duration_min",
        "from_avg_duration_min",
        "to_total_duration_min",
        "to_avg_duration_min",
        "total_waiting_time_min",
        "avg_waiting_time_min",
        "total_duration_min",
        "avg_duration_min",
        "median_duration_min",
        "min_duration_min",
        "max_duration_min",
        "p75_duration_min",
        "p90_duration_min",
        "p95_duration_min",
        "case_ratio_pct",
    ]
    result[numeric_cols] = result[numeric_cols].round(2)

    # 標準偏差は単一遷移だと NaN になるため表示用に "-" へ置換します。
    result["std_duration_min"] = (
        result["std_duration_min"].round(2).where(result["std_duration_min"].notna(), other="-")
    )

    sort_keys = valid_group_cols + ["transition_count", "from_activity", "to_activity"]
    sort_ascending = [True] * len(valid_group_cols) + [False, True, True]
    result = result.sort_values(sort_keys, ascending=sort_ascending).reset_index(drop=True)

    ordered_columns = valid_group_cols + [
        "from_activity",
        "to_activity",
        "transition_count",
        "case_count",
        "case_ratio_pct",
        "total_duration_min",
        "avg_duration_min",
        "median_duration_min",
        "std_duration_min",
        "min_duration_min",
        "max_duration_min",
        "p75_duration_min",
        "p90_duration_min",
        "p95_duration_min",
        "from_total_duration_min",
        "from_avg_duration_min",
        "to_total_duration_min",
        "to_avg_duration_min",
        "total_waiting_time_min",
        "avg_waiting_time_min",
        "transition_ratio_pct",
    ]

    return result[[col for col in ordered_columns if col in result.columns]]
