import duckdb
import pandas as pd

from core.duckdb_core import *
from core.duckdb_core import (
    _build_scoped_relation_cte,
    _query_dataframe,
    _quote_identifier,
)
from core.duckdb_analysis_queries import query_filtered_meta, query_variant_summary

def query_pattern_bottleneck_details(
    parquet_path,
    pattern,
    filter_params=None,
    filter_column_settings=None,
    scope_variant_pattern=None,
):
    normalized_pattern = str(pattern or "").strip()
    if not normalized_pattern:
        raise ValueError("パターンが必要です。")

    total_meta = query_filtered_meta(
        parquet_path,
        filter_params=filter_params,
        filter_column_settings=filter_column_settings,
        variant_pattern=scope_variant_pattern,
    )
    pattern_meta = query_filtered_meta(
        parquet_path,
        filter_params=filter_params,
        filter_column_settings=filter_column_settings,
        variant_pattern=normalized_pattern,
    )
    if pattern_meta["event_count"] <= 0:
        raise ValueError("パターンが見つかりません。")

    connection = duckdb.connect()
    try:
        cte_sql, params, relation_name = _build_scoped_relation_cte(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=normalized_pattern,
        )

        step_query_sql = f"""
        , interval_rows AS (
            SELECT
                sequence_no,
                activity,
                LEAD(activity) OVER (PARTITION BY case_id ORDER BY sequence_no) AS next_activity,
                case_id,
                duration_min
            FROM {relation_name}
        )
        SELECT
            sequence_no,
            activity,
            next_activity,
            COUNT(*) AS case_count,
            ROUND(AVG(duration_min), 2) AS avg_duration_min,
            ROUND(MEDIAN(duration_min), 2) AS median_duration_min,
            ROUND(MIN(duration_min), 2) AS min_duration_min,
            ROUND(MAX(duration_min), 2) AS max_duration_min,
            ROUND(SUM(duration_min), 2) AS total_duration_min
        FROM interval_rows
        WHERE next_activity IS NOT NULL
        GROUP BY sequence_no, activity, next_activity
        ORDER BY sequence_no ASC, activity ASC, next_activity ASC
        """
        step_metrics_df = _query_dataframe(connection, cte_sql, params, step_query_sql)

        if step_metrics_df.empty:
            step_metrics = []
            bottleneck_transition = None
        else:
            total_wait_min = float(step_metrics_df["total_duration_min"].sum())
            if total_wait_min > 0:
                step_metrics_df["wait_share_pct"] = (
                    step_metrics_df["total_duration_min"] / total_wait_min * 100
                ).round(2)
            else:
                step_metrics_df["wait_share_pct"] = 0.0

            step_metrics_df["transition_label"] = (
                step_metrics_df["activity"] + " → " + step_metrics_df["next_activity"]
            )
            step_metrics = [
                {
                    "sequence_no": int(row["sequence_no"]),
                    "activity": row["activity"],
                    "next_activity": row["next_activity"],
                    "case_count": int(row["case_count"]),
                    "avg_duration_min": float(row["avg_duration_min"]),
                    "median_duration_min": float(row["median_duration_min"]),
                    "min_duration_min": float(row["min_duration_min"]),
                    "max_duration_min": float(row["max_duration_min"]),
                    "total_duration_min": float(row["total_duration_min"]),
                    "wait_share_pct": float(row["wait_share_pct"]),
                    "transition_label": row["transition_label"],
                    "transition_key": build_transition_key(row["activity"], row["next_activity"]),
                }
                for row in step_metrics_df.to_dict(orient="records")
            ]

            bottleneck_row = step_metrics_df.sort_values(
                ["avg_duration_min", "median_duration_min", "max_duration_min", "sequence_no"],
                ascending=[False, False, False, True],
            ).iloc[0]
            bottleneck_transition = {
                "sequence_no": int(bottleneck_row["sequence_no"]),
                "from_activity": bottleneck_row["activity"],
                "to_activity": bottleneck_row["next_activity"],
                "transition_label": bottleneck_row["transition_label"],
                "transition_key": build_transition_key(
                    bottleneck_row["activity"],
                    bottleneck_row["next_activity"],
                ),
                "avg_duration_min": float(bottleneck_row["avg_duration_min"]),
                "median_duration_min": float(bottleneck_row["median_duration_min"]),
                "max_duration_min": float(bottleneck_row["max_duration_min"]),
                "wait_share_pct": float(bottleneck_row["wait_share_pct"]),
            }

        case_query_sql = f"""
        SELECT
            case_id,
            MIN(start_time) AS start_time,
            MAX(next_time) AS end_time,
            ROUND(SUM(duration_min), 2) AS case_total_duration_min
        FROM {relation_name}
        GROUP BY case_id
        ORDER BY case_total_duration_min DESC, case_id ASC
        """
        case_summary_df = _query_dataframe(connection, cte_sql, params, case_query_sql)
        if case_summary_df.empty:
            raise ValueError("パターンが見つかりません。")

        avg_case_duration_min = round(float(case_summary_df["case_total_duration_min"].mean()), 2)
        median_case_duration_min = round(float(case_summary_df["case_total_duration_min"].median()), 2)
        min_case_duration_min = round(float(case_summary_df["case_total_duration_min"].min()), 2)
        max_case_duration_min = round(float(case_summary_df["case_total_duration_min"].max()), 2)

        return {
            "pattern": normalized_pattern,
            "pattern_steps": normalized_pattern.split(FLOW_PATH_SEPARATOR),
            "case_count": int(pattern_meta["case_count"]),
            "case_ratio_pct": round(
                (int(pattern_meta["case_count"]) / int(total_meta["case_count"]) * 100)
                if total_meta["case_count"]
                else 0.0,
                2,
            ),
            "avg_case_duration_min": avg_case_duration_min,
            "median_case_duration_min": median_case_duration_min,
            "min_case_duration_min": min_case_duration_min,
            "max_case_duration_min": max_case_duration_min,
            "bottleneck_transition": bottleneck_transition,
            "step_metrics": step_metrics,
            "case_examples": [
                {
                    "case_id": row["case_id"],
                    "start_time": pd.Timestamp(row["start_time"]).isoformat(),
                    "end_time": pd.Timestamp(row["end_time"]).isoformat(),
                    "case_total_duration_min": float(row["case_total_duration_min"]),
                }
                for row in case_summary_df.head(20).to_dict(orient="records")
            ],
        }
    finally:
        connection.close()


def query_transition_case_drilldown(parquet_path, from_activity, to_activity, limit=20, filter_params=None, filter_column_settings=None, variant_pattern=None):
    connection = duckdb.connect()
    try:
        cte_sql, params, relation_name = _build_scoped_relation_cte(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
        )
        query_sql = f"""
        , interval_rows AS (
            SELECT
                case_id,
                activity,
                LEAD(activity) OVER (PARTITION BY case_id ORDER BY sequence_no) AS next_activity,
                start_time,
                next_time,
                duration_sec
            FROM {relation_name}
        )
        SELECT
            case_id,
            ROUND(duration_sec, 2) AS duration_sec,
            start_time,
            next_time
        FROM interval_rows
        WHERE activity = ? AND next_activity = ?
        ORDER BY duration_sec DESC, case_id ASC, start_time ASC
        LIMIT ?
        """
        drilldown_df = _query_dataframe(connection, cte_sql, params + [from_activity, to_activity, int(limit)], query_sql)
        return [
            {
                "case_id": row["case_id"],
                "duration_sec": float(row["duration_sec"]),
                "duration_text": format_duration_text(row["duration_sec"]),
                "from_time": pd.Timestamp(row["start_time"]).isoformat(),
                "to_time": pd.Timestamp(row["next_time"]).isoformat(),
            }
            for row in drilldown_df.to_dict(orient="records")
        ]
    finally:
        connection.close()


def query_activity_case_drilldown(parquet_path, activity, limit=20, filter_params=None, filter_column_settings=None, variant_pattern=None):
    connection = duckdb.connect()
    try:
        cte_sql, params, relation_name = _build_scoped_relation_cte(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
        )
        query_sql = f"""
        , interval_rows AS (
            SELECT
                case_id,
                activity,
                LEAD(activity) OVER (PARTITION BY case_id ORDER BY sequence_no) AS next_activity,
                start_time,
                next_time,
                duration_sec
            FROM {relation_name}
        )
        SELECT
            case_id,
            activity,
            next_activity,
            ROUND(duration_sec, 2) AS duration_sec,
            start_time,
            next_time
        FROM interval_rows
        WHERE activity = ? AND next_activity IS NOT NULL
        ORDER BY duration_sec DESC, case_id ASC, start_time ASC
        LIMIT ?
        """
        drilldown_df = _query_dataframe(connection, cte_sql, params + [activity, int(limit)], query_sql)
        return [
            {
                "case_id": row["case_id"],
                "activity": row["activity"],
                "next_activity": row["next_activity"],
                "duration_sec": float(row["duration_sec"]),
                "duration_text": format_duration_text(row["duration_sec"]),
                "from_time": pd.Timestamp(row["start_time"]).isoformat(),
                "to_time": pd.Timestamp(row["next_time"]).isoformat(),
            }
            for row in drilldown_df.to_dict(orient="records")
        ]
    finally:
        connection.close()


