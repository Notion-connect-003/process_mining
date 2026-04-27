import duckdb
import pandas as pd

from core.duckdb_core import *
from core.duckdb_core import (
    _build_scoped_relation_cte,
    _format_stddev_column,
    _get_parquet_column_names,
    _query_dataframe,
    _quote_identifier,
)

def query_frequency_analysis_df(parquet_path, filter_params=None, filter_column_settings=None, variant_pattern=None):
    connection = duckdb.connect()
    try:
        cte_sql, params, relation_name = _build_scoped_relation_cte(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
        )
        query_sql = f"""
        SELECT
            activity,
            COUNT(*) AS event_count,
            COUNT(DISTINCT case_id) AS case_count,
            ROUND(
                COUNT(DISTINCT case_id) * 100.0 / NULLIF((SELECT COUNT(DISTINCT case_id) FROM {relation_name}), 0),
                2
            ) AS case_ratio_pct,
            ROUND(SUM(duration_min), 2) AS total_duration_min,
            ROUND(AVG(duration_min), 2) AS avg_duration_min,
            ROUND(MEDIAN(duration_min), 2) AS median_duration_min,
            CASE WHEN COUNT(*) > 1 THEN ROUND(STDDEV_SAMP(duration_min), 2) ELSE NULL END AS std_duration_min,
            ROUND(MIN(duration_min), 2) AS min_duration_min,
            ROUND(MAX(duration_min), 2) AS max_duration_min,
            ROUND(QUANTILE_CONT(duration_min, 0.75), 2) AS p75_duration_min,
            ROUND(QUANTILE_CONT(duration_min, 0.90), 2) AS p90_duration_min,
            ROUND(QUANTILE_CONT(duration_min, 0.95), 2) AS p95_duration_min,
            ROUND(
                COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM {relation_name}), 0),
                2
            ) AS event_ratio_pct
        FROM {relation_name}
        GROUP BY activity
        ORDER BY event_count DESC, activity ASC
        """
        result_df = _query_dataframe(connection, cte_sql, params, query_sql)
        if "std_duration_min" in result_df.columns:
            result_df["std_duration_min"] = _format_stddev_column(result_df["std_duration_min"])
        return result_df
    finally:
        connection.close()


def query_transition_analysis_df(parquet_path, filter_params=None, filter_column_settings=None, variant_pattern=None):
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
                activity AS from_activity,
                LEAD(activity) OVER (PARTITION BY case_id ORDER BY sequence_no) AS to_activity,
                case_id,
                duration_min,
                next_time,
                LEAD(start_time) OVER (PARTITION BY case_id ORDER BY sequence_no) AS next_start_time,
                LEAD(duration_min) OVER (PARTITION BY case_id ORDER BY sequence_no) AS next_duration_min
            FROM {relation_name}
        )
        SELECT
            from_activity,
            to_activity,
            COUNT(*) AS transition_count,
            COUNT(DISTINCT case_id) AS case_count,
            ROUND(
                COUNT(DISTINCT case_id) * 100.0 / NULLIF(
                    (SELECT COUNT(DISTINCT case_id) FROM interval_rows WHERE to_activity IS NOT NULL),
                    0
                ),
                2
            ) AS case_ratio_pct,
            ROUND(SUM(duration_min), 2) AS total_duration_min,
            ROUND(AVG(duration_min), 2) AS avg_duration_min,
            ROUND(MEDIAN(duration_min), 2) AS median_duration_min,
            CASE WHEN COUNT(*) > 1 THEN ROUND(STDDEV_SAMP(duration_min), 2) ELSE NULL END AS std_duration_min,
            ROUND(MIN(duration_min), 2) AS min_duration_min,
            ROUND(MAX(duration_min), 2) AS max_duration_min,
            ROUND(QUANTILE_CONT(duration_min, 0.75), 2) AS p75_duration_min,
            ROUND(QUANTILE_CONT(duration_min, 0.90), 2) AS p90_duration_min,
            ROUND(QUANTILE_CONT(duration_min, 0.95), 2) AS p95_duration_min,
            ROUND(SUM(duration_min), 2) AS from_total_duration_min,
            ROUND(AVG(duration_min), 2) AS from_avg_duration_min,
            ROUND(SUM(next_duration_min), 2) AS to_total_duration_min,
            ROUND(AVG(next_duration_min), 2) AS to_avg_duration_min,
            ROUND(SUM(date_diff('second', next_time, next_start_time) / 60.0), 2) AS total_waiting_time_min,
            ROUND(AVG(date_diff('second', next_time, next_start_time) / 60.0), 2) AS avg_waiting_time_min,
            ROUND(
                COUNT(*) * 100.0 / NULLIF(
                    (SELECT COUNT(*) FROM interval_rows WHERE to_activity IS NOT NULL),
                    0
                ),
                2
            ) AS transition_ratio_pct
        FROM interval_rows
        WHERE to_activity IS NOT NULL
        GROUP BY from_activity, to_activity
        ORDER BY transition_count DESC, from_activity ASC, to_activity ASC
        """
        result_df = _query_dataframe(connection, cte_sql, params, query_sql)
        if "std_duration_min" in result_df.columns:
            result_df["std_duration_min"] = _format_stddev_column(result_df["std_duration_min"])
        ordered_columns = [
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
        return result_df[ordered_columns] if not result_df.empty else result_df
    finally:
        connection.close()


def query_pattern_analysis_df(parquet_path, filter_params=None, filter_column_settings=None, variant_pattern=None):
    connection = duckdb.connect()
    try:
        cte_sql, params, relation_name = _build_scoped_relation_cte(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
        )
        query_sql = f"""
        , case_path AS (
            SELECT
                case_id,
                string_agg(CAST(activity AS VARCHAR), '{FLOW_PATH_SEPARATOR}' ORDER BY sequence_no) AS pattern
            FROM {relation_name}
            GROUP BY case_id
        ),
        case_duration AS (
            SELECT
                case_id,
                ROUND(date_diff('second', MIN(start_time), MAX(next_time)) / 60.0, 2) AS case_total_duration_min
            FROM {relation_name}
            GROUP BY case_id
        ),
        merged AS (
            SELECT
                case_path.case_id,
                case_path.pattern,
                case_duration.case_total_duration_min
            FROM case_path
            INNER JOIN case_duration USING (case_id)
        )
        SELECT
            COUNT(*) AS case_count,
            ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(DISTINCT case_id) FROM merged), 0), 2) AS case_ratio_pct,
            ROUND(AVG(case_total_duration_min), 2) AS avg_case_duration_min,
            ROUND(MEDIAN(case_total_duration_min), 2) AS median_case_duration_min,
            CASE WHEN COUNT(*) > 1 THEN ROUND(STDDEV_SAMP(case_total_duration_min), 2) ELSE NULL END AS std_case_duration_min,
            ROUND(MIN(case_total_duration_min), 2) AS min_case_duration_min,
            ROUND(MAX(case_total_duration_min), 2) AS max_case_duration_min,
            ROUND(QUANTILE_CONT(case_total_duration_min, 0.75), 2) AS p75_case_duration_min,
            ROUND(QUANTILE_CONT(case_total_duration_min, 0.90), 2) AS p90_case_duration_min,
            ROUND(QUANTILE_CONT(case_total_duration_min, 0.95), 2) AS p95_case_duration_min,
            pattern
        FROM merged
        GROUP BY pattern
        ORDER BY case_count DESC, pattern ASC
        """
        result_df = _query_dataframe(connection, cte_sql, params, query_sql)
        if "std_case_duration_min" in result_df.columns:
            result_df["std_case_duration_min"] = _format_stddev_column(result_df["std_case_duration_min"])
        return enrich_pattern_analysis_result(result_df)
    finally:
        connection.close()


def query_analysis_records(parquet_path, analysis_key, filter_params=None, filter_column_settings=None, variant_pattern=None):
    if analysis_key == "frequency":
        result_df = query_frequency_analysis_df(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
        )
    elif analysis_key == "transition":
        result_df = query_transition_analysis_df(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
        )
    elif analysis_key == "pattern":
        result_df = query_pattern_analysis_df(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
        )
    else:
        raise ValueError(f"未対応の分析種別です: {analysis_key}")

    analysis_config = ANALYSIS_CONFIGS[analysis_key]
    return {
        "analysis_name": analysis_config["analysis_name"],
        "sheet_name": analysis_config["sheet_name"],
        "output_file_name": analysis_config["output_file_name"],
        "rows": convert_analysis_result_to_records(
            result_df,
            analysis_config["display_columns"],
        ),
        "excel_file": None,
    }


def query_transition_records_for_patterns(
    parquet_path,
    patterns,
    filter_params=None,
    filter_column_settings=None,
):
    normalized_patterns = []
    for pattern_value in patterns or []:
        normalized_pattern = str(pattern_value or "").strip()
        if normalized_pattern and normalized_pattern not in normalized_patterns:
            normalized_patterns.append(normalized_pattern)

    if not normalized_patterns:
        return []

    connection = duckdb.connect()
    try:
        cte_sql, params, relation_name = _build_scoped_relation_cte(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
        )
        placeholders = ", ".join("?" for _ in normalized_patterns)
        query_sql = f"""
        , case_patterns AS (
            SELECT
                case_id,
                string_agg(CAST(activity AS VARCHAR), '{FLOW_PATH_SEPARATOR}' ORDER BY sequence_no) AS pattern
            FROM {relation_name}
            GROUP BY case_id
        ),
        pattern_scoped AS (
            SELECT scoped.*
            FROM {relation_name} AS scoped
            INNER JOIN case_patterns USING (case_id)
            WHERE case_patterns.pattern IN ({placeholders})
        ),
        interval_rows AS (
            SELECT
                activity AS from_activity,
                LEAD(activity) OVER (PARTITION BY case_id ORDER BY sequence_no) AS to_activity,
                case_id,
                duration_min,
                next_time,
                LEAD(start_time) OVER (PARTITION BY case_id ORDER BY sequence_no) AS next_start_time,
                LEAD(duration_min) OVER (PARTITION BY case_id ORDER BY sequence_no) AS next_duration_min
            FROM pattern_scoped
        )
        SELECT
            from_activity,
            to_activity,
            COUNT(*) AS transition_count,
            COUNT(DISTINCT case_id) AS case_count,
            ROUND(
                COUNT(DISTINCT case_id) * 100.0 / NULLIF(
                    (SELECT COUNT(DISTINCT case_id) FROM interval_rows WHERE to_activity IS NOT NULL),
                    0
                ),
                2
            ) AS case_ratio_pct,
            ROUND(SUM(duration_min), 2) AS total_duration_min,
            ROUND(AVG(duration_min), 2) AS avg_duration_min,
            ROUND(MEDIAN(duration_min), 2) AS median_duration_min,
            CASE WHEN COUNT(*) > 1 THEN ROUND(STDDEV_SAMP(duration_min), 2) ELSE NULL END AS std_duration_min,
            ROUND(MIN(duration_min), 2) AS min_duration_min,
            ROUND(MAX(duration_min), 2) AS max_duration_min,
            ROUND(QUANTILE_CONT(duration_min, 0.75), 2) AS p75_duration_min,
            ROUND(QUANTILE_CONT(duration_min, 0.90), 2) AS p90_duration_min,
            ROUND(QUANTILE_CONT(duration_min, 0.95), 2) AS p95_duration_min,
            ROUND(SUM(duration_min), 2) AS from_total_duration_min,
            ROUND(AVG(duration_min), 2) AS from_avg_duration_min,
            ROUND(SUM(next_duration_min), 2) AS to_total_duration_min,
            ROUND(AVG(next_duration_min), 2) AS to_avg_duration_min,
            ROUND(SUM(date_diff('second', next_time, next_start_time) / 60.0), 2) AS total_waiting_time_min,
            ROUND(AVG(date_diff('second', next_time, next_start_time) / 60.0), 2) AS avg_waiting_time_min,
            ROUND(
                COUNT(*) * 100.0 / NULLIF(
                    (SELECT COUNT(*) FROM interval_rows WHERE to_activity IS NOT NULL),
                    0
                ),
                2
            ) AS transition_ratio_pct
        FROM interval_rows
        WHERE to_activity IS NOT NULL
        GROUP BY from_activity, to_activity
        ORDER BY transition_count DESC, from_activity ASC, to_activity ASC
        """
        result_df = _query_dataframe(
            connection,
            cte_sql,
            params + normalized_patterns,
            query_sql,
        )
        if "std_duration_min" in result_df.columns:
            result_df["std_duration_min"] = _format_stddev_column(result_df["std_duration_min"])
        return convert_analysis_result_to_records(
            result_df,
            TRANSITION_ANALYSIS_CONFIG["display_columns"],
        )
    finally:
        connection.close()


