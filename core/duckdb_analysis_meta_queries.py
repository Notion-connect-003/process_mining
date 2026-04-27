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

def query_filtered_meta(parquet_path, filter_params=None, filter_column_settings=None, variant_pattern=None):
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
            COUNT(DISTINCT case_id) AS case_count,
            COUNT(*) AS event_count
        FROM {relation_name}
        """
        row = connection.execute(f"{cte_sql}\n{query_sql}", params).fetchone()
        return {
            "case_count": int(row[0] or 0),
            "event_count": int(row[1] or 0),
        }
    finally:
        connection.close()


def query_period_text(parquet_path, filter_params=None, filter_column_settings=None, variant_pattern=None):
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
            MIN(COALESCE(start_time, timestamp)) AS period_start,
            MAX(COALESCE(next_time, start_time, timestamp)) AS period_end
        FROM {relation_name}
        """
        row = connection.execute(f"{cte_sql}\n{query_sql}", params).fetchone()
        if not row or row[0] is None or row[1] is None:
            return "不明"

        period_start = pd.Timestamp(row[0])
        period_end = pd.Timestamp(row[1])
        return f"{period_start.strftime('%Y-%m-%d %H:%M')} 〜 {period_end.strftime('%Y-%m-%d %H:%M')}"
    finally:
        connection.close()


def query_group_summary(parquet_path, group_columns, filter_params=None, filter_column_settings=None, variant_pattern=None):
    valid_columns = [
        str(column_name)
        for column_name in (group_columns or [])
        if str(column_name) in _get_parquet_column_names(parquet_path)
    ]
    if not valid_columns:
        return {}

    connection = duckdb.connect()
    try:
        cte_sql, params, relation_name = _build_scoped_relation_cte(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
        )
        total_meta_row = connection.execute(
            f"""
            {cte_sql}
            SELECT
                COUNT(DISTINCT case_id) AS total_case_count,
                COUNT(*) AS total_event_count
            FROM {relation_name}
            """,
            params,
        ).fetchone()
        total_case_count = int((total_meta_row or [0, 0])[0] or 0)
        total_event_count = int((total_meta_row or [0, 0])[1] or 0)

        case_meta_df = connection.execute(
            f"""
            {cte_sql}
            SELECT
                ROUND(AVG(case_duration_min), 2) AS avg_duration_min,
                ROUND(MEDIAN(case_duration_min), 2) AS median_duration_min,
                ROUND(MAX(case_duration_min), 2) AS max_duration_min,
                ROUND(SUM(case_duration_min), 2) AS total_duration_min
            FROM (
                SELECT
                    case_id,
                    SUM(duration_sec) / 60.0 AS case_duration_min
                FROM {relation_name}
                GROUP BY case_id
            ) AS case_durations
            """,
            params,
        ).df()
        meta_row = case_meta_df.iloc[0] if not case_meta_df.empty else None

        summary = {}
        for column_name in valid_columns:
            quoted_column = _quote_identifier(column_name)
            event_counts_df = connection.execute(
                f"""
                {cte_sql}
                SELECT
                    TRIM(CAST({quoted_column} AS VARCHAR)) AS value,
                    COUNT(*) AS event_count
                FROM {relation_name}
                WHERE {quoted_column} IS NOT NULL
                  AND TRIM(CAST({quoted_column} AS VARCHAR)) <> ''
                GROUP BY 1
                """,
                params,
            ).df()
            case_stats_df = connection.execute(
                f"""
                {cte_sql}
                , case_values AS (
                    SELECT
                        case_id,
                        TRIM(CAST({quoted_column} AS VARCHAR)) AS value,
                        ROW_NUMBER() OVER (
                            PARTITION BY case_id
                            ORDER BY sequence_no ASC, COALESCE(start_time, timestamp) ASC
                        ) AS row_number
                    FROM {relation_name}
                    WHERE {quoted_column} IS NOT NULL
                      AND TRIM(CAST({quoted_column} AS VARCHAR)) <> ''
                ),
                case_first_values AS (
                    SELECT
                        case_id,
                        value
                    FROM case_values
                    WHERE row_number = 1
                ),
                case_durations AS (
                    SELECT
                        case_id,
                        SUM(duration_sec) / 60.0 AS case_duration_min
                    FROM {relation_name}
                    GROUP BY case_id
                )
                SELECT
                    case_first_values.value AS value,
                    COUNT(DISTINCT case_first_values.case_id) AS case_count,
                    ROUND(AVG(case_durations.case_duration_min), 2) AS avg_duration_min,
                    ROUND(MEDIAN(case_durations.case_duration_min), 2) AS median_duration_min,
                    ROUND(MAX(case_durations.case_duration_min), 2) AS max_duration_min,
                    ROUND(SUM(case_durations.case_duration_min), 2) AS total_duration_min
                FROM case_first_values
                INNER JOIN case_durations USING (case_id)
                GROUP BY case_first_values.value
                """,
                params,
            ).df()

            grouped = event_counts_df.merge(case_stats_df, on="value", how="outer")
            if grouped.empty:
                summary[column_name] = {}
                continue

            grouped["event_count"] = grouped["event_count"].fillna(0).astype(int)
            grouped["case_count"] = grouped["case_count"].fillna(0).astype(int)
            grouped["case_ratio_pct"] = (
                grouped["case_count"] / total_case_count * 100
            ).round(2) if total_case_count else 0.0
            grouped["event_ratio_pct"] = (
                grouped["event_count"] / total_event_count * 100
            ).round(2) if total_event_count else 0.0

            def _optional_float(value):
                if pd.isna(value):
                    return None
                return round(float(value), 2)

            column_summary = {}
            for _, row in grouped.iterrows():
                column_summary[str(row["value"])] = {
                    "case_count": int(row["case_count"]),
                    "case_ratio_pct": float(row["case_ratio_pct"]),
                    "event_count": int(row["event_count"]),
                    "event_ratio_pct": float(row["event_ratio_pct"]),
                    "avg_duration_min": _optional_float(row.get("avg_duration_min")),
                    "median_duration_min": _optional_float(row.get("median_duration_min")),
                    "max_duration_min": _optional_float(row.get("max_duration_min")),
                    "total_duration_min": _optional_float(row.get("total_duration_min")),
                }
            summary[column_name] = column_summary

        summary["__meta__"] = {
            "total_case_count": total_case_count,
            "total_event_count": total_event_count,
            "avg_duration_min": 0.0 if meta_row is None or pd.isna(meta_row["avg_duration_min"]) else float(meta_row["avg_duration_min"]),
            "median_duration_min": 0.0 if meta_row is None or pd.isna(meta_row["median_duration_min"]) else float(meta_row["median_duration_min"]),
            "max_duration_min": 0.0 if meta_row is None or pd.isna(meta_row["max_duration_min"]) else float(meta_row["max_duration_min"]),
            "total_duration_min": 0.0 if meta_row is None or pd.isna(meta_row["total_duration_min"]) else float(meta_row["total_duration_min"]),
        }
        return summary
    finally:
        connection.close()


def query_filter_options(parquet_path, filter_column_settings=None):
    normalized_settings = normalize_filter_column_settings(**(filter_column_settings or {}))
    filters = []
    all_activity_names = []
    connection = duckdb.connect()
    try:
        if "activity" in _get_parquet_column_names(parquet_path):
            try:
                activity_rows = connection.execute(
                    """
                    SELECT DISTINCT TRIM(CAST(activity AS VARCHAR)) AS val
                    FROM read_parquet(?)
                    WHERE activity IS NOT NULL
                      AND TRIM(CAST(activity AS VARCHAR)) <> ''
                    ORDER BY val
                    """,
                    [str(parquet_path)],
                ).fetchall()
                all_activity_names = [str(row[0]) for row in activity_rows]
            except Exception:
                all_activity_names = []

        for filter_key in FILTER_SLOT_KEYS:
            column_name = normalized_settings[filter_key]["column_name"]
            label = normalized_settings[filter_key]["label"]
            options = []
            if column_name and str(column_name) in _get_parquet_column_names(parquet_path):
                quoted_column = _quote_identifier(column_name)
                try:
                    rows = connection.execute(
                        f"""
                        SELECT DISTINCT TRIM(CAST({quoted_column} AS VARCHAR)) AS val
                        FROM read_parquet(?)
                        WHERE {quoted_column} IS NOT NULL
                          AND TRIM(CAST({quoted_column} AS VARCHAR)) <> ''
                        ORDER BY val
                        """,
                        [str(parquet_path)],
                    ).fetchall()
                    options = [str(row[0]) for row in rows]
                except Exception:
                    options = []
            filters.append(
                {
                    "slot": filter_key,
                    "label": label,
                    "column_name": column_name,
                    "options": options,
                }
            )
        return {
            "filters": filters,
            "all_activity_names": all_activity_names,
        }
    finally:
        connection.close()
