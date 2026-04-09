from pathlib import Path
from functools import lru_cache

import duckdb
import pandas as pd

from 共通スクリプト.Excel出力.excel_exporter import convert_analysis_result_to_records
from 共通スクリプト.analysis_service import (
    DEFAULT_FILTER_LABELS,
    FILTER_SLOT_KEYS,
    FLOW_PATH_SEPARATOR,
    _build_heatmap,
    _format_duration_text,
    build_transition_key,
    normalize_filter_column_settings,
    normalize_filter_params,
)
from 共通スクリプト.分析.前後処理分析.transition_analysis import ANALYSIS_CONFIG as TRANSITION_ANALYSIS_CONFIG
from 共通スクリプト.分析.処理順パターン分析.pattern_analysis import (
    ANALYSIS_CONFIG as PATTERN_ANALYSIS_CONFIG,
    enrich_pattern_analysis_result,
)
from 共通スクリプト.分析.頻度分析.frequency_analysis import ANALYSIS_CONFIG as FREQUENCY_ANALYSIS_CONFIG


ANALYSIS_CONFIGS = {
    "frequency": FREQUENCY_ANALYSIS_CONFIG,
    "transition": TRANSITION_ANALYSIS_CONFIG,
    "pattern": PATTERN_ANALYSIS_CONFIG,
}
FLOW_PATH_SEPARATOR = "\u2192"


@lru_cache(maxsize=64)
def _get_parquet_column_names(parquet_path):
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(parquet_path)],
        ).fetchall()
        return tuple(str(row[0]) for row in rows)
    finally:
        connection.close()


@lru_cache(maxsize=64)
def _variant_column_is_exact_pattern_key(parquet_path):
    parquet_columns = {
        str(column_name or "").strip().lower()
        for column_name in _get_parquet_column_names(parquet_path)
    }
    if "variant" not in parquet_columns:
        return False

    connection = duckdb.connect()
    try:
        result = connection.execute(
            f"""
            WITH case_patterns AS (
                SELECT
                    case_id,
                    TRIM(CAST(variant AS VARCHAR)) AS variant,
                    string_agg(CAST(activity AS VARCHAR), '{FLOW_PATH_SEPARATOR}' ORDER BY sequence_no) AS pattern
                FROM read_parquet(?)
                WHERE variant IS NOT NULL
                  AND TRIM(CAST(variant AS VARCHAR)) <> ''
                GROUP BY case_id, TRIM(CAST(variant AS VARCHAR))
            ),
            variant_pattern_counts AS (
                SELECT
                    variant,
                    COUNT(DISTINCT pattern) AS pattern_count
                FROM case_patterns
                GROUP BY variant
            )
            SELECT COALESCE(MAX(pattern_count), 0) <= 1
            FROM variant_pattern_counts
            """,
            [str(parquet_path)],
        ).fetchone()
        return bool(result[0]) if result else False
    finally:
        connection.close()


def _can_use_variant_pattern_fast_path(parquet_path, normalized_filters):
    if not parquet_path:
        return False

    # Event-level filters can trim a trace mid-case, so the variant column only stays exact
    # when we are scoping whole traces (no date/category filters applied).
    if normalized_filters.get("date_from") or normalized_filters.get("date_to"):
        return False

    if any(normalized_filters.get(filter_key) for filter_key in FILTER_SLOT_KEYS):
        return False

    parquet_columns = {
        str(column_name or "").strip().lower()
        for column_name in _get_parquet_column_names(parquet_path)
    }
    return "variant" in parquet_columns and _variant_column_is_exact_pattern_key(parquet_path)


def persist_prepared_parquet(prepared_df, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()

    try:
        connection.register("prepared_source", prepared_df)
        escaped_path = str(output_path).replace("'", "''")
        connection.execute(
            f"COPY prepared_source TO '{escaped_path}' (FORMAT PARQUET)"
        )
    finally:
        connection.close()

    return output_path


def _quote_identifier(identifier):
    return f'"{str(identifier or "").replace(chr(34), chr(34) * 2)}"'


def _parse_filter_datetime(value, is_end=False):
    if not value:
        return None

    parsed_value = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed_value):
        return None

    if is_end and len(str(value)) <= 10:
        return parsed_value.normalize() + pd.Timedelta(days=1)

    return parsed_value


def _normalize_activity_values(raw_value):
    if isinstance(raw_value, (list, tuple, set)):
        values = [str(value or "").strip() for value in raw_value]
    else:
        values = [part.strip() for part in str(raw_value or "").split(",")]
    return [value for value in values if value]


def _build_scoped_relation_cte(
    parquet_path,
    filter_params=None,
    filter_column_settings=None,
    variant_pattern=None,
):
    normalized_filters = normalize_filter_params(**(filter_params or {}))
    normalized_column_settings = normalize_filter_column_settings(**(filter_column_settings or {}))
    params = [str(parquet_path)]
    where_clauses = []

    from_boundary = _parse_filter_datetime(normalized_filters.get("date_from"))
    if from_boundary is not None:
        where_clauses.append("timestamp >= ?")
        params.append(from_boundary.to_pydatetime())

    to_boundary = _parse_filter_datetime(normalized_filters.get("date_to"), is_end=True)
    if to_boundary is not None:
        where_clauses.append("timestamp < ?")
        params.append(to_boundary.to_pydatetime())

    for filter_key in FILTER_SLOT_KEYS:
        filter_value = normalized_filters.get(filter_key)
        column_name = normalized_column_settings.get(filter_key, {}).get("column_name")
        if not filter_value or not column_name:
            continue
        where_clauses.append(f"TRIM(CAST({_quote_identifier(column_name)} AS VARCHAR)) = ?")
        params.append(str(filter_value).strip())

    ctes = ["source AS (SELECT * FROM read_parquet(?))"]
    base_query = "SELECT * FROM source"
    if where_clauses:
        base_query = f"{base_query} WHERE {' AND '.join(where_clauses)}"
    ctes.append(f"filtered_base AS ({base_query})")
    current_relation = "filtered_base"

    activity_mode = normalized_filters.get("activity_mode")
    activity_values = _normalize_activity_values(normalized_filters.get("activity_values"))
    if activity_mode in {"include", "exclude"} and activity_values:
        placeholders = ", ".join("?" for _ in activity_values)
        operator = "IN" if activity_mode == "include" else "NOT IN"
        ctes.append(
            f"""
            activity_scoped AS (
                SELECT *
                FROM {current_relation}
                WHERE case_id {operator} (
                    SELECT DISTINCT case_id
                    FROM {current_relation}
                    WHERE TRIM(CAST(activity AS VARCHAR)) IN ({placeholders})
                )
            )
            """.strip()
        )
        params.extend(activity_values)
        current_relation = "activity_scoped"

    if variant_pattern:
        normalized_pattern = str(variant_pattern).strip()
        if _can_use_variant_pattern_fast_path(parquet_path, normalized_filters):
            variant_identifier = _quote_identifier("variant")
            ctes.append(
                f"""
                variant_steps AS (
                    SELECT DISTINCT
                        TRIM(CAST({variant_identifier} AS VARCHAR)) AS variant,
                        sequence_no,
                        CAST(activity AS VARCHAR) AS activity
                    FROM {current_relation}
                    WHERE {variant_identifier} IS NOT NULL
                      AND TRIM(CAST({variant_identifier} AS VARCHAR)) <> ''
                )
                """.strip()
            )
            ctes.append(
                f"""
                variant_patterns AS (
                    SELECT
                        variant,
                        string_agg(activity, '{FLOW_PATH_SEPARATOR}' ORDER BY sequence_no) AS pattern
                    FROM variant_steps
                    GROUP BY variant
                )
                """.strip()
            )
            ctes.append(
                f"""
                pattern_scoped AS (
                    SELECT scoped.*
                    FROM {current_relation} AS scoped
                    INNER JOIN variant_patterns
                        ON TRIM(CAST(scoped.{variant_identifier} AS VARCHAR)) = variant_patterns.variant
                    WHERE variant_patterns.pattern = ?
                )
                """.strip()
            )
        else:
            ctes.append(
                f"""
                case_patterns AS (
                    SELECT
                        case_id,
                        string_agg(CAST(activity AS VARCHAR), '{FLOW_PATH_SEPARATOR}' ORDER BY sequence_no) AS pattern
                    FROM {current_relation}
                    GROUP BY case_id
                )
                """.strip()
            )
            ctes.append(
                f"""
                pattern_scoped AS (
                    SELECT scoped.*
                    FROM {current_relation} AS scoped
                    INNER JOIN case_patterns USING (case_id)
                    WHERE case_patterns.pattern = ?
                )
                """.strip()
            )
        params.append(normalized_pattern)
        current_relation = "pattern_scoped"

    return f"WITH {', '.join(ctes)}", params, current_relation


def _query_dataframe(connection, cte_sql, params, query_sql):
    return connection.execute(f"{cte_sql}\n{query_sql}", params).df()


def _format_stddev_column(series):
    return series.round(2).where(series.notna(), other="-")


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
    connection = duckdb.connect()
    try:
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
        return {"filters": filters}
    finally:
        connection.close()


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


def query_variant_summary(parquet_path, filter_params=None, filter_column_settings=None, variant_pattern=None, limit=None):
    connection = duckdb.connect()
    try:
        cte_sql, params, relation_name = _build_scoped_relation_cte(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
        )
        query_sql = f"""
        , case_patterns AS (
            SELECT
                case_id,
                string_agg(CAST(activity AS VARCHAR), '{FLOW_PATH_SEPARATOR}' ORDER BY sequence_no) AS pattern
            FROM {relation_name}
            GROUP BY case_id
        ),
        case_duration AS (
            SELECT
                case_id,
                ROUND(SUM(duration_sec), 2) AS case_duration_sec
            FROM {relation_name}
            GROUP BY case_id
        )
        SELECT
            case_patterns.pattern,
            COUNT(*) AS count,
            ROUND(AVG(case_duration.case_duration_sec), 2) AS avg_case_duration_sec,
            COUNT(*) * 1.0 / NULLIF((SELECT COUNT(*) FROM case_patterns), 0) AS ratio
        FROM case_patterns
        INNER JOIN case_duration USING (case_id)
        GROUP BY case_patterns.pattern
        ORDER BY count DESC, case_patterns.pattern ASC
        """
        summary_df = _query_dataframe(connection, cte_sql, params, query_sql)
        if limit is not None:
            summary_df = summary_df.head(max(0, int(limit))).reset_index(drop=True)
        variant_items = []
        for index, row in enumerate(summary_df.to_dict(orient="records"), start=1):
            pattern_text = str(row.get("pattern") or "")
            activities = [part.strip() for part in pattern_text.split(FLOW_PATH_SEPARATOR) if part.strip()]
            avg_case_duration_sec = round(float(row.get("avg_case_duration_sec") or 0.0), 2)
            variant_items.append(
                {
                    "variant_id": index,
                    "activities": activities,
                    "activity_count": len(activities),
                    "pattern": pattern_text,
                    "count": int(row.get("count") or 0),
                    "ratio": round(float(row.get("ratio") or 0.0), 4),
                    "avg_case_duration_sec": avg_case_duration_sec,
                    "avg_case_duration_text": _format_duration_text(avg_case_duration_sec),
                }
            )
        return variant_items
    finally:
        connection.close()


def _sort_bottleneck_rows(items, key_columns):
    return sorted(
        items,
        key=lambda item: (
            -float(item["avg_duration_sec"]),
            -float(item["median_duration_sec"]),
            -float(item["max_duration_sec"]),
            -int(item["count"]),
            *[str(item.get(key_column) or "") for key_column in key_columns],
        ),
    )


def query_bottleneck_summary(parquet_path, filter_params=None, filter_column_settings=None, variant_pattern=None, limit=None):
    connection = duckdb.connect()
    try:
        cte_sql, params, relation_name = _build_scoped_relation_cte(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
        )
        interval_cte_sql = f"""
        {cte_sql},
        interval_rows AS (
            SELECT
                activity,
                LEAD(activity) OVER (PARTITION BY case_id ORDER BY sequence_no) AS next_activity,
                case_id,
                duration_sec
            FROM {relation_name}
        )
        """
        activity_query = """
        SELECT
            activity,
            COUNT(*) AS count,
            COUNT(DISTINCT case_id) AS case_count,
            ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
            ROUND(MEDIAN(duration_sec), 2) AS median_duration_sec,
            ROUND(MAX(duration_sec), 2) AS max_duration_sec
        FROM interval_rows
        WHERE next_activity IS NOT NULL
        GROUP BY activity
        """
        transition_query = """
        SELECT
            activity AS from_activity,
            next_activity AS to_activity,
            COUNT(*) AS count,
            COUNT(DISTINCT case_id) AS case_count,
            ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
            ROUND(MEDIAN(duration_sec), 2) AS median_duration_sec,
            ROUND(MAX(duration_sec), 2) AS max_duration_sec
        FROM interval_rows
        WHERE next_activity IS NOT NULL
        GROUP BY activity, next_activity
        """
        activity_df = connection.execute(f"{interval_cte_sql}\n{activity_query}", params).df()
        transition_df = connection.execute(f"{interval_cte_sql}\n{transition_query}", params).df()
        activity_items = [
            {
                "activity": row["activity"],
                "count": int(row["count"]),
                "case_count": int(row["case_count"]),
                "avg_duration_sec": float(row["avg_duration_sec"]),
                "median_duration_sec": float(row["median_duration_sec"]),
                "max_duration_sec": float(row["max_duration_sec"]),
                "avg_duration_hours": round(float(row["avg_duration_sec"]) / 3600, 2),
                "median_duration_hours": round(float(row["median_duration_sec"]) / 3600, 2),
                "max_duration_hours": round(float(row["max_duration_sec"]) / 3600, 2),
            }
            for row in activity_df.to_dict(orient="records")
        ]
        transition_items = [
            {
                "from_activity": row["from_activity"],
                "to_activity": row["to_activity"],
                "transition_key": build_transition_key(row["from_activity"], row["to_activity"]),
                "count": int(row["count"]),
                "case_count": int(row["case_count"]),
                "avg_duration_sec": float(row["avg_duration_sec"]),
                "median_duration_sec": float(row["median_duration_sec"]),
                "max_duration_sec": float(row["max_duration_sec"]),
                "avg_duration_hours": round(float(row["avg_duration_sec"]) / 3600, 2),
                "median_duration_hours": round(float(row["median_duration_sec"]) / 3600, 2),
                "max_duration_hours": round(float(row["max_duration_sec"]) / 3600, 2),
            }
            for row in transition_df.to_dict(orient="records")
        ]
        activity_items = _sort_bottleneck_rows(activity_items, ["activity"])
        transition_items = _sort_bottleneck_rows(transition_items, ["from_activity", "to_activity"])
        if limit is not None:
            limited_activity_items = activity_items[: max(0, int(limit))]
            limited_transition_items = transition_items[: max(0, int(limit))]
        else:
            limited_activity_items = activity_items
            limited_transition_items = transition_items
        return {
            "activity_bottlenecks": limited_activity_items,
            "transition_bottlenecks": limited_transition_items,
            "activity_heatmap": _build_heatmap(activity_items, "activity"),
            "transition_heatmap": _build_heatmap(transition_items, "transition_key"),
        }
    finally:
        connection.close()


def query_impact_summary(parquet_path, filter_params=None, filter_column_settings=None, variant_pattern=None, limit=None):
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
                duration_sec
            FROM {relation_name}
        )
        SELECT
            from_activity,
            to_activity,
            COUNT(*) AS count,
            COUNT(DISTINCT case_id) AS case_count,
            ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
            ROUND(MAX(duration_sec), 2) AS max_duration_sec,
            ROUND(SUM(duration_sec), 2) AS total_duration_sec
        FROM interval_rows
        WHERE to_activity IS NOT NULL
        GROUP BY from_activity, to_activity
        """
        summary_df = _query_dataframe(connection, cte_sql, params, query_sql)
        if summary_df.empty:
            return {
                "has_data": False,
                "total_transition_count": 0,
                "returned_transition_count": 0,
                "rows": [],
            }
        total_wait_sec = float(summary_df["total_duration_sec"].sum())
        summary_df["wait_share_pct"] = (
            (summary_df["total_duration_sec"] / total_wait_sec) * 100
            if total_wait_sec > 0
            else 0.0
        )
        summary_df["impact_score"] = summary_df["avg_duration_sec"] * summary_df["case_count"]
        total_impact_score = float(summary_df["impact_score"].sum())
        summary_df["impact_share_pct"] = (
            (summary_df["impact_score"] / total_impact_score) * 100
            if total_impact_score > 0
            else 0.0
        )
        summary_df["transition_key"] = summary_df.apply(
            lambda row: build_transition_key(row["from_activity"], row["to_activity"]),
            axis=1,
        )
        summary_df["transition_label"] = summary_df.apply(
            lambda row: f"{row['from_activity']} {FLOW_PATH_SEPARATOR} {row['to_activity']}",
            axis=1,
        )
        summary_df = summary_df.sort_values(
            ["impact_score", "avg_duration_sec", "case_count", "transition_label"],
            ascending=[False, False, False, True],
        ).reset_index(drop=True)
        total_transition_count = int(len(summary_df))
        if limit is not None:
            summary_df = summary_df.head(max(0, int(limit))).reset_index(drop=True)
        numeric_columns = [
            "avg_duration_sec",
            "max_duration_sec",
            "wait_share_pct",
            "impact_score",
            "impact_share_pct",
        ]
        summary_df[numeric_columns] = summary_df[numeric_columns].round(2)
        rows = []
        for index, row in enumerate(summary_df.to_dict(orient="records"), start=1):
            rows.append(
                {
                    "rank": index,
                    "transition_label": row["transition_label"],
                    "from_activity": row["from_activity"],
                    "to_activity": row["to_activity"],
                    "transition_key": row["transition_key"],
                    "count": int(row["count"]),
                    "case_count": int(row["case_count"]),
                    "avg_duration_sec": float(row["avg_duration_sec"]),
                    "avg_duration_text": _format_duration_text(row["avg_duration_sec"]),
                    "max_duration_sec": float(row["max_duration_sec"]),
                    "max_duration_text": _format_duration_text(row["max_duration_sec"]),
                    "wait_share_pct": float(row["wait_share_pct"]),
                    "impact_score": float(row["impact_score"]),
                    "impact_share_pct": float(row["impact_share_pct"]),
                }
            )
        return {
            "has_data": True,
            "total_transition_count": total_transition_count,
            "returned_transition_count": len(rows),
            "rows": rows,
        }
    finally:
        connection.close()


def query_dashboard_summary(
    parquet_path,
    filter_params=None,
    filter_column_settings=None,
    variant_pattern=None,
    variant_items=None,
    bottleneck_summary=None,
    coverage_limit=10,
):
    connection = duckdb.connect()
    try:
        cte_sql, params, relation_name = _build_scoped_relation_cte(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
        )
        totals_query = f"""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT case_id) AS total_cases,
            COUNT(DISTINCT NULLIF(TRIM(CAST(activity AS VARCHAR)), '')) AS activity_type_count
        FROM {relation_name}
        """
        totals_row = connection.execute(f"{cte_sql}\n{totals_query}", params).fetchone()
        total_records = int(totals_row[0] or 0)
        total_cases = int(totals_row[1] or 0)
        activity_type_count = int(totals_row[2] or 0)

        if total_cases:
            duration_query = f"""
            SELECT
                ROUND(AVG(case_duration_sec), 2) AS avg_case_duration_sec,
                ROUND(MEDIAN(case_duration_sec), 2) AS median_case_duration_sec,
                ROUND(MAX(case_duration_sec), 2) AS max_case_duration_sec
            FROM (
                SELECT case_id, SUM(duration_sec) AS case_duration_sec
                FROM {relation_name}
                GROUP BY case_id
            )
            """
            duration_row = connection.execute(f"{cte_sql}\n{duration_query}", params).fetchone()
            avg_case_duration_sec = float(duration_row[0] or 0.0)
            median_case_duration_sec = float(duration_row[1] or 0.0)
            max_case_duration_sec = float(duration_row[2] or 0.0)
        else:
            avg_case_duration_sec = 0.0
            median_case_duration_sec = 0.0
            max_case_duration_sec = 0.0

        top_variant_items = list(variant_items or [])[: max(0, int(coverage_limit))]
        if variant_items is None:
            top_variant_items = query_variant_summary(
                parquet_path,
                filter_params=filter_params,
                filter_column_settings=filter_column_settings,
                variant_pattern=variant_pattern,
                limit=coverage_limit,
            )
        covered_case_count = sum(int(item["count"]) for item in top_variant_items)
        top10_variant_coverage_ratio = round(covered_case_count / total_cases, 4) if total_cases else 0.0

        resolved_bottleneck_summary = bottleneck_summary or query_bottleneck_summary(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
            limit=1,
        )
        top_transition_bottleneck = (
            resolved_bottleneck_summary["transition_bottlenecks"][0]
            if resolved_bottleneck_summary.get("transition_bottlenecks")
            else None
        )
        top_bottleneck_avg_wait_sec = float(top_transition_bottleneck["avg_duration_sec"]) if top_transition_bottleneck else 0.0
        return {
            "has_data": total_records > 0,
            "total_cases": total_cases,
            "total_records": total_records,
            "activity_type_count": activity_type_count,
            "avg_case_duration_sec": round(avg_case_duration_sec, 2),
            "avg_case_duration_text": _format_duration_text(avg_case_duration_sec),
            "median_case_duration_sec": round(median_case_duration_sec, 2),
            "median_case_duration_text": _format_duration_text(median_case_duration_sec),
            "max_case_duration_sec": round(max_case_duration_sec, 2),
            "max_case_duration_text": _format_duration_text(max_case_duration_sec),
            "top10_variant_coverage_ratio": top10_variant_coverage_ratio,
            "top10_variant_coverage_pct": round(top10_variant_coverage_ratio * 100, 2),
            "top_bottleneck_transition_label": (
                f"{top_transition_bottleneck['from_activity']} {FLOW_PATH_SEPARATOR} {top_transition_bottleneck['to_activity']}"
                if top_transition_bottleneck
                else ""
            ),
            "top_bottleneck_avg_wait_sec": top_bottleneck_avg_wait_sec,
            "top_bottleneck_avg_wait_hours": round(top_bottleneck_avg_wait_sec / 3600, 2) if top_transition_bottleneck else 0.0,
            "top_bottleneck_avg_wait_text": _format_duration_text(top_bottleneck_avg_wait_sec) if top_transition_bottleneck else "",
        }
    finally:
        connection.close()


def query_root_cause_summary(
    parquet_path,
    filter_params=None,
    filter_column_settings=None,
    variant_pattern=None,
    limit=10,
):
    normalized_column_settings = normalize_filter_column_settings(**(filter_column_settings or {}))
    filtered_meta = query_filtered_meta(
        parquet_path,
        filter_params=filter_params,
        filter_column_settings=filter_column_settings,
        variant_pattern=variant_pattern,
    )
    total_case_count = int(filtered_meta["case_count"])
    groups = []

    connection = duckdb.connect()
    try:
        cte_sql, params, relation_name = _build_scoped_relation_cte(
            parquet_path,
            filter_params=filter_params,
            filter_column_settings=filter_column_settings,
            variant_pattern=variant_pattern,
        )
        for filter_key in FILTER_SLOT_KEYS:
            filter_setting = normalized_column_settings.get(filter_key, {})
            column_name = filter_setting.get("column_name")
            if not column_name:
                continue
            value_expression = f"NULLIF(TRIM(CAST({_quote_identifier(column_name)} AS VARCHAR)), '')"
            query_sql = f"""
            , case_values AS (
                SELECT case_id, value
                FROM (
                    SELECT
                        case_id,
                        {value_expression} AS value,
                        ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY sequence_no) AS row_no
                    FROM {relation_name}
                    WHERE {value_expression} IS NOT NULL
                )
                WHERE row_no = 1
            ),
            case_duration AS (
                SELECT case_id, SUM(duration_sec) AS case_duration_sec
                FROM {relation_name}
                GROUP BY case_id
            )
            SELECT
                value,
                COUNT(DISTINCT case_values.case_id) AS case_count,
                ROUND(AVG(case_duration.case_duration_sec), 2) AS avg_case_duration_sec,
                ROUND(MEDIAN(case_duration.case_duration_sec), 2) AS median_case_duration_sec,
                ROUND(MAX(case_duration.case_duration_sec), 2) AS max_case_duration_sec
            FROM case_values
            INNER JOIN case_duration USING (case_id)
            GROUP BY value
            ORDER BY avg_case_duration_sec DESC, case_count DESC, value ASC
            """
            summary_df = _query_dataframe(connection, cte_sql, params, query_sql)
            total_value_count = int(len(summary_df))
            if limit is not None:
                summary_df = summary_df.head(max(0, int(limit))).reset_index(drop=True)
            rows = []
            for row in summary_df.to_dict(orient="records"):
                case_ratio_pct = round((float(row["case_count"]) / total_case_count) * 100, 2) if total_case_count else 0.0
                avg_case_duration_sec = float(row["avg_case_duration_sec"] or 0.0)
                median_case_duration_sec = float(row["median_case_duration_sec"] or 0.0)
                max_case_duration_sec = float(row["max_case_duration_sec"] or 0.0)
                rows.append(
                    {
                        "value": row["value"],
                        "case_count": int(row["case_count"]),
                        "case_ratio_pct": case_ratio_pct,
                        "avg_case_duration_sec": avg_case_duration_sec,
                        "avg_case_duration_text": _format_duration_text(avg_case_duration_sec),
                        "median_case_duration_sec": median_case_duration_sec,
                        "median_case_duration_text": _format_duration_text(median_case_duration_sec),
                        "max_case_duration_sec": max_case_duration_sec,
                        "max_case_duration_text": _format_duration_text(max_case_duration_sec),
                    }
                )
            groups.append(
                {
                    "slot": filter_key,
                    "label": filter_setting.get("label") or DEFAULT_FILTER_LABELS[filter_key],
                    "column_name": column_name,
                    "total_value_count": total_value_count,
                    "returned_value_count": len(rows),
                    "rows": rows,
                }
            )
        return {
            "has_data": filtered_meta["event_count"] > 0,
            "configured_group_count": len(groups),
            "groups": groups,
        }
    finally:
        connection.close()


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
                "duration_text": _format_duration_text(row["duration_sec"]),
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
                "duration_text": _format_duration_text(row["duration_sec"]),
                "from_time": pd.Timestamp(row["start_time"]).isoformat(),
                "to_time": pd.Timestamp(row["next_time"]).isoformat(),
            }
            for row in drilldown_df.to_dict(orient="records")
        ]
    finally:
        connection.close()


def query_case_trace_details(parquet_path, case_id):
    normalized_case_id = str(case_id or "").strip()
    if not normalized_case_id:
        raise ValueError("ケースIDが必要です。")
    connection = duckdb.connect()
    try:
        query_sql = """
        SELECT
            sequence_no,
            activity,
            start_time,
            next_time,
            duration_sec,
            LEAD(activity) OVER (ORDER BY sequence_no, start_time) AS next_activity
        FROM read_parquet(?)
        WHERE case_id = ?
        ORDER BY sequence_no ASC, start_time ASC
        """
        case_df = connection.execute(query_sql, [str(parquet_path), normalized_case_id]).df()
        if case_df.empty:
            return {
                "case_id": normalized_case_id,
                "found": False,
                "summary": None,
                "events": [],
            }
        total_duration_sec = round(float(case_df["duration_sec"].sum()), 2)
        start_time = pd.Timestamp(case_df["start_time"].min()).isoformat()
        end_time = pd.Timestamp(case_df["next_time"].max()).isoformat()
        events = []
        for row in case_df.to_dict(orient="records"):
            has_next_activity = isinstance(row["next_activity"], str) and bool(row["next_activity"])
            duration_sec = float(row["duration_sec"] or 0.0)
            events.append(
                {
                    "sequence_no": int(row["sequence_no"]),
                    "activity": row["activity"],
                    "timestamp": pd.Timestamp(row["start_time"]).isoformat(),
                    "next_activity": row["next_activity"] if has_next_activity else None,
                    "wait_to_next_sec": duration_sec if has_next_activity else None,
                    "wait_to_next_text": _format_duration_text(duration_sec) if has_next_activity else "",
                }
            )
        return {
            "case_id": normalized_case_id,
            "found": True,
            "summary": {
                "event_count": int(len(case_df)),
                "start_time": start_time,
                "end_time": end_time,
                "total_duration_sec": total_duration_sec,
                "total_duration_text": _format_duration_text(total_duration_sec),
            },
            "events": events,
        }
    finally:
        connection.close()
