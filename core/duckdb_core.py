from pathlib import Path
from functools import lru_cache

import duckdb
import pandas as pd

from excel.basic_exporter import convert_analysis_result_to_records
from core.analysis_constants import (
    DEFAULT_FILTER_LABELS,
    FILTER_SLOT_KEYS,
    FLOW_PATH_SEPARATOR,
)
from core.analysis_core import build_transition_key
from core.analysis_filters import (
    normalize_filter_column_settings,
    normalize_filter_params,
)
from core.analysis_core import build_heatmap, format_duration_text
from core.分析.前後処理分析.transition_analysis import ANALYSIS_CONFIG as TRANSITION_ANALYSIS_CONFIG
from core.分析.処理順パターン分析.pattern_analysis import (
    ANALYSIS_CONFIG as PATTERN_ANALYSIS_CONFIG,
    enrich_pattern_analysis_result,
)
from core.分析.頻度分析.frequency_analysis import ANALYSIS_CONFIG as FREQUENCY_ANALYSIS_CONFIG


ANALYSIS_CONFIGS = {
    "frequency": FREQUENCY_ANALYSIS_CONFIG,
    "transition": TRANSITION_ANALYSIS_CONFIG,
    "pattern": PATTERN_ANALYSIS_CONFIG,
}


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


def _normalize_multi_value_filter(raw_value):
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

    start_activity_values = _normalize_multi_value_filter(normalized_filters.get("start_activity_values"))
    end_activity_values = _normalize_multi_value_filter(normalized_filters.get("end_activity_values"))
    if start_activity_values or end_activity_values:
        case_endpoint_clauses = []
        case_endpoint_params = []

        if start_activity_values:
            start_placeholders = ", ".join("?" for _ in start_activity_values)
            case_endpoint_clauses.append(
                f"""
                case_id IN (
                    SELECT case_id
                    FROM case_endpoint_ranked
                    WHERE rn_first = 1
                      AND TRIM(CAST(activity AS VARCHAR)) IN ({start_placeholders})
                )
                """.strip()
            )
            case_endpoint_params.extend(start_activity_values)

        if end_activity_values:
            end_placeholders = ", ".join("?" for _ in end_activity_values)
            case_endpoint_clauses.append(
                f"""
                case_id IN (
                    SELECT case_id
                    FROM case_endpoint_ranked
                    WHERE rn_last = 1
                      AND TRIM(CAST(activity AS VARCHAR)) IN ({end_placeholders})
                )
                """.strip()
            )
            case_endpoint_params.extend(end_activity_values)

        ctes.append(
            f"""
            case_endpoint_ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY case_id
                        ORDER BY COALESCE(start_time, timestamp) ASC, sequence_no ASC, activity ASC
                    ) AS rn_first,
                    ROW_NUMBER() OVER (
                        PARTITION BY case_id
                        ORDER BY COALESCE(start_time, timestamp) DESC, sequence_no DESC, activity DESC
                    ) AS rn_last
                FROM {current_relation}
            ),
            case_endpoint_valid AS (
                SELECT DISTINCT case_id
                FROM case_endpoint_ranked
                WHERE {' AND '.join(case_endpoint_clauses)}
            ),
            case_endpoint_scoped AS (
                SELECT scoped.*
                FROM {current_relation} AS scoped
                INNER JOIN case_endpoint_valid USING (case_id)
            )
            """.strip()
        )
        params.extend(case_endpoint_params)
        current_relation = "case_endpoint_scoped"

    activity_mode = normalized_filters.get("activity_mode")
    activity_values = _normalize_multi_value_filter(normalized_filters.get("activity_values"))
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
