import duckdb
import pandas as pd

from 共通スクリプト.duckdb_core import *


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
