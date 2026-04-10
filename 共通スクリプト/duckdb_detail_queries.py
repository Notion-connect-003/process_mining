import duckdb
import pandas as pd

from 共通スクリプト.duckdb_core import *
from 共通スクリプト.duckdb_analysis_queries import query_filtered_meta, query_variant_summary


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
