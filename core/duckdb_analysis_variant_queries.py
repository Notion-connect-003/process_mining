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
                    "avg_case_duration_text": format_duration_text(avg_case_duration_sec),
                }
            )
        return variant_items
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
                    "wait_to_next_text": format_duration_text(duration_sec) if has_next_activity else "",
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
                "total_duration_text": format_duration_text(total_duration_sec),
            },
            "events": events,
        }
    finally:
        connection.close()
