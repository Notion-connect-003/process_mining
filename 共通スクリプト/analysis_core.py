from 共通スクリプト.analysis_constants import (
    ANALYSIS_DEFINITIONS,
    DEFAULT_ANALYSIS_KEYS,
    FLOW_PATH_SEPARATOR,
    FILTER_SLOT_KEYS,
    DEFAULT_FILTER_LABELS,
    TRANSITION_ANALYSIS_CONFIG,
    PATTERN_ANALYSIS_CONFIG,
    FREQUENCY_ANALYSIS_CONFIG,
    prepare_event_log,
    read_csv_data,
    convert_analysis_result_to_records,
    export_analysis_to_excel,
    create_transition_analysis,
    create_pattern_analysis,
    create_frequency_analysis,
)


# -----------------------------------------------------------------------------
# Analysis execution
# -----------------------------------------------------------------------------


def get_available_analysis_definitions():
    return ANALYSIS_DEFINITIONS.copy()


def resolve_analysis_keys(selected_analysis_keys=None):
    if selected_analysis_keys is None:
        analysis_keys = DEFAULT_ANALYSIS_KEYS
    else:
        analysis_keys = selected_analysis_keys

    if not analysis_keys:
        raise ValueError("少なくとも1つの分析を選択してください。")

    return analysis_keys


def load_prepared_event_log(
    file_source,
    case_id_column,
    activity_column,
    timestamp_column,
):
    raw_df = read_csv_data(
        file_path=file_source,
        case_id_column=case_id_column,
        activity_column=activity_column,
    )
    return prepare_event_log(
        df=raw_df,
        case_id_column=case_id_column,
        activity_column=activity_column,
        timestamp_column=timestamp_column,
    )


def analyze_prepared_event_log(
    prepared_df,
    selected_analysis_keys=None,
    output_root_dir=None,
    export_excel=False,
    group_columns=None,
):
    from 共通スクリプト.analysis_filters import build_group_summary
    analysis_keys = resolve_analysis_keys(selected_analysis_keys)
    analysis_results = {}

    for analysis_key in analysis_keys:
        if analysis_key not in ANALYSIS_DEFINITIONS:
            raise ValueError(f"未対応の分析種別です: {analysis_key}")

        definition = ANALYSIS_DEFINITIONS[analysis_key]
        result_df = definition["create_function"](prepared_df, group_columns=group_columns)
        analysis_config = definition["config"]

        excel_file = None
        if export_excel:
            _group_summary = build_group_summary(prepared_df, group_columns) if group_columns else None
            excel_file = export_analysis_to_excel(
                df=result_df,
                output_root_dir=output_root_dir,
                analysis_name=analysis_config["analysis_name"],
                output_file_name=analysis_config["output_file_name"],
                sheet_name=analysis_config["sheet_name"],
                display_columns=analysis_config["display_columns"],
                group_columns=group_columns,
                group_summary=_group_summary,
            )

        analysis_results[analysis_key] = {
            "analysis_name": analysis_config["analysis_name"],
            "sheet_name": analysis_config["sheet_name"],
            "output_file_name": analysis_config["output_file_name"],
            "rows": convert_analysis_result_to_records(
                result_df,
                analysis_config["display_columns"],
                group_columns=group_columns,
            ),
            "excel_file": str(excel_file.resolve()) if excel_file else None,
        }

    return {
        "case_count": int(prepared_df["case_id"].nunique()),
        "event_count": int(len(prepared_df)),
        "group_columns": group_columns or [],
        "group_mode": bool(group_columns),
        "analyses": analysis_results,
    }


def create_analysis_records(prepared_df, analysis_key):
    if analysis_key not in ANALYSIS_DEFINITIONS:
        raise ValueError(f"未対応の分析種別です: {analysis_key}")

    definition = ANALYSIS_DEFINITIONS[analysis_key]
    analysis_config = definition["config"]
    result_df = definition["create_function"](prepared_df, group_columns=None)

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


# -----------------------------------------------------------------------------
# Variant and pattern helpers
# -----------------------------------------------------------------------------

def build_case_variant_table(prepared_df):
    return (
        prepared_df.sort_values(["case_id", "sequence_no"])
        .groupby("case_id")["activity"]
        .apply(lambda series: tuple(series.tolist()))
        .reset_index(name="activities")
    )


def build_case_pattern_table(prepared_df):
    case_variant_df = build_case_variant_table(prepared_df)
    case_variant_df["pattern"] = case_variant_df["activities"].apply(
        lambda activities: FLOW_PATH_SEPARATOR.join(activities)
    )
    return case_variant_df[["case_id", "pattern"]]


def create_variant_summary(prepared_df, limit=10):
    from 共通スクリプト.analysis_insights import _format_duration_text
    case_variant_df = build_case_variant_table(prepared_df)
    case_variant_df["pattern"] = case_variant_df["activities"].apply(
        lambda activities: FLOW_PATH_SEPARATOR.join(activities)
    )
    case_duration_df = (
        prepared_df.groupby("case_id", as_index=False)
        .agg(case_duration_sec=("duration_sec", "sum"))
    )
    case_variant_df = case_variant_df.merge(case_duration_df, on="case_id", how="left")

    total_cases = int(case_variant_df["case_id"].nunique())
    variant_summary_df = (
        case_variant_df.groupby(["activities", "pattern"])
        .agg(
            count=("case_id", "count"),
            avg_case_duration_sec=("case_duration_sec", "mean"),
        )
        .reset_index()
        .sort_values(["count", "pattern"], ascending=[False, True])
        .reset_index(drop=True)
    )
    variant_summary_df["avg_case_duration_sec"] = (
        variant_summary_df["avg_case_duration_sec"]
        .fillna(0)
        .round(2)
    )

    if limit is not None:
        variant_summary_df = variant_summary_df.head(max(0, int(limit))).reset_index(drop=True)

    return [
        {
            "variant_id": index + 1,
            "activities": list(row["activities"]),
            "activity_count": int(len(row["activities"])),
            "pattern": row["pattern"],
            "count": int(row["count"]),
            "ratio": round(float(row["count"]) / total_cases, 4) if total_cases else 0.0,
            "avg_case_duration_sec": float(row["avg_case_duration_sec"]),
            "avg_case_duration_text": _format_duration_text(row["avg_case_duration_sec"]),
        }
        for index, row in enumerate(variant_summary_df.to_dict(orient="records"))
    ]


def create_pattern_index_entries(prepared_df):
    pattern_summary_df = create_pattern_analysis(prepared_df)
    return pattern_summary_df.to_dict(orient="records")


HEAT_CLASS_COUNT = 5


def build_transition_key(from_activity, to_activity):
    return f"{from_activity}__TO__{to_activity}"
