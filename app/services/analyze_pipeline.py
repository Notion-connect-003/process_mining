from web_services.run_helpers import (
    get_form_filter_column_settings,
    get_form_filter_params,
    save_run_data,
    validate_filter_column_settings,
)

from 共通スクリプト.analysis_service import (
    analyze_prepared_event_log,
    build_group_summary,
    detect_group_columns,
    filter_prepared_df,
    load_prepared_event_log,
)


def get_form_value_or_default(form, key, default_value):
    raw_value = form.get(key)
    if raw_value is None:
        return default_value
    return str(raw_value).strip()


def parse_analyze_form(form, default_headers):
    return {
        "uploaded_file": form.get("csv_file"),
        "case_id_column": get_form_value_or_default(
            form,
            "case_id_column",
            default_headers["case_id_column"],
        ),
        "activity_column": get_form_value_or_default(
            form,
            "activity_column",
            default_headers["activity_column"],
        ),
        "timestamp_column": get_form_value_or_default(
            form,
            "timestamp_column",
            default_headers["timestamp_column"],
        ),
        "selected_analysis_keys": form.getlist("analysis_keys"),
        "filter_column_settings": get_form_filter_column_settings(form),
        "base_filter_params": get_form_filter_params(form),
    }


def resolve_analyze_file_source(uploaded_file, sample_file):
    if uploaded_file and uploaded_file.filename:
        uploaded_file.file.seek(0)
        return uploaded_file.file, uploaded_file.filename
    return sample_file, sample_file.name


def prepare_analysis_input_data(
    file_source,
    case_id_column,
    activity_column,
    timestamp_column,
    filter_column_settings,
    base_filter_params,
    read_raw_log_dataframe,
    resolve_required_column_name,
    validate_selected_columns,
):
    raw_df = read_raw_log_dataframe(file_source)
    headers = [str(column_name) for column_name in raw_df.columns.tolist()]
    resolved_case_id_column = resolve_required_column_name(
        headers,
        "case_id_column",
        case_id_column,
    )
    resolved_activity_column = resolve_required_column_name(
        headers,
        "activity_column",
        activity_column,
    )
    resolved_timestamp_column = resolve_required_column_name(
        headers,
        "timestamp_column",
        timestamp_column,
    )
    validate_selected_columns(
        case_id_column=resolved_case_id_column,
        activity_column=resolved_activity_column,
        timestamp_column=resolved_timestamp_column,
    )
    validate_filter_column_settings(filter_column_settings)
    prepared_df = load_prepared_event_log(
        file_source=file_source,
        case_id_column=resolved_case_id_column,
        activity_column=resolved_activity_column,
        timestamp_column=resolved_timestamp_column,
    )
    filtered_prepared_df = filter_prepared_df(
        prepared_df,
        base_filter_params,
        filter_column_settings=filter_column_settings,
    )
    group_columns = detect_group_columns(base_filter_params, filter_column_settings)
    return {
        "prepared_df": prepared_df,
        "filtered_prepared_df": filtered_prepared_df,
        "group_columns": group_columns,
        "case_id_column": resolved_case_id_column,
        "activity_column": resolved_activity_column,
        "timestamp_column": resolved_timestamp_column,
    }


def execute_analysis_pipeline(
    source_file_name,
    selected_analysis_keys,
    prepared_df,
    filtered_prepared_df,
    group_columns,
    case_id_column,
    activity_column,
    timestamp_column,
    filter_column_settings,
    base_filter_params,
    run_storage_dir,
    max_stored_runs,
):
    result = analyze_prepared_event_log(
        prepared_df=filtered_prepared_df,
        selected_analysis_keys=selected_analysis_keys,
        output_root_dir=None,
        export_excel=False,
        group_columns=group_columns if group_columns else None,
    )
    result["group_summary"] = (
        build_group_summary(filtered_prepared_df, group_columns)
        if group_columns
        else {}
    )
    run_id = save_run_data(
        source_file_name=source_file_name,
        selected_analysis_keys=selected_analysis_keys,
        prepared_df=prepared_df,
        result=result,
        column_settings={
            "case_id_column": case_id_column,
            "activity_column": activity_column,
            "timestamp_column": timestamp_column,
            **filter_column_settings,
        },
        base_filter_params=base_filter_params,
        run_storage_dir=run_storage_dir,
        max_stored_runs=max_stored_runs,
    )
    return run_id, result
