import inspect
import json
from pathlib import Path

import duckdb
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from web_config.app_settings import (
    BASE_DIR as APP_BASE_DIR,
    COLUMN_CANDIDATES as APP_COLUMN_CANDIDATES,
    COLUMN_DISPLAY_LABELS as APP_COLUMN_DISPLAY_LABELS,
    DEFAULT_HEADERS as APP_DEFAULT_HEADERS,
    LARGE_DATASET_FLOW_FAST_PATH_THRESHOLD as APP_LARGE_DATASET_FLOW_FAST_PATH_THRESHOLD,
    MAX_STORED_RUNS as APP_MAX_STORED_RUNS,
    PREVIEW_ROW_COUNT as APP_PREVIEW_ROW_COUNT,
    PROFILE_SAMPLE_FILE as APP_PROFILE_SAMPLE_FILE,
    RUN_STORAGE_DIR as APP_RUN_STORAGE_DIR,
    SAMPLE_FILE as APP_SAMPLE_FILE,
)
from web_reports.excel_common import (
    ANALYSIS_PRECONDITIONS_TEXT,
    APPLIED_FILTERS_NOTE_TEXT,
    EXCEL_ALT_ROW_FILL,
    EXCEL_ASSUMPTION_SECTION_FILL,
    EXCEL_BODY_FONT,
    EXCEL_BOLD_FONT,
    EXCEL_GROUP_SECTION_FILL,
    EXCEL_GROUP_SECTION_FONT,
    EXCEL_HEADER_FILL,
    EXCEL_LABEL_FILL,
    EXCEL_MUTED_FONT,
    EXCEL_MUTED_SECTION_FILL,
    EXCEL_NOTE_FONT,
    EXCEL_SECTION_FILL,
    EXCEL_SUBTITLE_FILL,
    EXCEL_TEXT_BLOCK_FILL,
    EXCEL_THIN_BORDER,
    EXCEL_TITLE_BORDER,
    EXCEL_TITLE_FILL,
    EXCEL_TITLE_FONT,
    GROUPING_CONDITION_NOTE_TEXT,
    REPORT_HEADER_LABELS,
    REPORT_SHEET_NAMES,
    TERMINOLOGY_ROWS,
    append_bullet_rows,
    append_custom_text_section_to_worksheet,
    append_definition_table_to_worksheet,
    append_key_value_rows,
    append_table_to_worksheet,
    append_text_block_to_worksheet,
    autosize_worksheet_columns,
    build_analysis_excel_file_name,
    estimate_wrapped_row_height,
    initialize_excel_worksheet,
    merge_excel_row,
    normalize_excel_cell_value,
    resolve_analysis_display_name,
    sanitize_workbook_sheet_name,
    style_excel_cell,
)

from web_reports.detail_report import (
    build_detail_export_workbook_bytes,
    build_transition_display_label,
)
from web_services.ai_helpers import (
    build_ai_insights_summary,
    build_empty_ai_summary,
    build_excel_ai_summary,
    get_cached_ai_summary,
)
from web_services.llm_helpers import (
    build_bottleneck_prompt as build_bottleneck_prompt_impl,
    request_ollama_insights_text as request_ollama_insights_text_impl,
)
from web_services.analyze_pipeline import (
    execute_analysis_pipeline,
    parse_analyze_form,
    prepare_analysis_input_data,
    resolve_analyze_file_source,
)
from web_routes.detail import register_detail_routes
from web_routes.flow import register_flow_routes
from web_routes.ingest import register_ingest_routes
from web_services.detail_context import (
    build_detail_export_context as build_detail_export_context_impl,
    collect_analysis_detail_sections as collect_analysis_detail_sections_impl,
    get_analysis_export_sheet_keys,
    parse_transition_selection,
)
from web_services.support_helpers import (
    build_analysis_payload as build_analysis_payload_impl,
    build_column_selection_payload as build_column_selection_payload_impl,
    build_log_profile_payload as build_log_profile_payload_impl,
    build_preview_response as build_preview_response_impl,
    build_variant_coverage_payload,
    get_analysis_options as get_analysis_options_impl,
    get_filter_options_payload as get_filter_options_payload_impl,
    get_static_version as get_static_version_impl,
    read_raw_log_dataframe,
    resolve_profile_file_source as resolve_profile_file_source_impl,
    resolve_required_column_name as resolve_required_column_name_impl,
    suggest_column_name as suggest_column_name_impl,
    validate_selected_columns,
)
from web_services.run_helpers import (
    build_column_settings_payload,
    build_filter_summary_text,
    get_effective_filter_params,
    get_form_filter_column_settings,
    get_request_filter_params,
    get_run_data,
    has_parquet_backing,
)
from web_reports.log_diagnostic_report import (
    build_log_diagnostic_workbook_bytes,
    resolve_log_diagnostic_sample_row_limit,
)

from web_services.analysis_queries import (
    build_filter_cache_key,
    build_pattern_index_entries_from_rows,
    build_variant_response_item,
    extract_pattern_text_from_row,
    get_analysis_data,
    get_bottleneck_summary,
    get_dashboard_summary,
    get_filtered_meta_for_run,
    get_impact_summary,
    get_pattern_flow_snapshot,
    get_pattern_summary_row,
    get_root_cause_summary,
    get_rule_based_insights_summary,
    get_run_group_columns,
    get_run_variant_pattern,
    get_variant_items,
)

from 共通スクリプト.Excel出力.excel_exporter import (
    build_excel_bytes,
    build_summary_sheet_df,
    convert_analysis_result_to_records,
)
from 共通スクリプト.analysis_service import (
    DEFAULT_ANALYSIS_KEYS,
    create_analysis_records,
    create_log_diagnostics,
    create_pattern_flow_snapshot,
    filter_prepared_df,
    filter_prepared_df_by_pattern,
    get_filter_options,
    create_variant_summary,
    get_available_analysis_definitions,
    load_prepared_event_log,
    merge_filter_params,
    normalize_filter_params,
    normalize_filter_column_settings,
)
from 共通スクリプト.duckdb_service import (
    _build_scoped_relation_cte,
    _format_stddev_column,
    _get_parquet_column_names,
    _quote_identifier,
    persist_prepared_parquet,
    query_activity_case_drilldown,
    query_analysis_records,
    query_bottleneck_summary,
    query_case_trace_details,
    query_filter_options,
    query_group_summary,
    query_pattern_bottleneck_details,
    query_period_text,
    query_transition_case_drilldown,
)


BASE_DIR = APP_BASE_DIR
SAMPLE_FILE = APP_SAMPLE_FILE
PROFILE_SAMPLE_FILE = APP_PROFILE_SAMPLE_FILE
RUN_STORAGE_DIR = APP_RUN_STORAGE_DIR
MAX_STORED_RUNS = APP_MAX_STORED_RUNS
PREVIEW_ROW_COUNT = APP_PREVIEW_ROW_COUNT
LARGE_DATASET_FLOW_FAST_PATH_THRESHOLD = APP_LARGE_DATASET_FLOW_FAST_PATH_THRESHOLD
DEFAULT_HEADERS = APP_DEFAULT_HEADERS
COLUMN_CANDIDATES = APP_COLUMN_CANDIDATES
COLUMN_DISPLAY_LABELS = APP_COLUMN_DISPLAY_LABELS

app = FastAPI(title="Process Mining Workbench")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Starlette <0.41: TemplateResponse(name, context)  context must have "request" key
# Starlette >=0.41: TemplateResponse(request, name, context)
_STARLETTE_OLD_TEMPLATE_API = (
    list(inspect.signature(templates.TemplateResponse).parameters.keys())[0] == "name"
)


def _template_response(request: Request, name: str, context: dict):
    ctx = {"request": request, **context}
    if _STARLETTE_OLD_TEMPLATE_API:
        return templates.TemplateResponse(name, ctx)
    return templates.TemplateResponse(request, name, ctx)


def request_ollama_insights_text(prompt, model="qwen2.5:7b"):
    return request_ollama_insights_text_impl(prompt, model=model)


register_detail_routes(
    app,
    sample_file=SAMPLE_FILE,
    default_headers=DEFAULT_HEADERS,
    template_response=_template_response,
    read_raw_log_dataframe=read_raw_log_dataframe,
    build_log_profile_payload=lambda raw_df, source_file_name, case_id_column="", activity_column="", timestamp_column="", filter_column_settings=None, include_diagnostics=False: build_log_profile_payload_impl(
        raw_df,
        source_file_name,
        case_id_column=case_id_column,
        activity_column=activity_column,
        timestamp_column=timestamp_column,
        filter_column_settings=filter_column_settings,
        include_diagnostics=include_diagnostics,
        default_headers=DEFAULT_HEADERS,
        column_candidates=COLUMN_CANDIDATES,
        build_column_settings_payload_fn=build_column_settings_payload,
    ),
    get_analysis_options=lambda: get_analysis_options_impl(
        DEFAULT_ANALYSIS_KEYS,
        get_available_analysis_definitions,
    ),
    get_static_version=lambda: get_static_version_impl(BASE_DIR),
    get_available_analysis_definitions=get_available_analysis_definitions,
    get_run_data=get_run_data,
    get_pattern_summary_row=get_pattern_summary_row,
    query_pattern_bottleneck_details=query_pattern_bottleneck_details,
    resolve_request_filter_params=lambda request, run_data: get_effective_filter_params(
        run_data,
        get_request_filter_params(request),
    ),
    get_analysis_data=get_analysis_data,
    get_filtered_meta_for_run=get_filtered_meta_for_run,
    build_analysis_payload=build_analysis_payload_impl,
    collect_analysis_detail_sections=collect_analysis_detail_sections_impl,
    build_column_settings_payload=build_column_settings_payload,
    get_cached_ai_summary=get_cached_ai_summary,
    build_empty_ai_summary=build_empty_ai_summary,
    build_ai_insights_summary=build_ai_insights_summary,
    get_request_ollama_insights_text=lambda: request_ollama_insights_text,
    build_excel_bytes=build_excel_bytes,
    build_analysis_excel_file_name=build_analysis_excel_file_name,
    build_detail_export_context=lambda run_data, analysis_key, filter_params, selected_transition_key="", variant_id=None: build_detail_export_context_impl(
        run_data,
        analysis_key,
        filter_params,
        selected_transition_key=selected_transition_key,
        variant_id=variant_id,
        generate_text=request_ollama_insights_text,
        build_excel_ai_summary_fn=build_excel_ai_summary,
    ),
    build_detail_export_workbook_bytes=build_detail_export_workbook_bytes,
    resolve_analysis_display_name=resolve_analysis_display_name,
    get_filter_options_payload=lambda run_data: get_filter_options_payload_impl(
        run_data,
        query_filter_options,
    ),
)


register_flow_routes(
    app,
    get_run_data=get_run_data,
    get_effective_filter_params=get_effective_filter_params,
    get_request_filter_params=get_request_filter_params,
    get_analysis_data=get_analysis_data,
    get_pattern_flow_snapshot=get_pattern_flow_snapshot,
    get_filtered_meta_for_run=get_filtered_meta_for_run,
    build_variant_response_item=build_variant_response_item,
    build_variant_coverage_payload=build_variant_coverage_payload,
    get_variant_items=get_variant_items,
    get_run_variant_pattern=get_run_variant_pattern,
    get_bottleneck_summary=get_bottleneck_summary,
    query_transition_case_drilldown=query_transition_case_drilldown,
    query_activity_case_drilldown=query_activity_case_drilldown,
    query_case_trace_details=query_case_trace_details,
    get_pattern_flow_snapshot_builder=lambda: create_pattern_flow_snapshot,
    get_large_dataset_flow_fast_path_threshold=lambda: LARGE_DATASET_FLOW_FAST_PATH_THRESHOLD,
)


def build_bottleneck_prompt(data: dict) -> str:
    return build_bottleneck_prompt_impl(data)


register_ingest_routes(
    app,
    sample_file=SAMPLE_FILE,
    default_headers=DEFAULT_HEADERS,
    resolve_profile_file_source=lambda form: resolve_profile_file_source_impl(
        form,
        PROFILE_SAMPLE_FILE,
    ),
    read_raw_log_dataframe=read_raw_log_dataframe,
    build_log_profile_payload=lambda raw_df, source_file_name, case_id_column="", activity_column="", timestamp_column="", filter_column_settings=None, include_diagnostics=False: build_log_profile_payload_impl(
        raw_df,
        source_file_name,
        case_id_column=case_id_column,
        activity_column=activity_column,
        timestamp_column=timestamp_column,
        filter_column_settings=filter_column_settings,
        include_diagnostics=include_diagnostics,
        default_headers=DEFAULT_HEADERS,
        column_candidates=COLUMN_CANDIDATES,
        build_column_settings_payload_fn=build_column_settings_payload,
    ),
    get_form_filter_column_settings=get_form_filter_column_settings,
    resolve_log_diagnostic_sample_row_limit=resolve_log_diagnostic_sample_row_limit,
    build_log_diagnostic_workbook_bytes=build_log_diagnostic_workbook_bytes,
    build_analysis_excel_file_name=build_analysis_excel_file_name,
    parse_analyze_form=parse_analyze_form,
    resolve_analyze_file_source=resolve_analyze_file_source,
    prepare_analysis_input_data=prepare_analysis_input_data,
    execute_analysis_pipeline=execute_analysis_pipeline,
    run_storage_dir=RUN_STORAGE_DIR,
    max_stored_runs=MAX_STORED_RUNS,
    resolve_required_column_name=lambda headers, field_name, preferred_name="": resolve_required_column_name_impl(
        headers,
        field_name,
        preferred_name=preferred_name,
        default_headers=DEFAULT_HEADERS,
        column_candidates=COLUMN_CANDIDATES,
        column_display_labels=COLUMN_DISPLAY_LABELS,
    ),
    validate_selected_columns=validate_selected_columns,
    build_preview_response=lambda run_id, source_file_name, selected_analysis_keys, result, run_data: build_preview_response_impl(
        run_id,
        source_file_name,
        selected_analysis_keys,
        result,
        run_data,
        build_column_settings_payload_fn=build_column_settings_payload,
        get_filter_options_payload_fn=lambda current_run_data: get_filter_options_payload_impl(
            current_run_data,
            query_filter_options,
        ),
        preview_row_count=PREVIEW_ROW_COUNT,
    ),
    get_run_data=get_run_data,
    build_bottleneck_prompt=build_bottleneck_prompt,
    httpx_module=httpx,
)


if __name__ == "__main__":
    uvicorn.run("web_app:app", host="127.0.0.1", port=5000, reload=True)
