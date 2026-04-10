from web_services.ai_helpers import build_excel_ai_summary
from web_services.analysis_queries import (
    get_analysis_data,
    get_bottleneck_summary,
    get_dashboard_summary,
    get_filtered_meta_for_run,
    get_impact_summary,
    get_root_cause_summary,
    get_rule_based_insights_summary,
    get_run_group_columns,
    get_run_variant_pattern,
    get_variant_items,
)
from web_services.run_helpers import build_filter_summary_text

from 共通スクリプト.analysis_service import get_available_analysis_definitions
from 共通スクリプト.duckdb_service import (
    query_bottleneck_summary,
    query_period_text,
)


def get_analysis_export_sheet_keys(analysis_key):
    normalized_analysis_key = str(analysis_key or "").strip().lower()
    sheet_keys = ["summary", "ai_insights"]

    if normalized_analysis_key == "frequency":
        sheet_keys.append("frequency")
    elif normalized_analysis_key == "transition":
        sheet_keys.extend(["transition", "bottleneck", "impact"])
    elif normalized_analysis_key == "pattern":
        sheet_keys.extend(["pattern_summary", "pattern"])
    else:
        sheet_keys.append(normalized_analysis_key)

    return sheet_keys


def parse_transition_selection(selected_transition_key):
    normalized_key = str(selected_transition_key or "").strip()
    if "__TO__" not in normalized_key:
        return "", ""
    from_activity, to_activity = normalized_key.split("__TO__", 1)
    return from_activity.strip(), to_activity.strip()


def get_detail_export_bottleneck_summary(
    run_data,
    filter_params,
    variant_pattern=None,
    variant_id=None,
):
    if variant_pattern:
        return query_bottleneck_summary(
            run_data["prepared_parquet_path"],
            filter_params=filter_params,
            filter_column_settings=run_data.get("column_settings"),
            variant_pattern=variant_pattern,
            limit=None,
        )

    return get_bottleneck_summary(
        run_data,
        variant_id=variant_id,
        filter_params=filter_params,
    )


def build_detail_export_context(
    run_data,
    analysis_key,
    filter_params,
    *,
    selected_transition_key="",
    variant_id=None,
    generate_text,
    build_excel_ai_summary_fn=build_excel_ai_summary,
):
    analysis_definitions = get_available_analysis_definitions()
    analysis_name = analysis_definitions.get(analysis_key, {}).get("config", {}).get(
        "analysis_name",
        analysis_key,
    )
    export_sheet_keys = set(get_analysis_export_sheet_keys(analysis_key))
    variant_pattern = get_run_variant_pattern(
        run_data,
        variant_id=variant_id,
        filter_params=filter_params,
    )
    filtered_meta = get_filtered_meta_for_run(
        run_data,
        filter_params=filter_params,
        variant_pattern=variant_pattern,
    )
    selected_analysis = get_analysis_data(
        run_data,
        analysis_key,
        filter_params=filter_params,
        variant_pattern=variant_pattern,
    )
    export_variant_items = get_variant_items(
        run_data,
        filter_params=filter_params,
        variant_pattern=variant_pattern,
    )
    bottleneck_summary = get_detail_export_bottleneck_summary(
        run_data,
        filter_params=filter_params,
        variant_pattern=variant_pattern,
        variant_id=variant_id,
    )
    dashboard_summary = get_dashboard_summary(
        run_data,
        filter_params=filter_params,
        prepared_df=None,
        variant_pattern=variant_pattern,
    )
    impact_summary = get_impact_summary(
        run_data,
        filter_params=filter_params,
        prepared_df=None,
        variant_pattern=variant_pattern,
    )
    root_cause_summary = get_root_cause_summary(
        run_data,
        filter_params=filter_params,
        prepared_df=None,
        variant_pattern=variant_pattern,
    )
    insights_summary = get_rule_based_insights_summary(
        run_data,
        analysis_key=analysis_key,
        analysis_rows=selected_analysis.get("rows", []),
        filter_params=filter_params,
        prepared_df=None,
        variant_pattern=variant_pattern,
        dashboard_summary=dashboard_summary,
        bottleneck_summary=bottleneck_summary,
        impact_summary=impact_summary,
    )
    ai_summary = build_excel_ai_summary_fn(
        run_data=run_data,
        analysis_key=analysis_key,
        analysis_name=analysis_name,
        filter_params=filter_params,
        prepared_df=None,
        variant_pattern=variant_pattern,
        dashboard_summary=dashboard_summary,
        impact_summary=impact_summary,
        bottleneck_summary=bottleneck_summary,
        analysis=selected_analysis,
        root_cause_summary=root_cause_summary,
        insights_summary=insights_summary,
        variant_items=export_variant_items[:5],
        use_cache=variant_id is None,
        generate_text=generate_text,
    )
    from_activity, to_activity = parse_transition_selection(selected_transition_key)
    selected_transition_label = (
        f"{from_activity} → {to_activity}"
        if from_activity and to_activity
        else str(selected_transition_key or "").strip()
    )
    period_text = query_period_text(
        run_data["prepared_parquet_path"],
        filter_params=filter_params,
        filter_column_settings=run_data.get("column_settings"),
        variant_pattern=variant_pattern,
    )
    group_columns = get_run_group_columns(run_data)

    return {
        "analysis_definitions": analysis_definitions,
        "analysis_name": analysis_name,
        "export_sheet_keys": export_sheet_keys,
        "variant_pattern": variant_pattern,
        "applied_filter_summary_text": build_filter_summary_text(
            filter_params,
            run_data.get("column_settings"),
        ),
        "filtered_meta": filtered_meta,
        "selected_analysis": selected_analysis,
        "export_variant_items": export_variant_items,
        "bottleneck_summary": bottleneck_summary,
        "dashboard_summary": dashboard_summary,
        "impact_summary": impact_summary,
        "root_cause_summary": root_cause_summary,
        "insights_summary": insights_summary,
        "ai_summary": ai_summary,
        "from_activity": from_activity,
        "to_activity": to_activity,
        "selected_transition_label": selected_transition_label,
        "period_text": period_text,
        "group_columns": group_columns,
    }


def load_optional_detail_section(include_section, section_name, deferred_sections, loader):
    if not include_section:
        deferred_sections.append(section_name)
        return None
    return loader()


def collect_analysis_detail_sections(
    run_data,
    analysis_key,
    analysis_rows,
    filter_params,
    *,
    include_dashboard=True,
    include_impact=True,
    include_root_cause=True,
    include_insights=True,
):
    filtered_df = None
    deferred_sections = []
    dashboard_summary = load_optional_detail_section(
        include_dashboard,
        "dashboard",
        deferred_sections,
        lambda: get_dashboard_summary(
            run_data,
            filter_params=filter_params,
            prepared_df=filtered_df,
        ),
    )
    impact_summary = load_optional_detail_section(
        include_impact,
        "impact",
        deferred_sections,
        lambda: get_impact_summary(
            run_data,
            filter_params=filter_params,
            prepared_df=filtered_df,
        ),
    )
    root_cause_summary = load_optional_detail_section(
        include_root_cause,
        "root_cause",
        deferred_sections,
        lambda: get_root_cause_summary(
            run_data,
            filter_params=filter_params,
            prepared_df=filtered_df,
        ),
    )
    insights_summary = load_optional_detail_section(
        include_insights,
        "insights",
        deferred_sections,
        lambda: get_rule_based_insights_summary(
            run_data,
            analysis_key=analysis_key,
            analysis_rows=analysis_rows,
            filter_params=filter_params,
            prepared_df=filtered_df,
            dashboard_summary=dashboard_summary,
            impact_summary=impact_summary,
        ),
    )
    return {
        "dashboard": dashboard_summary,
        "impact": impact_summary,
        "root_cause": root_cause_summary,
        "insights": insights_summary,
        "deferred_sections": deferred_sections,
    }
