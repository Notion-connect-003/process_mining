from app.services.ai_helpers import build_excel_ai_summary
from app.services.analysis_queries import (
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
from app.services.run_helpers import build_filter_summary_text

from core.analysis_service import get_available_analysis_definitions
from core.duckdb_service import (
    query_bottleneck_summary,
    query_group_summary,
    query_period_text,
)


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
