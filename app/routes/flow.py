from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse


def register_flow_routes(
    app,
    *,
    get_run_data,
    get_effective_filter_params,
    get_request_filter_params,
    get_analysis_data,
    get_pattern_flow_snapshot,
    get_filtered_meta_for_run,
    build_variant_response_item,
    build_variant_coverage_payload,
    get_variant_items,
    get_run_variant_pattern,
    get_bottleneck_summary,
    query_transition_case_drilldown,
    query_activity_case_drilldown,
    query_case_trace_details,
    get_pattern_flow_snapshot_builder,
    get_large_dataset_flow_fast_path_threshold,
):
    @app.get("/api/runs/{run_id}/pattern-flow")
    def pattern_flow_api(
        request: Request,
        run_id: str,
        pattern_percent: int = 10,
        pattern_count: int | None = None,
        activity_percent: int = 40,
        connection_percent: int = 30,
        variant_id: int | None = None,
    ):
        run_data = get_run_data(run_id)
        filter_params = get_effective_filter_params(
            run_data,
            get_request_filter_params(request),
        )
        pattern_analysis = get_analysis_data(
            run_data,
            "pattern",
            filter_params=filter_params,
        )

        if not pattern_analysis and variant_id is None:
            raise HTTPException(status_code=400, detail="処理順パターン分析を有効化してください。")

        snapshot = get_pattern_flow_snapshot(
            run_data=run_data,
            pattern_percent=pattern_percent,
            pattern_count=pattern_count,
            activity_percent=activity_percent,
            connection_percent=connection_percent,
            variant_id=variant_id,
            filter_params=filter_params,
            snapshot_builder=get_pattern_flow_snapshot_builder(),
            large_dataset_flow_fast_path_threshold=get_large_dataset_flow_fast_path_threshold(),
        )
        filtered_meta = get_filtered_meta_for_run(
            run_data,
            filter_params=filter_params,
        )

        return JSONResponse(
            content={
                "run_id": run_id,
                "filtered_case_count": filtered_meta["case_count"],
                "filtered_event_count": filtered_meta["event_count"],
                "applied_filters": filter_params,
                **snapshot,
            }
        )

    @app.get("/api/runs/{run_id}/variants")
    def variant_list_api(request: Request, run_id: str, limit: int = 10):
        run_data = get_run_data(run_id)
        filter_params = get_effective_filter_params(
            run_data,
            get_request_filter_params(request),
        )
        safe_limit = max(0, int(limit))
        filtered_meta = get_filtered_meta_for_run(
            run_data,
            filter_params=filter_params,
        )
        all_variant_items = get_variant_items(run_data, filter_params=filter_params)
        variant_items = all_variant_items if safe_limit == 0 else all_variant_items[:safe_limit]

        return JSONResponse(
            content={
                "run_id": run_id,
                "variants": [
                    build_variant_response_item(variant_item, run_data=run_data)
                    for variant_item in variant_items
                ],
                "coverage": build_variant_coverage_payload(
                    total_case_count=filtered_meta["case_count"],
                    variant_items=variant_items,
                ),
                "filtered_case_count": filtered_meta["case_count"],
                "filtered_event_count": filtered_meta["event_count"],
                "applied_filters": filter_params,
            }
        )

    @app.get("/api/runs/{run_id}/bottlenecks")
    def bottleneck_list_api(
        request: Request,
        run_id: str,
        limit: int = 5,
        variant_id: int | None = None,
        pattern_index: int | None = None,
    ):
        run_data = get_run_data(run_id)
        filter_params = get_effective_filter_params(
            run_data,
            get_request_filter_params(request),
        )
        safe_limit = max(0, int(limit))
        variant_pattern = get_run_variant_pattern(
            run_data,
            variant_id=variant_id,
            pattern_index=pattern_index,
            filter_params=filter_params,
        )
        bottleneck_summary = get_bottleneck_summary(
            run_data,
            variant_id=variant_id,
            pattern_index=pattern_index,
            filter_params=filter_params,
        )
        filtered_meta = get_filtered_meta_for_run(
            run_data,
            filter_params=filter_params,
            variant_pattern=variant_pattern,
        )

        return JSONResponse(
            content={
                "run_id": run_id,
                "limit": safe_limit,
                "variant_id": variant_id,
                "pattern_index": pattern_index,
                "filtered_case_count": filtered_meta["case_count"],
                "filtered_event_count": filtered_meta["event_count"],
                "applied_filters": filter_params,
                "activity_bottlenecks": bottleneck_summary["activity_bottlenecks"][:safe_limit],
                "transition_bottlenecks": bottleneck_summary["transition_bottlenecks"][:safe_limit],
                "activity_heatmap": bottleneck_summary["activity_heatmap"],
                "transition_heatmap": bottleneck_summary["transition_heatmap"],
            }
        )

    @app.get("/api/runs/{run_id}/transition-cases")
    def transition_case_drilldown_api(
        request: Request,
        run_id: str,
        from_activity: str,
        to_activity: str,
        limit: int = 20,
        variant_id: int | None = None,
        pattern_index: int | None = None,
    ):
        run_data = get_run_data(run_id)
        filter_params = get_effective_filter_params(
            run_data,
            get_request_filter_params(request),
        )
        safe_limit = max(0, int(limit))
        case_rows = query_transition_case_drilldown(
            run_data["prepared_parquet_path"],
            from_activity=from_activity,
            to_activity=to_activity,
            limit=safe_limit,
            filter_params=filter_params,
            filter_column_settings=run_data.get("column_settings"),
            variant_pattern=get_run_variant_pattern(
                run_data,
                variant_id=variant_id,
                pattern_index=pattern_index,
                filter_params=filter_params,
            ),
        )

        return JSONResponse(
            content={
                "run_id": run_id,
                "variant_id": variant_id,
                "pattern_index": pattern_index,
                "from_activity": from_activity,
                "to_activity": to_activity,
                "transition_key": f"{from_activity}__TO__{to_activity}",
                "transition_label": f"{from_activity} → {to_activity}",
                "limit": safe_limit,
                "returned_case_count": len(case_rows),
                "applied_filters": filter_params,
                "cases": case_rows,
            }
        )

    @app.get("/api/runs/{run_id}/activity-cases")
    def activity_case_drilldown_api(
        request: Request,
        run_id: str,
        activity: str,
        limit: int = 20,
        variant_id: int | None = None,
        pattern_index: int | None = None,
    ):
        run_data = get_run_data(run_id)
        filter_params = get_effective_filter_params(
            run_data,
            get_request_filter_params(request),
        )
        safe_limit = max(0, int(limit))
        case_rows = query_activity_case_drilldown(
            run_data["prepared_parquet_path"],
            activity=activity,
            limit=safe_limit,
            filter_params=filter_params,
            filter_column_settings=run_data.get("column_settings"),
            variant_pattern=get_run_variant_pattern(
                run_data,
                variant_id=variant_id,
                pattern_index=pattern_index,
                filter_params=filter_params,
            ),
        )

        return JSONResponse(
            content={
                "run_id": run_id,
                "variant_id": variant_id,
                "pattern_index": pattern_index,
                "activity": activity,
                "limit": safe_limit,
                "returned_case_count": len(case_rows),
                "applied_filters": filter_params,
                "cases": case_rows,
            }
        )

    @app.get("/api/runs/{run_id}/cases/{case_id:path}")
    def case_trace_api(run_id: str, case_id: str):
        run_data = get_run_data(run_id)
        normalized_case_id = str(case_id or "").strip()

        if not normalized_case_id:
            return JSONResponse(
                status_code=400,
                content={
                    "run_id": run_id,
                    "case_id": "",
                    "found": False,
                    "summary": None,
                    "events": [],
                    "error": "ケースIDが必要です。",
                },
            )

        case_trace = query_case_trace_details(
            run_data["prepared_parquet_path"],
            normalized_case_id,
        )
        return JSONResponse(
            content={
                "run_id": run_id,
                **case_trace,
            }
        )
