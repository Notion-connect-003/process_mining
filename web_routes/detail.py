from io import BytesIO
from pathlib import Path
from datetime import datetime
from urllib.parse import quote
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd
import duckdb
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, Response


def register_detail_routes(
    app,
    *,
    sample_file,
    default_headers,
    template_response,
    read_raw_log_dataframe,
    build_log_profile_payload,
    get_analysis_options,
    get_static_version,
    get_available_analysis_definitions,
    get_run_data,
    get_pattern_summary_row,
    query_pattern_bottleneck_details,
    resolve_request_filter_params,
    get_analysis_data,
    get_filtered_meta_for_run,
    build_analysis_payload,
    collect_analysis_detail_sections,
    build_column_settings_payload,
    get_cached_ai_summary,
    build_empty_ai_summary,
    build_ai_insights_summary,
    get_request_ollama_insights_text,
    build_excel_bytes,
    build_analysis_excel_file_name,
    build_detail_export_context,
    build_detail_export_workbook_bytes,
    build_log_diagnostic_workbook_bytes,
    resolve_analysis_display_name,
    get_filter_options_payload,
):
    @app.get("/")
    def index(request: Request):
        sample_profile_payload = build_log_profile_payload(
            raw_df=read_raw_log_dataframe(sample_file),
            source_file_name=sample_file.name,
            include_diagnostics=False,
        )

        return template_response(
            request,
            "index.html",
            {
                "analysis_options": get_analysis_options(),
                "default_headers": default_headers,
                "sample_profile_payload": sample_profile_payload,
                "sample_file_name": sample_file.name,
                "static_version": get_static_version(),
            },
        )

    @app.get("/analysis/patterns/{pattern_index}")
    def pattern_detail_page(request: Request, pattern_index: int):
        return template_response(
            request,
            "pattern_detail.html",
            {
                "pattern_index": pattern_index,
                "static_version": get_static_version(),
            },
        )

    @app.get("/analysis/{analysis_key}")
    def analysis_detail(request: Request, analysis_key):
        analysis_definitions = get_available_analysis_definitions()

        if analysis_key not in analysis_definitions:
            raise HTTPException(status_code=404, detail="分析種別が見つかりません。")

        return template_response(
            request,
            "analysis_detail.html",
            {
                "analysis_key": analysis_key,
                "analysis_name": analysis_definitions[analysis_key]["config"][
                    "analysis_name"
                ],
                "static_version": get_static_version(),
            },
        )

    @app.get("/api/runs/{run_id}/patterns/{pattern_index}")
    def pattern_detail_api(run_id: str, pattern_index: int):
        run_data = get_run_data(run_id)
        pattern_analysis, summary_row, _, pattern = get_pattern_summary_row(
            run_data,
            pattern_index,
        )

        detail = query_pattern_bottleneck_details(
            run_data["prepared_parquet_path"],
            pattern,
            filter_column_settings=run_data.get("column_settings"),
        )
        return JSONResponse(
            content={
                "run_id": run_id,
                "pattern_index": pattern_index,
                "source_file_name": run_data["source_file_name"],
                "analysis_name": pattern_analysis["analysis_name"],
                "summary_row": summary_row,
                "repeat_flag": summary_row.get("繰り返し", ""),
                "repeat_count": summary_row.get("繰り返し回数", 0),
                "repeat_rate_pct": summary_row.get("繰り返し率(%)", 0),
                "repeat_rate_band": summary_row.get("繰り返し率区分", ""),
                "review_flag": summary_row.get("確認区分", ""),
                "avg_case_duration_diff_min": summary_row.get("平均処理時間差分(分)", 0),
                "improvement_priority_score": summary_row.get("改善優先度スコア", 0),
                "overall_impact_pct": summary_row.get("全体影響度(%)", 0),
                "fastest_pattern_flag": summary_row.get("最短処理", ""),
                "simple_comment": summary_row.get("簡易コメント", ""),
                **detail,
            }
        )

    @app.get("/api/runs/{run_id}/analyses/{analysis_key}")
    def analysis_detail_api(
        request: Request,
        run_id: str,
        analysis_key: str,
        row_limit: int | None = None,
        row_offset: int = 0,
        include_dashboard: bool = True,
        include_impact: bool = True,
        include_root_cause: bool = True,
        include_insights: bool = True,
    ):
        run_data = get_run_data(run_id)
        filter_params = resolve_request_filter_params(request, run_data)
        analysis = get_analysis_data(run_data, analysis_key, filter_params=filter_params)
        filtered_meta = get_filtered_meta_for_run(
            run_data,
            filter_params=filter_params,
        )
        response_analyses = {
            analysis_key: build_analysis_payload(
                analysis,
                row_limit=row_limit,
                row_offset=row_offset,
            )
        }
        detail_sections = collect_analysis_detail_sections(
            run_data,
            analysis_key,
            analysis.get("rows"),
            filter_params,
            include_dashboard=include_dashboard,
            include_impact=include_impact,
            include_root_cause=include_root_cause,
            include_insights=include_insights,
        )

        return JSONResponse(
            content={
                "run_id": run_id,
                "source_file_name": run_data["source_file_name"],
                "selected_analysis_keys": run_data["selected_analysis_keys"],
                "case_count": filtered_meta["case_count"],
                "event_count": filtered_meta["event_count"],
                "dashboard": detail_sections["dashboard"],
                "impact": detail_sections["impact"],
                "insights": detail_sections["insights"],
                "root_cause": detail_sections["root_cause"],
                "deferred_sections": detail_sections["deferred_sections"],
                "applied_filters": filter_params,
                "column_settings": build_column_settings_payload(
                    run_data.get("column_settings")
                ),
                "analyses": response_analyses,
            }
        )

    @app.get("/api/runs/{run_id}/ai-insights/{analysis_key}")
    def ai_insights_state_api(request: Request, run_id: str, analysis_key: str):
        run_data = get_run_data(run_id)
        analysis_definitions = get_available_analysis_definitions()
        normalized_analysis_key = str(analysis_key or "").strip().lower()

        if normalized_analysis_key not in analysis_definitions:
            raise HTTPException(status_code=404, detail="分析種別が見つかりません。")

        filter_params = resolve_request_filter_params(request, run_data)
        cached_summary = get_cached_ai_summary(
            run_data,
            normalized_analysis_key,
            filter_params=filter_params,
        )
        if cached_summary is not None:
            return JSONResponse(content=cached_summary)

        return JSONResponse(
            content=build_empty_ai_summary(
                normalized_analysis_key,
                analysis_definitions[normalized_analysis_key]["config"][
                    "analysis_name"
                ],
            )
        )

    @app.post("/api/runs/{run_id}/ai-insights/{analysis_key}")
    def ai_insights_generate_api(
        request: Request,
        run_id: str,
        analysis_key: str,
        force_refresh: bool = False,
    ):
        run_data = get_run_data(run_id)
        analysis_definitions = get_available_analysis_definitions()
        normalized_analysis_key = str(analysis_key or "").strip().lower()

        if normalized_analysis_key not in analysis_definitions:
            raise HTTPException(status_code=404, detail="分析種別が見つかりません。")

        filter_params = resolve_request_filter_params(request, run_data)
        payload = build_ai_insights_summary(
            run_data=run_data,
            analysis_key=normalized_analysis_key,
            filter_params=filter_params,
            force_refresh=force_refresh,
            generate_text=get_request_ollama_insights_text(),
        )

        return JSONResponse(content=payload)

    @app.get("/api/runs/{run_id}/excel-files/{analysis_key}")
    def analysis_excel_file_api(run_id: str, analysis_key: str):
        run_data = get_run_data(run_id)
        analyses = run_data["result"]["analyses"]
        analysis = analyses.get(analysis_key)

        if not analysis:
            raise HTTPException(status_code=404, detail="分析データが見つかりません。")

        excel_df = pd.DataFrame(analysis["rows"])
        excel_bytes = build_excel_bytes(excel_df, analysis["sheet_name"])
        output_file_name = build_analysis_excel_file_name(
            run_data["source_file_name"],
            analysis_key,
            analysis.get("analysis_name", ""),
        )

        return Response(
            content=excel_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(output_file_name)}",
            },
        )

    @app.get("/api/runs/{run_id}/excel-archive")
    def analysis_excel_archive_api(run_id: str):
        run_data = get_run_data(run_id)
        filter_params = run_data.get("base_filter_params") or {}
        errors = []

        archive_buffer = BytesIO()
        with ZipFile(
            archive_buffer,
            mode="w",
            compression=ZIP_DEFLATED,
        ) as archive_file:
            try:
                raw_csv_parquet_path = str(run_data.get("raw_csv_parquet_path") or "").strip()
                profile_payload = run_data.get("log_diagnostic_profile_payload")
                if raw_csv_parquet_path and profile_payload:
                    with duckdb.connect() as connection:
                        raw_df = connection.execute(
                            "SELECT * FROM read_parquet(?)",
                            [raw_csv_parquet_path],
                        ).df()
                    diagnostic_excel_bytes = build_log_diagnostic_workbook_bytes(
                        profile_payload=profile_payload,
                        raw_df=raw_df,
                        sample_row_limit=3000,
                    )
                    archive_file.writestr("ログ診断.xlsx", diagnostic_excel_bytes)
                else:
                    errors.append("ログ診断: raw CSVデータが保存されていません（分析実行を再度行ってください）")
            except Exception as exc:
                errors.append(f"ログ診断: {exc}")

            for analysis_key, display_name in (
                ("frequency", "頻度分析"),
                ("transition", "前後処理分析"),
                ("pattern", "処理順パターン分析"),
            ):
                if analysis_key not in (run_data.get("result", {}).get("analyses", {}) or {}):
                    errors.append(f"{display_name}: 分析が実行されていません")
                    continue
                try:
                    context = build_detail_export_context(
                        run_data,
                        analysis_key,
                        filter_params,
                        generate_text=lambda *_args, **_kwargs: "",
                    )
                    excel_bytes = build_detail_export_workbook_bytes(
                        run_data=run_data,
                        analysis_key=analysis_key,
                        context=context,
                        filter_params=filter_params,
                        pattern_display_limit="10",
                    )
                    archive_file.writestr(f"{display_name}.xlsx", excel_bytes)
                except Exception as exc:
                    errors.append(f"{display_name}: {exc}")

            if errors:
                error_text = "以下のExcel生成でエラーが発生しました:\n" + "\n".join(errors)
                archive_file.writestr("エラーログ.txt", error_text.encode("utf-8"))

        archive_file_name = (
            f"{Path(run_data['source_file_name']).stem}_{datetime.now().strftime('%Y%m%d')}_全分析レポート.zip"
        )

        return Response(
            content=archive_buffer.getvalue(),
            media_type="application/zip",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(archive_file_name)}",
            },
        )

    @app.get("/api/runs/{run_id}/detail-excel")
    @app.get("/api/runs/{run_id}/report-excel")
    def detail_excel_export_api(
        request: Request,
        run_id: str,
        analysis_key: str,
        pattern_display_limit: str = "10",
        variant_id: int | None = None,
        selected_activity: str = "",
        selected_transition_key: str = "",
        case_id: str = "",
        drilldown_limit: int = 20,
    ):
        run_data = get_run_data(run_id)
        filter_params = resolve_request_filter_params(request, run_data)
        context = build_detail_export_context(
            run_data,
            analysis_key,
            filter_params,
            selected_transition_key=selected_transition_key,
            variant_id=variant_id,
        )
        excel_bytes = build_detail_export_workbook_bytes(
            run_data=run_data,
            analysis_key=analysis_key,
            context=context,
            filter_params=filter_params,
            pattern_display_limit=pattern_display_limit,
            variant_id=variant_id,
            selected_activity=selected_activity,
            case_id=case_id,
            drilldown_limit=drilldown_limit,
        )
        analysis_name = resolve_analysis_display_name(analysis_key)
        output_file_name = build_analysis_excel_file_name(
            run_data["source_file_name"],
            analysis_key,
            analysis_name,
            suffix="レポート",
        )

        return Response(
            content=excel_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(output_file_name)}",
            },
        )

    @app.get("/api/runs/{run_id}/filter-options")
    def filter_options_api(run_id: str):
        run_data = get_run_data(run_id)

        return JSONResponse(
            content={
                "run_id": run_id,
                "options": get_filter_options_payload(run_data),
                "applied_filters": run_data.get("base_filter_params"),
                "column_settings": build_column_settings_payload(
                    run_data.get("column_settings")
                ),
            }
        )

