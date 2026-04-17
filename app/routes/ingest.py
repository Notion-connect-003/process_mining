import json
from pathlib import Path
from urllib.parse import quote

import duckdb
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from app.services.llm_helpers import stream_ollama_response


def register_ingest_routes(
    app,
    *,
    sample_file,
    default_headers,
    resolve_profile_file_source,
    read_raw_log_dataframe,
    build_log_profile_payload,
    get_form_filter_column_settings,
    resolve_log_diagnostic_sample_row_limit,
    build_log_diagnostic_workbook_bytes,
    build_analysis_excel_file_name,
    parse_analyze_form,
    resolve_analyze_file_source,
    prepare_analysis_input_data,
    execute_analysis_pipeline,
    run_storage_dir,
    max_stored_runs,
    resolve_required_column_name,
    validate_selected_columns,
    build_preview_response,
    get_run_data,
    build_bottleneck_prompt,
    httpx_module,
):
    @app.post("/api/csv-headers")
    async def csv_headers(request: Request):
        form = await request.form()
        raw_case_id_column = form.get("case_id_column")
        raw_activity_column = form.get("activity_column")
        raw_timestamp_column = form.get("timestamp_column")
        filter_column_settings = get_form_filter_column_settings(form)
        file_source, source_file_name = resolve_profile_file_source(form)

        try:
            raw_df = read_raw_log_dataframe(file_source)
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        except Exception as exc:
            return JSONResponse(
                status_code=400,
                content={
                    "error": "CSVヘッダーを読み取れませんでした。ファイルの文字コードとヘッダー行を確認してください。",
                    "detail": str(exc),
                },
            )

        return JSONResponse(
            content=build_log_profile_payload(
                raw_df=raw_df,
                source_file_name=source_file_name,
                case_id_column=str(raw_case_id_column or "").strip(),
                activity_column=str(raw_activity_column or "").strip(),
                timestamp_column=str(raw_timestamp_column or "").strip(),
                filter_column_settings=filter_column_settings,
                include_diagnostics=False,
            )
        )

    @app.post("/api/log-diagnostics")
    async def log_diagnostics(request: Request):
        form = await request.form()
        raw_case_id_column = form.get("case_id_column")
        raw_activity_column = form.get("activity_column")
        raw_timestamp_column = form.get("timestamp_column")
        filter_column_settings = get_form_filter_column_settings(form)
        file_source, source_file_name = resolve_profile_file_source(form)

        try:
            raw_df = read_raw_log_dataframe(file_source)
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        except Exception as exc:
            return JSONResponse(
                status_code=400,
                content={
                    "error": "ログ診断を読み取れませんでした。ファイルの文字コードとヘッダー行を確認してください。",
                    "detail": str(exc),
                },
            )

        return JSONResponse(
            content=build_log_profile_payload(
                raw_df=raw_df,
                source_file_name=source_file_name,
                case_id_column=str(raw_case_id_column or "").strip(),
                activity_column=str(raw_activity_column or "").strip(),
                timestamp_column=str(raw_timestamp_column or "").strip(),
                filter_column_settings=filter_column_settings,
                include_diagnostics=True,
            )
        )

    @app.post("/api/log-diagnostics-excel")
    async def log_diagnostics_excel(request: Request):
        form = await request.form()
        raw_case_id_column = form.get("case_id_column")
        raw_activity_column = form.get("activity_column")
        raw_timestamp_column = form.get("timestamp_column")
        filter_column_settings = get_form_filter_column_settings(form)
        sample_row_limit = resolve_log_diagnostic_sample_row_limit(
            form.get("sample_row_limit")
        )
        file_source, source_file_name = resolve_profile_file_source(form)

        try:
            raw_df = read_raw_log_dataframe(file_source)
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        except Exception as exc:
            return JSONResponse(
                status_code=400,
                content={
                    "error": "ログ診断を読み取れませんでした。ファイルの文字コードとヘッダー行を確認してください。",
                    "detail": str(exc),
                },
            )

        profile_payload = build_log_profile_payload(
            raw_df=raw_df,
            source_file_name=source_file_name,
            case_id_column=str(raw_case_id_column or "").strip(),
            activity_column=str(raw_activity_column or "").strip(),
            timestamp_column=str(raw_timestamp_column or "").strip(),
            filter_column_settings=filter_column_settings,
            include_diagnostics=True,
        )
        excel_bytes = build_log_diagnostic_workbook_bytes(
            profile_payload=profile_payload,
            raw_df=raw_df,
            sample_row_limit=sample_row_limit,
        )
        output_file_name = build_analysis_excel_file_name(
            source_file_name,
            "log_diagnostics",
            "ログ診断",
        )

        return Response(
            content=excel_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(output_file_name)}",
            },
        )

    @app.post("/api/analyze")
    async def analyze(request: Request):
        form = await request.form()
        analyze_request = parse_analyze_form(form, default_headers)
        file_source, source_file_name = resolve_analyze_file_source(
            analyze_request["uploaded_file"],
            sample_file,
        )

        try:
            raw_df = read_raw_log_dataframe(file_source)
            if hasattr(file_source, "seek"):
                file_source.seek(0)
            prepared_input = prepare_analysis_input_data(
                file_source=file_source,
                case_id_column=analyze_request["case_id_column"],
                activity_column=analyze_request["activity_column"],
                timestamp_column=analyze_request["timestamp_column"],
                filter_column_settings=analyze_request["filter_column_settings"],
                base_filter_params=analyze_request["base_filter_params"],
                read_raw_log_dataframe=read_raw_log_dataframe,
                resolve_required_column_name=resolve_required_column_name,
                validate_selected_columns=validate_selected_columns,
            )
            run_id, result = execute_analysis_pipeline(
                source_file_name=source_file_name,
                selected_analysis_keys=analyze_request["selected_analysis_keys"],
                prepared_df=prepared_input["prepared_df"],
                filtered_prepared_df=prepared_input["filtered_prepared_df"],
                group_columns=prepared_input["group_columns"],
                case_id_column=prepared_input["case_id_column"],
                activity_column=prepared_input["activity_column"],
                timestamp_column=prepared_input["timestamp_column"],
                filter_column_settings=analyze_request["filter_column_settings"],
                base_filter_params=analyze_request["base_filter_params"],
                run_storage_dir=run_storage_dir,
                max_stored_runs=max_stored_runs,
            )
            run_data = get_run_data(run_id)
            run_storage_path = Path(str(run_data["prepared_parquet_path"])).resolve().parent

            raw_csv_parquet_path = run_storage_path / "raw_upload.parquet"
            with duckdb.connect() as connection:
                connection.register("raw_upload_df", raw_df)
                connection.execute(
                    "COPY raw_upload_df TO ? (FORMAT PARQUET)",
                    [str(raw_csv_parquet_path)],
                )
            run_data["raw_csv_parquet_path"] = str(raw_csv_parquet_path)

            profile_payload = build_log_profile_payload(
                raw_df=raw_df,
                source_file_name=source_file_name,
                case_id_column=str(prepared_input["case_id_column"] or "").strip(),
                activity_column=str(prepared_input["activity_column"] or "").strip(),
                timestamp_column=str(prepared_input["timestamp_column"] or "").strip(),
                filter_column_settings=analyze_request["filter_column_settings"],
                include_diagnostics=True,
            )
            run_data["log_diagnostic_profile_payload"] = profile_payload
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={
                    "error": "分析に失敗しました。",
                    "detail": str(exc),
                },
            )

        return JSONResponse(
            content=build_preview_response(
                run_id=run_id,
                source_file_name=source_file_name,
                selected_analysis_keys=analyze_request["selected_analysis_keys"],
                result=result,
                run_data=run_data,
            )
        )

    @app.post("/api/ai-insights")
    async def ai_insights(request: Request):
        data = await request.json()
        prompt = build_bottleneck_prompt(data)

        async def generate():
            async for chunk in stream_ollama_response(
                prompt,
                httpx_module=httpx_module,
            ):
                yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"

        return StreamingResponse(generate(), media_type="text/event-stream")
