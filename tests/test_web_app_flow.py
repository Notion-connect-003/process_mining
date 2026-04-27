from tests.web_app_test_base import *

class WebAppFlowTestCase(WebAppTestCaseBase):
    def test_pattern_report_excel_caps_detail_sheets_at_twenty(self):
        csv_lines = ["case_id,activity,start_time"]
        for case_index in range(1, 26):
            csv_lines.append(f"C{case_index:03d},Start_{case_index},2024-01-01 09:00:00")
            csv_lines.append(f"C{case_index:03d},End_{case_index},2024-01-01 10:00:00")

        run_id = self.analyze_uploaded_csv(
            "\n".join(csv_lines),
            analysis_keys=["pattern"],
        )

        with mock.patch(
            "app.main.request_ollama_insights_text",
            return_value="pattern ai",
        ):
            response = self.client.get(
                f"/api/runs/{run_id}/report-excel?analysis_key=pattern&pattern_display_limit=50"
            )

        self.assertEqual(200, response.status_code)
        workbook = load_workbook(BytesIO(response.content))
        detail_sheet_names = [
            sheet_name
            for sheet_name in workbook.sheetnames
            if sheet_name.startswith("\u30d1\u30bf\u30fc\u30f3") and sheet_name.endswith("詳細")
        ]
        self.assertEqual(20, len(detail_sheet_names))

    def test_pattern_detail_api_handles_variant_code_collisions(self):
        run_id = self.analyze_uploaded_csv(
            self.build_variant_collision_csv(),
            analysis_keys=["pattern"],
        )

        response = self.client.get(f"/api/runs/{run_id}/patterns/1")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertIn("Reminder", payload["pattern"])
        self.assertEqual(1, payload["case_count"])
        self.assertTrue(payload["step_metrics"])

    def test_pattern_report_excel_handles_variant_code_collisions(self):
        run_id = self.analyze_uploaded_csv(
            self.build_variant_collision_csv(),
            analysis_keys=["pattern"],
        )

        with mock.patch(
            "app.main.request_ollama_insights_text",
            return_value="pattern ai",
        ):
            response = self.client.get(
                f"/api/runs/{run_id}/report-excel?analysis_key=pattern&pattern_display_limit=10"
            )

        self.assertEqual(200, response.status_code)
        workbook = load_workbook(BytesIO(response.content))
        self.assertIn("\u30d1\u30bf\u30fc\u30f301\u8a73\u7d30", workbook.sheetnames)
        self.assertIn("\u30d1\u30bf\u30fc\u30f302\u8a73\u7d30", workbook.sheetnames)

    def test_pattern_flow_api_accepts_exact_pattern_count(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["frequency", "pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        flow_response = self.client.get(f"/api/runs/{run_id}/pattern-flow?pattern_count=1")

        self.assertEqual(200, flow_response.status_code)
        payload = flow_response.json()
        self.assertEqual(1, payload["pattern_window"]["requested_count"])
        self.assertEqual(1, payload["pattern_window"]["used_pattern_count"])
        self.assertGreaterEqual(len(payload["flow_data"]["nodes"]), 1)
        self.assertTrue(payload["flow_data"]["edges"])
        self.assertIn("avg_duration_text", payload["flow_data"]["edges"][0])
        self.assertGreater(payload["flow_data"]["edges"][0]["avg_duration_sec"], 0)

    def test_pattern_flow_api_reuses_cached_snapshot(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["frequency", "pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        flow_path = (
            f"/api/runs/{run_id}/pattern-flow"
            "?pattern_percent=20&activity_percent=30&connection_percent=20"
        )

        with mock.patch(
            "app.main.create_pattern_flow_snapshot",
            wraps=app_main.create_pattern_flow_snapshot,
        ) as snapshot_mock:
            first_response = self.client.get(flow_path)
            second_response = self.client.get(flow_path)

        self.assertEqual(200, first_response.status_code)
        self.assertEqual(200, second_response.status_code)
        self.assertEqual(first_response.json(), second_response.json())
        self.assertEqual(1, snapshot_mock.call_count)

    def test_variant_list_api_returns_top_variants(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        variant_response = self.client.get(f"/api/runs/{run_id}/variants?limit=2")

        self.assertEqual(200, variant_response.status_code)
        payload = variant_response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertEqual(2, len(payload["variants"]))
        self.assertEqual(1, payload["variants"][0]["variant_id"])
        self.assertTrue(payload["variants"][0]["activities"])
        self.assertEqual(len(payload["variants"][0]["activities"]), payload["variants"][0]["activity_count"])
        self.assertEqual(0, payload["variants"][0]["pattern_index"])
        self.assertIn("repeat_flag", payload["variants"][0])
        self.assertGreater(payload["variants"][0]["avg_case_duration_sec"], 0)
        self.assertEqual(2, payload["coverage"]["displayed_variant_count"])
        self.assertGreater(payload["coverage"]["covered_case_count"], 0)
        self.assertLessEqual(payload["coverage"]["covered_case_count"], payload["filtered_case_count"])
        self.assertGreater(payload["coverage"]["ratio"], 0)
        self.assertLessEqual(payload["coverage"]["ratio"], 1)

    def test_variant_list_api_limit_zero_returns_all_variants(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        variant_response = self.client.get(f"/api/runs/{run_id}/variants?limit=0")

        self.assertEqual(200, variant_response.status_code)
        payload = variant_response.json()
        self.assertGreaterEqual(len(payload["variants"]), 3)
        self.assertEqual(len(payload["variants"]), payload["coverage"]["displayed_variant_count"])

    def test_variant_list_api_reuses_existing_pattern_analysis_when_unfiltered(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time",
                    "C001,受付,2024-01-01 09:00:00",
                    "C001,確認,2024-01-01 10:00:00",
                    "C001,完了,2024-01-01 11:00:00",
                    "C002,受付,2024-01-02 09:00:00",
                    "C002,確認,2024-01-02 10:00:00",
                    "C002,完了,2024-01-02 11:00:00",
                ]
            ),
            analysis_keys=["pattern"],
        )

        with mock.patch("app.main.create_variant_summary", side_effect=AssertionError("create_variant_summary should not run")):
            variant_response = self.client.get(f"/api/runs/{run_id}/variants?limit=0")

        self.assertEqual(200, variant_response.status_code)
        payload = variant_response.json()
        self.assertEqual(1, len(payload["variants"]))
        self.assertEqual(["受付", "確認", "完了"], payload["variants"][0]["activities"])

    def test_pattern_flow_api_supports_variant_filter(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["frequency", "pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        flow_response = self.client.get(f"/api/runs/{run_id}/pattern-flow?variant_id=1")

        self.assertEqual(200, flow_response.status_code)
        payload = flow_response.json()
        self.assertEqual(1, payload["selected_variant"]["variant_id"])
        self.assertEqual(0, payload["selected_variant"]["pattern_index"])
        self.assertEqual(1, payload["pattern_window"]["used_pattern_count"])
        self.assertTrue(payload["flow_data"]["nodes"])
        self.assertTrue(all("avg_duration_text" in edge for edge in payload["flow_data"]["edges"]))

    def test_pattern_flow_api_uses_lightweight_mode_for_large_datasets(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time",
                    "C001,受付,2024-01-01 09:00:00",
                    "C001,確認,2024-01-01 10:00:00",
                    "C001,完了,2024-01-01 11:00:00",
                    "C002,受付,2024-01-02 09:00:00",
                    "C002,確認,2024-01-02 10:00:00",
                    "C002,完了,2024-01-02 11:00:00",
                ]
            ),
            analysis_keys=["frequency", "pattern"],
        )

        with mock.patch.object(app_main, "LARGE_DATASET_FLOW_FAST_PATH_THRESHOLD", 1):
            with mock.patch("app.main.create_pattern_flow_snapshot", wraps=app_main.create_pattern_flow_snapshot) as wrapped_snapshot:
                flow_response = self.client.get(f"/api/runs/{run_id}/pattern-flow")

        self.assertEqual(200, flow_response.status_code)
        payload = flow_response.json()
        self.assertTrue(payload["is_large_dataset_optimized"])
        self.assertIsNone(wrapped_snapshot.call_args.kwargs["prepared_df"])

    def test_bottleneck_list_api_returns_ranked_activity_and_transition_rows(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        response = self.client.get(f"/api/runs/{run_id}/bottlenecks?limit=2")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertEqual(2, payload["limit"])
        self.assertEqual(2, len(payload["activity_bottlenecks"]))
        self.assertEqual(2, len(payload["transition_bottlenecks"]))
        self.assertEqual("\u78ba\u8a8d", payload["activity_bottlenecks"][0]["activity"])
        self.assertEqual(577.5, payload["activity_bottlenecks"][0]["avg_duration_sec"])
        self.assertEqual(0.16, payload["activity_bottlenecks"][0]["avg_duration_hours"])
        self.assertEqual("heat-5", payload["activity_heatmap"]["\u78ba\u8a8d"]["heat_class"])
        self.assertEqual("\u78ba\u8a8d", payload["transition_bottlenecks"][0]["from_activity"])
        self.assertEqual("\u5dee\u623b\u3057", payload["transition_bottlenecks"][0]["to_activity"])
        self.assertEqual("\u78ba\u8a8d__TO__\u5dee\u623b\u3057", payload["transition_bottlenecks"][0]["transition_key"])
        self.assertEqual(0.2, payload["transition_bottlenecks"][0]["avg_duration_hours"])
        self.assertEqual("heat-5", payload["transition_heatmap"]["\u78ba\u8a8d__TO__\u5dee\u623b\u3057"]["heat_class"])

    def test_transition_case_drilldown_api_returns_slowest_cases(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        response = self.client.get(
            f"/api/runs/{run_id}/transition-cases"
            "?from_activity=\u78ba\u8a8d&to_activity=\u5dee\u623b\u3057&limit=5"
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertEqual("\u78ba\u8a8d", payload["from_activity"])
        self.assertEqual("\u5dee\u623b\u3057", payload["to_activity"])
        self.assertEqual("\u78ba\u8a8d__TO__\u5dee\u623b\u3057", payload["transition_key"])
        self.assertEqual("\u78ba\u8a8d \u2192 \u5dee\u623b\u3057", payload["transition_label"])
        self.assertEqual(2, payload["returned_case_count"])
        self.assertEqual(2, len(payload["cases"]))
        self.assertEqual("C002", payload["cases"][0]["case_id"])
        self.assertEqual(1020.0, payload["cases"][0]["duration_sec"])
        self.assertEqual("17m 0s", payload["cases"][0]["duration_text"])

    def test_activity_case_drilldown_api_returns_slowest_cases(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        response = self.client.get(
            f"/api/runs/{run_id}/activity-cases"
            "?activity=\u78ba\u8a8d&limit=5"
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertEqual("\u78ba\u8a8d", payload["activity"])
        self.assertEqual(5, payload["returned_case_count"])
        self.assertEqual(5, len(payload["cases"]))
        self.assertEqual("C002", payload["cases"][0]["case_id"])
        self.assertEqual(1020.0, payload["cases"][0]["duration_sec"])
        self.assertEqual("\u5dee\u623b\u3057", payload["cases"][0]["next_activity"])

    def test_case_trace_api_returns_case_timeline(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        response = self.client.get(f"/api/runs/{run_id}/cases/C001")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertTrue(payload["found"])
        self.assertEqual("C001", payload["case_id"])
        self.assertEqual(4, payload["summary"]["event_count"])
        self.assertEqual(900.0, payload["summary"]["total_duration_sec"])
        self.assertEqual("15m 0s", payload["summary"]["total_duration_text"])
        self.assertEqual(4, len(payload["events"]))
        self.assertEqual(1, payload["events"][0]["sequence_no"])
        self.assertEqual(120.0, payload["events"][0]["wait_to_next_sec"])
        self.assertIsNone(payload["events"][-1]["wait_to_next_sec"])

    def test_case_trace_api_returns_not_found_payload(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        response = self.client.get(f"/api/runs/{run_id}/cases/C999")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertFalse(payload["found"])
        self.assertEqual("C999", payload["case_id"])
        self.assertIsNone(payload["summary"])
        self.assertEqual([], payload["events"])

    def test_analyze_api_persists_prepared_parquet_file(self):
        run_id = self.analyze_uploaded_csv(
            self.build_duckdb_validation_csv(),
            analysis_keys=["frequency"],
        )

        run_data = app_main.get_run_data(run_id)
        self.assertTrue(app_main.has_parquet_backing(run_data))
        self.assertTrue(app_main.Path(run_data["prepared_parquet_path"]).exists())
        self.assertTrue(
            str(run_data["prepared_parquet_path"]).endswith(f"{run_id}\\prepared.parquet")
        )

    def test_analyze_api_persists_log_diagnostic_inputs_for_excel_archive(self):
        run_id = self.analyze_uploaded_csv(
            self.build_duckdb_validation_csv(),
            analysis_keys=["frequency"],
        )

        run_data = app_main.get_run_data(run_id)
        self.assertTrue(app_main.Path(run_data["raw_csv_parquet_path"]).exists())
        self.assertTrue(
            str(run_data["raw_csv_parquet_path"]).endswith(f"{run_id}\\raw_upload.parquet")
        )
        self.assertIn("log_diagnostic_profile_payload", run_data)
        self.assertEqual(
            run_data["source_file_name"],
            run_data["log_diagnostic_profile_payload"]["source_file_name"],
        )
        self.assertIsNotNone(run_data["log_diagnostic_profile_payload"]["diagnostics"])

    def test_bottleneck_list_api_supports_uploaded_csv_with_duckdb_backing(self):
        run_id = self.analyze_uploaded_csv(
            self.build_duckdb_validation_csv(),
            analysis_keys=["pattern"],
        )

        response = self.client.get(f"/api/runs/{run_id}/bottlenecks?limit=2")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(2, len(payload["activity_bottlenecks"]))
        self.assertEqual("Review", payload["activity_bottlenecks"][0]["activity"])
        self.assertEqual(920.0, payload["activity_bottlenecks"][0]["avg_duration_sec"])
        self.assertEqual("Review", payload["transition_bottlenecks"][0]["from_activity"])
        self.assertEqual("Rework", payload["transition_bottlenecks"][0]["to_activity"])
        self.assertEqual(
            "Review__TO__Rework",
            payload["transition_bottlenecks"][0]["transition_key"],
        )

    def test_transition_and_activity_drilldown_support_uploaded_csv_with_duckdb_backing(self):
        run_id = self.analyze_uploaded_csv(
            self.build_duckdb_validation_csv(),
            analysis_keys=["pattern"],
        )

        transition_response = self.client.get(
            f"/api/runs/{run_id}/transition-cases?from_activity=Review&to_activity=Rework&limit=5"
        )
        self.assertEqual(200, transition_response.status_code)
        transition_payload = transition_response.json()
        self.assertEqual(2, transition_payload["returned_case_count"])
        self.assertEqual("C002", transition_payload["cases"][0]["case_id"])
        self.assertEqual(1020.0, transition_payload["cases"][0]["duration_sec"])

        activity_response = self.client.get(
            f"/api/runs/{run_id}/activity-cases?activity=Review&limit=5"
        )
        self.assertEqual(200, activity_response.status_code)
        activity_payload = activity_response.json()
        self.assertEqual(3, activity_payload["returned_case_count"])
        self.assertEqual("C002", activity_payload["cases"][0]["case_id"])
        self.assertEqual("Rework", activity_payload["cases"][0]["next_activity"])

    def test_case_trace_api_supports_uploaded_csv_with_duckdb_backing(self):
        run_id = self.analyze_uploaded_csv(
            self.build_duckdb_validation_csv(),
            analysis_keys=["pattern"],
        )

        response = self.client.get(f"/api/runs/{run_id}/cases/C001")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertTrue(payload["found"])
        self.assertEqual("C001", payload["case_id"])
        self.assertEqual(4, payload["summary"]["event_count"])
        self.assertEqual(1200.0, payload["summary"]["total_duration_sec"])
        self.assertEqual("20m 0s", payload["summary"]["total_duration_text"])
        self.assertEqual(4, len(payload["events"]))
        self.assertEqual("Review", payload["events"][1]["activity"])

    def test_pattern_flow_api_supports_uploaded_csv_with_duckdb_backing(self):
        run_id = self.analyze_uploaded_csv(
            self.build_duckdb_validation_csv(),
            analysis_keys=["frequency", "pattern"],
        )

        response = self.client.get(
            f"/api/runs/{run_id}/pattern-flow?pattern_count=2&activity_percent=100&connection_percent=100"
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(2, payload["pattern_window"]["requested_count"])
        self.assertEqual(2, payload["pattern_window"]["used_pattern_count"])
        self.assertTrue(payload["flow_data"]["nodes"])
        self.assertTrue(payload["flow_data"]["edges"])
        self.assertTrue(
            any(
                edge["source"] == "Start" and edge["target"] == "Review"
                for edge in payload["flow_data"]["edges"]
            )
        )
        self.assertTrue(all("avg_duration_text" in edge for edge in payload["flow_data"]["edges"]))

    def test_variant_pattern_flow_api_supports_uploaded_csv_with_duckdb_backing(self):
        run_id = self.analyze_uploaded_csv(
            self.build_duckdb_validation_csv(),
            analysis_keys=["frequency", "pattern"],
        )

        response = self.client.get(
            f"/api/runs/{run_id}/pattern-flow?variant_id=1&activity_percent=100&connection_percent=100"
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(1, payload["selected_variant"]["variant_id"])
        self.assertEqual(1, payload["pattern_window"]["used_pattern_count"])
        self.assertTrue(payload["flow_data"]["nodes"])
        self.assertTrue(payload["flow_data"]["edges"])
        self.assertTrue(all("avg_duration_text" in edge for edge in payload["flow_data"]["edges"]))

    def test_large_parquet_backed_run_can_release_prepared_df_and_still_render_pattern_flow(self):
        with mock.patch.object(app_main, "LARGE_DATASET_FLOW_FAST_PATH_THRESHOLD", 999999):
            run_id = self.analyze_uploaded_csv(
                self.build_duckdb_validation_csv(),
                analysis_keys=["frequency", "pattern"],
            )
            run_data = app_main.get_run_data(run_id)
            self.assertIsNone(run_data["prepared_df"])

            filter_options_response = self.client.get(f"/api/runs/{run_id}/filter-options")
            self.assertEqual(200, filter_options_response.status_code)
            self.assertEqual(run_data["filter_options"], filter_options_response.json()["options"])

            flow_response = self.client.get(
                f"/api/runs/{run_id}/pattern-flow?pattern_count=2&activity_percent=100&connection_percent=100"
            )

        self.assertEqual(200, flow_response.status_code)
        payload = flow_response.json()
        self.assertFalse(payload.get("is_large_dataset_optimized", False))
        self.assertTrue(payload["flow_data"]["edges"])
        self.assertTrue(all("avg_duration_text" in edge for edge in payload["flow_data"]["edges"]))

    def test_filter_options_api_returns_available_values(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time,group_a,group_b,group_c",
                    "C001,Submit,2024-01-01 09:00:00,Sales,Web,A",
                    "C001,Approve,2024-01-02 09:00:00,Sales,Web,A",
                    "C002,Submit,2024-01-01 10:00:00,HR,Mail,B",
                    "C002,Reject,2024-01-03 10:00:00,HR,Mail,B",
                    "C003,Submit,2024-01-04 08:00:00,Sales,API,A",
                    "C003,Approve,2024-01-05 08:00:00,Sales,API,A",
                ]
            ),
            extra_data={
                "filter_column_1": "group_a",
                "filter_column_2": "group_b",
                "filter_column_3": "group_c",
            },
        )

        response = self.client.get(f"/api/runs/{run_id}/filter-options")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("group_a", payload["column_settings"]["filters"][0]["column_name"])
        self.assertEqual(["HR", "Sales"], payload["options"]["filters"][0]["options"])
        self.assertEqual(["API", "Mail", "Web"], payload["options"]["filters"][1]["options"])
        self.assertEqual(["A", "B"], payload["options"]["filters"][2]["options"])

