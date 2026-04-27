from tests.web_app_test_base import *

class WebAppDiagnosticsTestCase(WebAppTestCaseBase):
    def test_analysis_detail_api_supports_filters(self):
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

        response = self.client.get(
            f"/api/runs/{run_id}/analyses/frequency?filter_value_1=Sales&date_from=2024-01-02"
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(2, payload["case_count"])
        self.assertEqual(3, payload["event_count"])
        self.assertEqual(2, payload["dashboard"]["total_cases"])
        self.assertEqual(3, payload["dashboard"]["total_records"])
        self.assertTrue(payload["impact"]["rows"])
        self.assertEqual(3, payload["root_cause"]["configured_group_count"])
        self.assertTrue(payload["root_cause"]["groups"][0]["rows"])
        self.assertEqual("Sales", payload["applied_filters"]["filter_value_1"])
        self.assertEqual("2024-01-02", payload["applied_filters"]["date_from"])

    def test_analysis_detail_api_supports_activity_filters(self):
        # Activity filter is case-level: keeps all events of cases that contain (or don't contain)
        # the target activity. C004 is a Sales case with no Submit → survives exclude-Submit filter.
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time,group_a,group_b,group_c",
                    "C001,Submit,2024-01-01 09:00:00,Sales,Web,A",
                    "C001,Approve,2024-01-03 09:00:00,Sales,Web,A",
                    "C002,Submit,2024-01-02 10:00:00,HR,Mail,B",
                    "C002,Reject,2024-01-04 10:00:00,HR,Mail,B",
                    "C003,Submit,2024-01-04 08:00:00,Sales,API,A",
                    "C003,Approve,2024-01-05 08:00:00,Sales,API,A",
                    "C004,Approve,2024-01-06 08:00:00,Sales,API,A",
                ]
            ),
            extra_data={
                "filter_column_1": "group_a",
                "filter_column_2": "group_b",
                "filter_column_3": "group_c",
            },
        )

        response = self.client.get(
            f"/api/runs/{run_id}/analyses/frequency?filter_value_1=Sales&activity_mode=exclude&activity_values=Submit"
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        # Sales cases that never pass through Submit: only C004 (1 case, 1 event)
        self.assertEqual(1, payload["case_count"])
        self.assertEqual(1, payload["event_count"])
        self.assertEqual(1, payload["dashboard"]["total_cases"])
        self.assertEqual(1, payload["dashboard"]["total_records"])
        self.assertEqual("exclude", payload["applied_filters"]["activity_mode"])
        self.assertEqual("Submit", payload["applied_filters"]["activity_values"])

    def test_bottleneck_and_variant_api_support_filters(self):
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

        variant_response = self.client.get(f"/api/runs/{run_id}/variants?filter_value_2=Web")
        bottleneck_response = self.client.get(f"/api/runs/{run_id}/bottlenecks?filter_value_3=A")

        self.assertEqual(200, variant_response.status_code)
        self.assertEqual(200, bottleneck_response.status_code)

        variant_payload = variant_response.json()
        bottleneck_payload = bottleneck_response.json()

        self.assertEqual(1, variant_payload["filtered_case_count"])
        self.assertEqual(2, variant_payload["filtered_event_count"])
        self.assertEqual("Web", variant_payload["applied_filters"]["filter_value_2"])
        self.assertEqual(2, bottleneck_payload["filtered_case_count"])
        self.assertEqual(4, bottleneck_payload["filtered_event_count"])
        self.assertEqual("A", bottleneck_payload["applied_filters"]["filter_value_3"])

    def test_pattern_detail_api_uses_pattern_index_not_display_text(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        run_data = app_main.get_run_data(run_id)
        pattern_rows = run_data["result"]["analyses"]["pattern"]["rows"]

        if pattern_rows:
            first_row = pattern_rows[0]
            for key in list(first_row.keys()):
                if "pattern" in str(key).lower() or "\u30d1\u30bf\u30fc\u30f3" in str(key) or "\u51e6\u7406\u9806" in str(key):
                    first_row[key] = ""

        response = self.client.get(f"/api/runs/{run_id}/patterns/0")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(run_id, payload["run_id"])
        self.assertEqual(0, payload["pattern_index"])
        self.assertTrue(payload["pattern"])
        self.assertIn("repeat_flag", payload)
        self.assertIn("repeat_count", payload)
        self.assertIn("repeat_rate_pct", payload)
        self.assertIn("repeat_rate_band", payload)
        self.assertIn("review_flag", payload)
        self.assertIn("avg_case_duration_diff_min", payload)
        self.assertIn("improvement_priority_score", payload)
        self.assertIn("overall_impact_pct", payload)
        self.assertIn("fastest_pattern_flag", payload)
        self.assertIn("simple_comment", payload)
        self.assertEqual(payload["summary_row"].get("繰り返し", ""), payload["repeat_flag"])
        self.assertEqual(payload["summary_row"].get("繰り返し回数", 0), payload["repeat_count"])
        self.assertEqual(payload["summary_row"].get("繰り返し率(%)", 0), payload["repeat_rate_pct"])
        self.assertEqual(payload["summary_row"].get("繰り返し率区分", ""), payload["repeat_rate_band"])
        self.assertEqual(payload["summary_row"].get("確認区分", ""), payload["review_flag"])
        self.assertEqual(payload["summary_row"].get("平均処理時間差分(分)", 0), payload["avg_case_duration_diff_min"])
        self.assertEqual(payload["summary_row"].get("改善優先度スコア", 0), payload["improvement_priority_score"])
        self.assertEqual(payload["summary_row"].get("全体影響度(%)", 0), payload["overall_impact_pct"])
        self.assertEqual(payload["summary_row"].get("最短処理", ""), payload["fastest_pattern_flag"])
        self.assertEqual(payload["summary_row"].get("簡易コメント", ""), payload["simple_comment"])

    def test_pattern_detail_api_supports_parquet_backed_uploaded_csv_without_variant_column(self):
        run_id = self.analyze_uploaded_csv(
            self.build_duckdb_validation_csv(),
            analysis_keys=["pattern"],
        )

        run_data = app_main.get_run_data(run_id)
        self.assertIsNone(run_data["prepared_df"])

        detail_response = self.client.get(f"/api/runs/{run_id}/patterns/0")
        self.assertEqual(200, detail_response.status_code)
        detail_payload = detail_response.json()
        self.assertTrue(detail_payload["pattern"])
        self.assertTrue(detail_payload["step_metrics"])

        first_transition = detail_payload["step_metrics"][0]
        transition_response = self.client.get(
            f"/api/runs/{run_id}/transition-cases"
            f"?from_activity={quote(first_transition['activity'])}"
            f"&to_activity={quote(first_transition['next_activity'])}"
            "&pattern_index=0&limit=5"
        )
        self.assertEqual(200, transition_response.status_code)
        self.assertGreaterEqual(transition_response.json()["returned_case_count"], 1)

    def test_pattern_detail_api_supports_parquet_backed_uploaded_csv_with_variant_column(self):
        run_id = self.analyze_uploaded_csv(
            self.build_duckdb_validation_csv_with_variant(),
            analysis_keys=["pattern"],
        )

        run_data = app_main.get_run_data(run_id)
        self.assertIsNone(run_data["prepared_df"])

        detail_response = self.client.get(f"/api/runs/{run_id}/patterns/0")
        self.assertEqual(200, detail_response.status_code)
        detail_payload = detail_response.json()
        self.assertTrue(detail_payload["pattern"])
        self.assertTrue(detail_payload["step_metrics"])

        first_transition = detail_payload["step_metrics"][0]
        transition_response = self.client.get(
            f"/api/runs/{run_id}/transition-cases"
            f"?from_activity={quote(first_transition['activity'])}"
            f"&to_activity={quote(first_transition['next_activity'])}"
            "&pattern_index=0&limit=5"
        )
        self.assertEqual(200, transition_response.status_code)
        self.assertGreaterEqual(transition_response.json()["returned_case_count"], 1)

    def test_log_diagnostics_excel_includes_overall_status_and_next_actions(self):
        csv_bytes = "\n".join(
            [
                "case_id,activity,start_time",
                "C001,Submit,2024-01-01 09:00:00",
                "C001,Approve,2024-01-01 10:00:00",
                "C002,Submit,2024-01-02 09:00:00",
                "C002,Approve,2024-01-02 10:00:00",
            ]
        ).encode("utf-8")

        response = self.client.post(
            "/api/log-diagnostics-excel",
            data={
                "case_id_column": "case_id",
                "activity_column": "activity",
                "timestamp_column": "start_time",
            },
            files={"csv_file": ("custom_log.csv", BytesIO(csv_bytes), "text/csv")},
        )

        self.assertEqual(200, response.status_code)
        workbook = load_workbook(BytesIO(response.content))
        summary_sheet = workbook["ログ診断"]
        status_row = self.find_row_by_value(summary_sheet, "総合判定")
        self.assertEqual("問題なし", summary_sheet.cell(row=status_row, column=2).value)
        next_action_row = self.find_row_by_value(summary_sheet, "次に確認すること")
        self.assertIn(
            "致命的な問題は見つかっていません",
            str(summary_sheet.cell(row=next_action_row + 1, column=2).value),
        )

    def test_csv_headers_api_returns_sample_headers_without_upload(self):
        response = self.client.post("/api/csv-headers", data={})

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("sample_event_log.csv", payload["source_file_name"])
        self.assertEqual(["case_id", "activity", "start_time", "end_time"], payload["headers"])
        self.assertEqual("case_id", payload["default_selection"]["case_id_column"])
        self.assertEqual("case_id", payload["column_settings"]["case_id_column"])
        self.assertEqual(3, len(payload["column_settings"]["filters"]))
        self.assertEqual(3, len(payload["filter_options"]["filters"]))
        self.assertIsNone(payload["diagnostics"])

    def test_csv_headers_api_returns_uploaded_headers(self):
        csv_bytes = "request_id,step_name,event_at\nA001,Start,2024-01-01 09:00:00\n".encode("utf-8")

        response = self.client.post(
            "/api/csv-headers",
            files={"csv_file": ("custom_log.csv", BytesIO(csv_bytes), "text/csv")},
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("custom_log.csv", payload["source_file_name"])
        self.assertEqual(["request_id", "step_name", "event_at"], payload["headers"])
        self.assertEqual("", payload["default_selection"]["case_id_column"])

    def test_csv_headers_api_returns_filter_values_without_diagnostics(self):
        csv_bytes = "\n".join(
            [
                "case_no,step_name,event_at,division",
                "A001,Submit,2024-01-01 09:00:00,Sales",
                "A002,Approve,2024-01-02 10:00:00,HR",
                "A003,Review,2024-01-03 11:00:00,Sales",
            ]
        ).encode("utf-8")

        response = self.client.post(
            "/api/csv-headers",
            files={"csv_file": ("custom_log.csv", BytesIO(csv_bytes), "text/csv")},
            data={"filter_column_1": "division"},
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertIsNone(payload["diagnostics"])
        self.assertEqual("division", payload["filter_options"]["filters"][0]["column_name"])
        self.assertEqual(["HR", "Sales"], payload["filter_options"]["filters"][0]["options"])

    def test_analyze_api_rejects_duplicate_selected_columns(self):
        response = self.client.post(
            "/api/analyze",
            data={
                "case_id_column": "case_id",
                "activity_column": "case_id",
                "timestamp_column": "start_time",
                "analysis_keys": ["frequency"],
            },
        )

        self.assertEqual(400, response.status_code)
        payload = response.json()
        self.assertIn("ケースID列 / アクティビティ列 / タイムスタンプ列", payload["error"])


    def test_log_diagnostics_api_returns_uploaded_summary(self):
        csv_bytes = "\n".join(
            [
                "case_no,step_name,event_at,division",
                "A001,Submit,2024-01-01 09:00:00,Sales",
                "A001,Submit,2024-01-01 09:00:00,Sales",
                "A002,Approve,2024-01-02 10:00:00,HR",
            ]
        ).encode("utf-8")

        response = self.client.post(
            "/api/log-diagnostics",
            files={"csv_file": ("custom_log.csv", BytesIO(csv_bytes), "text/csv")},
            data={
                "case_id_column": "case_no",
                "activity_column": "step_name",
                "timestamp_column": "event_at",
            },
        )

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("custom_log.csv", payload["source_file_name"])
        self.assertEqual(["case_no", "step_name", "event_at", "division"], payload["headers"])
        self.assertEqual(3, payload["diagnostics"]["record_count"])
        self.assertEqual(2, payload["diagnostics"]["activity_type_count"])
        self.assertEqual(3, payload["diagnostics"]["event_count"])
        self.assertEqual(1, payload["diagnostics"]["duplicate_row_count"])
        self.assertEqual("\u3042\u308a", payload["diagnostics"]["duplicate_status"])
        self.assertEqual(2, payload["diagnostics"]["deduplicated_record_count"])
        self.assertEqual(0.3333, payload["diagnostics"]["duplicate_rate"])
        self.assertEqual("case_no", payload["default_selection"]["case_id_column"])

    def test_log_diagnostics_excel_api_returns_summary_and_sample_sheets(self):
        csv_bytes = "\n".join(
            [
                "case_no,step_name,event_at,division",
                "A001,Submit,2024-01-01 09:00:00,Sales",
                "A001,Submit,2024-01-01 09:00:00,Sales",
                "A002,Approve,2024-01-02 10:00:00,HR",
                "A003,Review,2024-01-03 11:00:00,Sales",
            ]
        ).encode("utf-8")

        response = self.client.post(
            "/api/log-diagnostics-excel",
            files={"csv_file": ("custom_log.csv", BytesIO(csv_bytes), "text/csv")},
            data={
                "case_id_column": "case_no",
                "activity_column": "step_name",
                "timestamp_column": "event_at",
                "filter_column_1": "division",
                "sample_row_limit": "2",
            },
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response.headers["content-type"],
        )
        self.assertIn(
            quote("custom_log_ログ診断.xlsx"),
            response.headers["content-disposition"],
        )

        workbook = load_workbook(BytesIO(response.content))
        self.assertEqual(["ログ診断", "ログサンプル"], workbook.sheetnames)

        summary_sheet = workbook["ログ診断"]
        self.assertEqual("ログ診断サマリー", summary_sheet["A1"].value)
        self.assertEqual("元ファイル名", summary_sheet["A4"].value)
        self.assertEqual("custom_log.csv", summary_sheet["B4"].value)
        summary_values = [
            summary_sheet.cell(row=row_index, column=1).value
            for row_index in range(1, summary_sheet.max_row + 1)
        ]
        self.assertNotIn("フィルター候補", summary_values)
        self.assertNotIn("グループ/カテゴリー フィルター①", summary_values)

        sample_sheet = workbook["ログサンプル"]
        self.assertEqual("ログサンプル", sample_sheet["A1"].value)
        self.assertEqual("レコード順", sample_sheet["A3"].value)
        self.assertEqual(1, sample_sheet["A4"].value)
        self.assertEqual("A001", sample_sheet["B4"].value)
        self.assertEqual(2, sample_sheet["A5"].value)
        self.assertEqual("A001", sample_sheet["B5"].value)
        self.assertIsNone(sample_sheet["A6"].value)


    # -------------------------------------------------------------------------
    # 統計指標（標準偏差・パーセンタイル）のテスト
    # -------------------------------------------------------------------------

    def _run_sample_analysis(self, analysis_keys):
        """サンプルCSV（sample_event_log.csv）でデフォルト設定のまま分析を実行して run_id を返す。"""
        response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": analysis_keys},
        )
        self.assertEqual(200, response.status_code)
        return response.json()["run_id"]

    def test_frequency_analysis_has_std_and_percentile_columns(self):
        """頻度分析の結果に標準偏差・P75/P90/P95 列が含まれること。"""
        run_id = self._run_sample_analysis(["frequency"])
        response = self.client.get(f"/api/runs/{run_id}/analyses/frequency")
        self.assertEqual(200, response.status_code)

        rows = response.json()["analyses"]["frequency"]["rows"]
        self.assertTrue(rows, "頻度分析の結果行が空です。")

        first_row = rows[0]
        for col in ("標準偏差(分)", "75%点(分)", "90%点(分)", "95%点(分)"):
            self.assertIn(col, first_row, f"列 '{col}' が頻度分析の結果に存在しません。")

    def test_frequency_analysis_kakuin_statistics_match_expected(self):
        """確認アクティビティ（8件）の統計値がサンプルデータの手計算値と一致すること。

        サンプルデータの確認 duration_min（次イベント開始 - 自イベント開始）:
          C001:8, C002-1回目:17, C002-2回目:14, C003:7, C004:12, C005-1回目:7, C005-2回目:8, C006:4
          sorted = [4, 7, 7, 8, 8, 12, 14, 17]
          std  ≈ 4.31  (ddof=1)
          P75  = 12.5
          P90  = 14.9
          P95  = 15.95
        """
        run_id = self._run_sample_analysis(["frequency"])
        response = self.client.get(f"/api/runs/{run_id}/analyses/frequency")
        rows = response.json()["analyses"]["frequency"]["rows"]

        kakuin_row = next((r for r in rows if r.get("アクティビティ") == "確認"), None)
        self.assertIsNotNone(kakuin_row, "確認アクティビティの行が見つかりません。")

        self.assertEqual(8, kakuin_row["イベント件数"])
        self.assertAlmostEqual(4.31, float(kakuin_row["標準偏差(分)"]), delta=0.15)
        self.assertAlmostEqual(12.5, float(kakuin_row["75%点(分)"]), delta=0.05)
        self.assertAlmostEqual(14.9, float(kakuin_row["90%点(分)"]), delta=0.05)
        self.assertAlmostEqual(15.95, float(kakuin_row["95%点(分)"]), delta=0.05)

    def test_frequency_analysis_std_is_dash_for_single_event_activity(self):
        """イベントが1件しかないアクティビティの標準偏差が '-' になること。"""
        csv_text = "\n".join([
            "case_id,activity,start_time",
            "C001,受付,2026-01-01 09:00:00",
            "C001,確認,2026-01-01 09:05:00",
            "C001,完了,2026-01-01 09:10:00",
        ])
        run_id = self.analyze_uploaded_csv(csv_text, analysis_keys=["frequency"])
        response = self.client.get(f"/api/runs/{run_id}/analyses/frequency")
        self.assertEqual(200, response.status_code)

        rows = response.json()["analyses"]["frequency"]["rows"]
        # 全アクティビティがそれぞれ1件 → 標準偏差は "-" になる
        for row in rows:
            self.assertEqual("-", row["標準偏差(分)"],
                             f"アクティビティ '{row.get('アクティビティ')}' の標準偏差が '-' ではありません。")

    def test_transition_analysis_has_statistics_columns(self):
        """前後処理分析の結果に標準偏差・P75/P90/P95 等の統計列が含まれること。"""
        run_id = self._run_sample_analysis(["transition"])
        response = self.client.get(f"/api/runs/{run_id}/analyses/transition")
        self.assertEqual(200, response.status_code)

        rows = response.json()["analyses"]["transition"]["rows"]
        self.assertTrue(rows, "前後処理分析の結果行が空です。")

        first_row = rows[0]
        self.assertEqual(17, len(first_row))
        for col in (
            "平均所要時間(分)",
            "中央値所要時間(分)",
            "標準偏差(分)",
            "最小所要時間(分)",
            "最大所要時間(分)",
            "75%点(分)",
            "90%点(分)",
            "95%点(分)",
            "ケース比率(%)",
            "前処理平均時間(分)",
            "後処理平均時間(分)",
        ):
            self.assertIn(col, first_row, f"列 '{col}' が前後処理分析の結果に存在しません。")

    def test_get_terminology_rows(self):
        from excel.common import get_terminology_rows

        freq_rows = get_terminology_rows("frequency")
        self.assertTrue(any(row["用語"] == "イベント比率(%)" for row in freq_rows))
        self.assertTrue(any(row["用語"] == "ケース比率(%)" for row in freq_rows))

        trans_rows = get_terminology_rows("transition")
        self.assertTrue(any(row["用語"] == "遷移" for row in trans_rows))
        self.assertTrue(any(row["用語"] == "ケース比率(%)" for row in trans_rows))
        self.assertTrue(any(row["用語"] == "前処理平均時間(分)" for row in trans_rows))
        self.assertTrue(any(row["用語"] == "後処理平均時間(分)" for row in trans_rows))
        self.assertFalse(any(row["用語"] == "イベント比率(%)" for row in trans_rows))

        pattern_rows = get_terminology_rows("pattern")
        self.assertTrue(any(row["用語"] == "処理順パターン" for row in pattern_rows))
        self.assertTrue(any(row["用語"] == "累積カバー率(%)" for row in pattern_rows))
        self.assertTrue(any(row["用語"] == "繰り返し率(%)" for row in pattern_rows))
        self.assertTrue(any(row["用語"] == "改善優先度スコア" for row in pattern_rows))
        self.assertTrue(any(row["用語"] == "全体影響度(%)" for row in pattern_rows))

        unknown_rows = get_terminology_rows("unknown")
        self.assertEqual(4, len(unknown_rows))

    def test_pattern_analysis_has_std_and_percentile_columns(self):
        """処理順パターン分析の結果に標準偏差・P75/P90/P95 列が含まれること。"""
        run_id = self._run_sample_analysis(["pattern"])
        response = self.client.get(f"/api/runs/{run_id}/analyses/pattern")
        self.assertEqual(200, response.status_code)

        rows = response.json()["analyses"]["pattern"]["rows"]
        self.assertTrue(rows, "処理順パターン分析の結果行が空です。")

        first_row = rows[0]
        for col in (
            "繰り返し",
            "繰り返し回数",
            "繰り返し率(%)",
            "繰り返し率区分",
            "確認区分",
            "平均処理時間差分(分)",
            "改善優先度スコア",
            "全体影響度(%)",
            "最短処理",
            "簡易コメント",
            "ステップ数",
            "繰り返しアクティビティ",
            "累積カバー率(%)",
            "標準偏差ケース処理時間(分)",
            "75%点ケース処理時間(分)",
            "90%点ケース処理時間(分)",
            "95%点ケース処理時間(分)",
        ):
            self.assertIn(col, first_row, f"列 '{col}' が処理順パターン分析の結果に存在しません。")
        self.assertGreater(first_row["ステップ数"], 0)
        self.assertIsInstance(first_row["繰り返しアクティビティ"], str)

    def test_pattern_analysis_assigns_repeat_flags_and_evaluations(self):
        csv_text = "\n".join([
            "case_id,activity,start_time",
            "C001,受付,2026-01-01 09:00:00",
            "C001,確認,2026-01-01 09:05:00",
            "C001,完了,2026-01-01 09:10:00",
            "C002,受付,2026-01-01 10:00:00",
            "C002,確認,2026-01-01 10:06:00",
            "C002,完了,2026-01-01 10:12:00",
            "C003,受付,2026-01-01 11:00:00",
            "C003,確認,2026-01-01 11:05:00",
            "C003,差戻し,2026-01-01 11:10:00",
            "C003,確認,2026-01-01 11:18:00",
            "C003,完了,2026-01-01 11:28:00",
            "C004,受付,2026-01-01 12:00:00",
            "C004,確認,2026-01-01 12:05:00",
            "C004,保留,2026-01-01 12:10:00",
            "C004,再確認,2026-01-01 12:20:00",
            "C004,承認,2026-01-01 12:30:00",
            "C004,完了,2026-01-01 12:40:00",
        ])
        run_id = self.analyze_uploaded_csv(csv_text, analysis_keys=["pattern"])
        response = self.client.get(f"/api/runs/{run_id}/analyses/pattern")
        self.assertEqual(200, response.status_code)

        rows = response.json()["analyses"]["pattern"]["rows"]
        pattern_by_route = {
            row["処理順パターン"]: row
            for row in rows
        }

        self.assertEqual("", pattern_by_route["受付→確認→完了"]["繰り返し"])
        self.assertEqual(0, pattern_by_route["受付→確認→完了"]["繰り返し回数"])
        self.assertEqual(0.0, pattern_by_route["受付→確認→完了"]["繰り返し率(%)"])
        self.assertEqual("0〜10%", pattern_by_route["受付→確認→完了"]["繰り返し率区分"])
        self.assertEqual("", pattern_by_route["受付→確認→完了"]["確認区分"])
        self.assertEqual("○", pattern_by_route["受付→確認→完了"]["最短処理"])
        self.assertEqual(3, pattern_by_route["受付→確認→完了"]["ステップ数"])
        self.assertEqual("", pattern_by_route["受付→確認→完了"]["繰り返しアクティビティ"])
        self.assertEqual(50.0, pattern_by_route["受付→確認→完了"]["累積カバー率(%)"])
        self.assertLess(float(pattern_by_route["受付→確認→完了"]["平均処理時間差分(分)"]), 0)
        self.assertEqual(0.0, float(pattern_by_route["受付→確認→完了"]["改善優先度スコア"]))
        self.assertIn("安定", pattern_by_route["受付→確認→完了"]["簡易コメント"])
        self.assertEqual("○", pattern_by_route["受付→確認→差戻し→確認→完了"]["繰り返し"])
        self.assertEqual(1, pattern_by_route["受付→確認→差戻し→確認→完了"]["繰り返し回数"])
        self.assertEqual(20.0, pattern_by_route["受付→確認→差戻し→確認→完了"]["繰り返し率(%)"])
        self.assertEqual("10〜30%", pattern_by_route["受付→確認→差戻し→確認→完了"]["繰り返し率区分"])
        self.assertEqual("", pattern_by_route["受付→確認→差戻し→確認→完了"]["確認区分"])
        self.assertEqual(5, pattern_by_route["受付→確認→差戻し→確認→完了"]["ステップ数"])
        self.assertEqual("確認", pattern_by_route["受付→確認→差戻し→確認→完了"]["繰り返しアクティビティ"])
        self.assertGreater(float(pattern_by_route["受付→確認→差戻し→確認→完了"]["平均処理時間差分(分)"]), 0)
        self.assertGreater(float(pattern_by_route["受付→確認→差戻し→確認→完了"]["改善優先度スコア"]), 0)
        self.assertGreaterEqual(float(pattern_by_route["受付→確認→差戻し→確認→完了"]["全体影響度(%)"]), 0)
        self.assertIn("改善候補", pattern_by_route["受付→確認→差戻し→確認→完了"]["簡易コメント"])
        self.assertEqual("", pattern_by_route["受付→確認→保留→再確認→承認→完了"]["繰り返し"])
        self.assertEqual(0, pattern_by_route["受付→確認→保留→再確認→承認→完了"]["繰り返し回数"])
        self.assertEqual(0.0, pattern_by_route["受付→確認→保留→再確認→承認→完了"]["繰り返し率(%)"])
        self.assertEqual("0〜10%", pattern_by_route["受付→確認→保留→再確認→承認→完了"]["繰り返し率区分"])
        self.assertEqual("", pattern_by_route["受付→確認→保留→再確認→承認→完了"]["確認区分"])
        self.assertEqual(6, pattern_by_route["受付→確認→保留→再確認→承認→完了"]["ステップ数"])
        self.assertEqual("", pattern_by_route["受付→確認→保留→再確認→承認→完了"]["繰り返しアクティビティ"])
        self.assertGreater(float(pattern_by_route["受付→確認→保留→再確認→承認→完了"]["平均処理時間差分(分)"]), 0)
        self.assertEqual(0.0, float(pattern_by_route["受付→確認→保留→再確認→承認→完了"]["改善優先度スコア"]))
        self.assertIn("平均超過", pattern_by_route["受付→確認→保留→再確認→承認→完了"]["簡易コメント"])

    def test_pattern_analysis_std_is_dash_for_single_case_pattern(self):
        """ケース数が1件のパターンの標準偏差が '-' になること。"""
        csv_text = "\n".join([
            "case_id,activity,start_time",
            "C001,受付,2026-01-01 09:00:00",
            "C001,確認,2026-01-01 09:05:00",
            "C001,完了,2026-01-01 09:10:00",
            "C002,受付,2026-01-01 10:00:00",
            "C002,確認,2026-01-01 10:05:00",
            "C002,差戻し,2026-01-01 10:08:00",
            "C002,確認,2026-01-01 10:12:00",
            "C002,完了,2026-01-01 10:18:00",
        ])
        run_id = self.analyze_uploaded_csv(csv_text, analysis_keys=["pattern"])
        response = self.client.get(f"/api/runs/{run_id}/analyses/pattern")
        self.assertEqual(200, response.status_code)

        rows = response.json()["analyses"]["pattern"]["rows"]
        # C001 と C002 で異なるパターン → それぞれ1件 → 標準偏差は "-"
        for row in rows:
            self.assertEqual("-", row["標準偏差ケース処理時間(分)"],
                             f"パターン '{row.get('処理順パターン')}' の標準偏差が '-' ではありません。")


if __name__ == "__main__":
    unittest.main()




