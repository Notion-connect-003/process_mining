from tests.web_app_test_base import *

class WebAppExcelTestCase(WebAppTestCaseBase):
    def test_report_excel_group_comparison_in_ai_text(self):
        """???????????AI????????????????????????????????"""
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time,department",
                    "C001,Submit,2024-01-01 09:00:00,Sales",
                    "C001,Approve,2024-01-01 10:00:00,Sales",
                    "C002,Submit,2024-01-02 09:00:00,Sales",
                    "C002,Approve,2024-01-02 11:00:00,Sales",
                    "C003,Submit,2024-01-03 09:00:00,HR",
                    "C003,Approve,2024-01-03 09:30:00,HR",
                    "C004,Submit,2024-01-04 09:00:00,HR",
                    "C004,Approve,2024-01-04 09:45:00,HR",
                ]
            ),
            extra_data={
                "filter_column_1": "department",
            },
        )

        with mock.patch(
            "app.main.request_ollama_insights_text",
            side_effect=app_main.httpx.ConnectError("offline"),
        ):
            response = self.client.get(
                f"/api/runs/{run_id}/report-excel"
                "?analysis_key=frequency"
            )

        self.assertEqual(200, response.status_code)

        workbook = load_workbook(BytesIO(response.content))
        ai_sheet = workbook["\u5206\u6790\u30b3\u30e1\u30f3\u30c8"]

        all_values = []
        for row_index in range(1, ai_sheet.max_row + 1):
            cell_value = ai_sheet.cell(row=row_index, column=1).value
            if cell_value:
                all_values.append(str(cell_value))

        full_text = "\n".join(all_values)

        self.assertIn("\u30b0\u30eb\u30fc\u30d7\u9593\u6bd4\u8f03", full_text)
        self.assertTrue(
            "Sales" in full_text or "HR" in full_text,
            f"Group names not found in AI text: {full_text[:500]}",
        )

    def test_report_excel_export_api_falls_back_when_ollama_is_unavailable(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time",
                    "C001,Submit,2024-01-01 09:00:00",
                    "C001,Approve,2024-01-02 09:00:00",
                    "C002,Submit,2024-01-03 09:00:00",
                    "C002,Approve,2024-01-04 09:00:00",
                ]
            )
        )

        with mock.patch(
            "app.main.request_ollama_insights_text",
            side_effect=app_main.httpx.ConnectError("offline"),
        ):
            response = self.client.get(
                f"/api/runs/{run_id}/report-excel?analysis_key=frequency"
            )

        self.assertEqual(200, response.status_code)
        self.assertIn(
            quote("custom_log_頻度分析レポート.xlsx"),
            response.headers["content-disposition"],
        )

        workbook = load_workbook(BytesIO(response.content))
        summary_sheet = workbook["サマリー"]
        summary_pairs = self.summary_section_pairs(summary_sheet)
        summary_values = [
            summary_sheet.cell(row=row_index, column=1).value
            for row_index in range(1, summary_sheet.max_row + 1)
        ]
        self.assertNotIn("実行ID", summary_pairs)
        self.assertNotIn("選択中アクティビティ", summary_pairs)
        self.assertNotIn("選択中遷移", summary_pairs)
        self.assertNotIn("選択中ケースID", summary_pairs)
        self.assertEqual("なし", summary_pairs["グルーピング条件"])
        applied_filters_row = self.find_row_by_value(summary_sheet, "適用条件")
        self.assertEqual("", summary_sheet.cell(row=applied_filters_row + 1, column=1).value or "")
        filter_note = str(summary_sheet.cell(row=applied_filters_row + 1, column=2).value)
        self.assertIn("※ 適用条件の種類:", filter_note)
        self.assertIn("期間フィルター: 開始日 / 終了日", filter_note)
        self.assertIn("グループ/カテゴリーフィルター①②③", filter_note)
        self.assertIn("アクティビティフィルター: 特定アクティビティを含む/除外", filter_note)
        grouping_row = self.find_row_by_value(summary_sheet, "グルーピング条件")
        self.assertEqual("", summary_sheet.cell(row=grouping_row + 1, column=1).value or "")
        grouping_note = str(summary_sheet.cell(row=grouping_row + 1, column=2).value)
        self.assertIn("カラムを選択し値を未選択にすると", grouping_note)
        self.assertIn("グルーピング軸（比較用）", grouping_note)
        self.assertEqual("分析ハイライト", summary_values[summary_values.index("分析ハイライト")])
        self.assertIn("最大ケース所要時間", summary_values)
        self.assertIn("バリアント総数", summary_values)
        self.assertIn("ユニークアクティビティ数", summary_values)
        self.assertIn("平均ケースあたりイベント数", summary_values)
        ai_sheet = workbook["分析コメント"]
        self.assertEqual("分析期間", ai_sheet["A5"].value)
        explanation_row = self.find_row_by_value(ai_sheet, "解説本文")
        self.assertTrue(str(ai_sheet.cell(row=explanation_row + 1, column=1).value).strip())
        ai_sheet_values = [
            ai_sheet.cell(row=row_index, column=1).value
            for row_index in range(1, ai_sheet.max_row + 1)
        ]
        self.assertNotIn("補足・免責事項", ai_sheet_values)
        frequency_sheet = workbook["頻度分析"]
        frequency_headers = [
            frequency_sheet.cell(row=3, column=column_index).value
            for column_index in range(1, frequency_sheet.max_column + 1)
        ]
        for header in ("標準偏差(分)", "75%点(分)", "90%点(分)", "95%点(分)"):
            self.assertIn(header, frequency_headers)

    def test_analyze_api_applies_start_end_activity_filters_to_preview_and_detail(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time",
                    "C001,Submit,2024-01-01 09:00:00",
                    "C001,Approve,2024-01-01 09:05:00",
                    "C001,Done,2024-01-01 09:10:00",
                    "C002,Intake,2024-01-01 10:00:00",
                    "C002,Approve,2024-01-01 10:05:00",
                    "C002,Done,2024-01-01 10:10:00",
                    "C003,Submit,2024-01-01 11:00:00",
                    "C003,Check,2024-01-01 11:05:00",
                    "C003,Reject,2024-01-01 11:10:00",
                ]
            ),
            analysis_keys=["frequency"],
            extra_data={
                "start_activity_values": "Submit",
                "end_activity_values": "Done",
            },
        )

        detail_response = self.client.get(f"/api/runs/{run_id}/analyses/frequency")
        self.assertEqual(200, detail_response.status_code)

        payload = detail_response.json()
        self.assertEqual(1, payload["case_count"])
        self.assertEqual(3, payload["event_count"])
        self.assertEqual("Submit", payload["applied_filters"]["start_activity_values"])
        self.assertEqual("Done", payload["applied_filters"]["end_activity_values"])

    def test_filter_options_api_returns_all_activity_names_for_start_end_filters(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time",
                    "C001,Submit,2024-01-01 09:00:00",
                    "C001,Approve,2024-01-01 09:05:00",
                    "C001,Done,2024-01-01 09:10:00",
                    "C002,Intake,2024-01-01 10:00:00",
                    "C002,Done,2024-01-01 10:10:00",
                ]
            ),
            analysis_keys=["frequency"],
        )

        response = self.client.get(f"/api/runs/{run_id}/filter-options")
        self.assertEqual(200, response.status_code)

        payload = response.json()
        self.assertEqual(
            ["Approve", "Done", "Intake", "Submit"],
            payload["options"]["all_activity_names"],
        )

    def test_frequency_ai_fallback_uses_actual_activity_name_and_event_count(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time",
                    "C001,Submit,2024-01-01 09:00:00",
                    "C001,Approve,2024-01-01 10:00:00",
                    "C002,Submit,2024-01-02 09:00:00",
                    "C002,Approve,2024-01-02 10:00:00",
                    "C003,Submit,2024-01-03 09:00:00",
                    "C003,Reject,2024-01-03 10:00:00",
                ]
            ),
            analysis_keys=["frequency"],
        )

        with mock.patch(
            "app.main.request_ollama_insights_text",
            side_effect=app_main.httpx.ConnectError("offline"),
        ):
            response = self.client.get(
                f"/api/runs/{run_id}/report-excel?analysis_key=frequency"
            )

        self.assertEqual(200, response.status_code)

        workbook = load_workbook(BytesIO(response.content))
        ai_sheet = workbook["分析コメント"]
        explanation_row = self.find_row_by_value(ai_sheet, "解説本文")
        explanation_text = "\n".join(
            str(ai_sheet.cell(row=row_index, column=1).value or "")
            for row_index in range(explanation_row + 1, ai_sheet.max_row + 1)
        )

        self.assertIn("Submit", explanation_text)
        self.assertIn("3 件", explanation_text)
        self.assertNotIn("「不明」", explanation_text)
        self.assertNotIn("0 件", explanation_text)

    def test_report_excel_export_api_returns_transition_workbook(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time",
                    "C001,Submit,2024-01-01 09:00:00",
                    "C001,Approve,2024-01-02 09:00:00",
                    "C002,Submit,2024-01-03 09:00:00",
                    "C002,Reject,2024-01-04 09:00:00",
                ]
            ),
            analysis_keys=["transition"],
        )

        with mock.patch(
            "app.main.build_excel_ai_summary",
            return_value={
                "title": "分析コメント",
                "mode": "ollama",
                "provider": "Ollama (qwen2.5:7b)",
                "generated_at": "2026-04-02T00:00:00+00:00",
                "period": "2024-01-01 09:00 〜 2024-01-04 09:00",
                "text": "前後処理向けの分析コメントです。",
                "highlights": ["遷移の詰まりを確認してください。"],
                "note": "ローカルLLMで生成した解説を掲載しています。",
            },
        ):
            response = self.client.get(
                f"/api/runs/{run_id}/report-excel"
                "?analysis_key=transition"
                "&selected_transition_key=Submit__TO__Approve"
            )

        self.assertEqual(200, response.status_code)
        self.assertIn(
            quote("custom_log_前後処理分析レポート.xlsx"),
            response.headers["content-disposition"],
        )
        workbook = load_workbook(BytesIO(response.content))
        self.assertEqual(
            ["サマリー", "分析コメント", "前後処理分析", "ボトルネック分析", "改善インパクト分析", "ドリルダウン"],
            self.visible_sheetnames(workbook),
        )
        summary_sheet = workbook["サマリー"]
        summary_pairs = self.summary_section_pairs(summary_sheet)
        self.assertNotIn("実行ID", summary_pairs)
        self.assertEqual("Submit → Approve", summary_pairs["選択中遷移"])
        self.assertEqual("未選択", summary_pairs["選択中アクティビティ"])
        self.assertEqual("未選択", summary_pairs["選択中ケースID"])
        self.assertNotIn("\u983b\u5ea6\u5206\u6790", workbook.sheetnames)
        self.assertNotIn("\u51e6\u7406\u9806\u30d1\u30bf\u30fc\u30f3\u5206\u6790", workbook.sheetnames)
        self.assertNotIn("\u30d0\u30ea\u30a2\u30f3\u30c8\u5206\u6790", workbook.sheetnames)
        transition_sheet = workbook["\u524d\u5f8c\u51e6\u7406\u5206\u6790"]
        self.assertEqual("\u524d\u5f8c\u51e6\u7406\u5206\u6790", transition_sheet["A1"].value)
        self.assertEqual("\u9806\u4f4d", transition_sheet["A3"].value)
        transition_headers = [
            transition_sheet.cell(row=3, column=column_index).value
            for column_index in range(1, transition_sheet.max_column + 1)
        ]
        for header in ("標準偏差(分)", "75%点(分)", "90%点(分)", "95%点(分)"):
            self.assertIn(header, transition_headers)

        bottleneck_sheet = workbook["\u30dc\u30c8\u30eb\u30cd\u30c3\u30af\u5206\u6790"]
        self.assertEqual("アクティビティボトルネック", bottleneck_sheet["A1"].value)
        self.assertEqual("平均所要時間", bottleneck_sheet["E3"].value)
        transition_title_row = next(
            row_index
            for row_index in range(1, bottleneck_sheet.max_row + 1)
            if bottleneck_sheet.cell(row=row_index, column=1).value == "遷移ボトルネック"
        )
        self.assertEqual("遷移ボトルネック", bottleneck_sheet.cell(row=transition_title_row, column=1).value)
        self.assertEqual("平均所要時間", bottleneck_sheet.cell(row=transition_title_row + 2, column=5).value)

        activity_data_rows = [
            row
            for row in bottleneck_sheet.iter_rows(min_row=4, max_row=transition_title_row - 1, values_only=True)
            if row[0] not in (None, "")
        ]
        self.assertTrue(activity_data_rows)
        self.assertTrue(all(row[0] not in (None, "") for row in activity_data_rows))
        self.assertTrue(all(row[1] not in (None, "") for row in activity_data_rows))
        self.assertTrue(all(row[4] not in (None, "") for row in activity_data_rows))
        self.assertTrue(all(row[5] not in (None, "") for row in activity_data_rows))
        self.assertTrue(all(row[6] not in (None, "") for row in activity_data_rows))

        transition_data_rows = [
            row
            for row in bottleneck_sheet.iter_rows(min_row=transition_title_row + 3, values_only=True)
            if row[0] not in (None, "")
        ]
        self.assertTrue(transition_data_rows)
        self.assertTrue(all(row[0] not in (None, "") for row in transition_data_rows))
        self.assertTrue(all(row[1] not in (None, "") for row in transition_data_rows))
        self.assertTrue(all(row[4] not in (None, "") for row in transition_data_rows))
        self.assertTrue(all(row[5] not in (None, "") for row in transition_data_rows))
        self.assertTrue(all(row[6] not in (None, "") for row in transition_data_rows))

    def test_query_group_summary_matches_build_group_summary_for_parquet(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time,group_a,group_b",
                    "C001,Submit,2024-01-01 09:00:00,Sales,Web",
                    "C001,Approve,2024-01-02 09:00:00,Sales,Web",
                    "C002,Submit,2024-01-03 09:00:00,Sales,API",
                    "C002,Approve,2024-01-04 09:00:00,Sales,API",
                    "C003,Submit,2024-01-05 09:00:00,HR,Mail",
                    "C003,Reject,2024-01-06 09:00:00,HR,Mail",
                ]
            ),
            analysis_keys=["frequency"],
            extra_data={
                "filter_column_1": "group_a",
                "filter_column_2": "group_b",
            },
        )

        run_data = app_main.get_run_data(run_id)
        expected = run_data["result"]["group_summary"]
        actual = app_main.query_group_summary(
            run_data["prepared_parquet_path"],
            ["group_a", "group_b"],
        )
        self.assertEqual(expected, actual)

    def test_report_excel_export_api_groups_frequency_sheet_sections_in_memory_mode(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time,group_a,group_b",
                    "C001,Submit,2024-01-01 09:00:00,Sales,Web",
                    "C001,Approve,2024-01-02 09:00:00,Sales,Web",
                    "C002,Submit,2024-01-03 09:00:00,Sales,API",
                    "C002,Approve,2024-01-04 09:00:00,Sales,API",
                    "C003,Submit,2024-01-05 09:00:00,Sales,Web",
                    "C003,Approve,2024-01-06 09:00:00,Sales,Web",
                    "C004,Submit,2024-01-07 09:00:00,HR,Mail",
                    "C004,Reject,2024-01-08 09:00:00,HR,Mail",
                    "C005,Submit,2024-01-09 09:00:00,HR,Mail",
                    "C005,Approve,2024-01-10 09:00:00,HR,Mail",
                    "C006,Submit,2024-01-11 09:00:00,,Tokyo",
                    "C006,Approve,2024-01-12 09:00:00,,Tokyo",
                ]
            ),
            analysis_keys=["frequency"],
            extra_data={
                "filter_column_1": "group_a",
            },
        )

        with mock.patch(
            "app.main.build_excel_ai_summary",
            return_value={
                "title": "分析コメント",
                "mode": "ollama",
                "provider": "Ollama (qwen2.5:7b)",
                "generated_at": "2026-04-09T00:00:00+00:00",
                "period": "2024-01-01 09:00 〜 2024-01-12 09:00",
                "text": "既存集計からの要約です。",
                "highlights": ["頻度の高い活動を確認してください。"],
                "note": "ローカルLLMで生成した解説を掲載しています。",
            },
        ):
            response = self.client.get(
                f"/api/runs/{run_id}/report-excel?analysis_key=frequency"
            )

        self.assertEqual(200, response.status_code)
        workbook = load_workbook(BytesIO(response.content))
        frequency_sheet = workbook["頻度分析"]
        self.assertEqual("═══ 全体 ═══", frequency_sheet["A1"].value)
        self.assertEqual(True, frequency_sheet["A1"].font.bold)
        self.assertEqual(12, frequency_sheet["A1"].font.size)
        self.assertTrue(str(frequency_sheet["A1"].fill.fgColor.rgb).endswith("D9E1F2"))
        self.assertEqual("順位", frequency_sheet["A2"].value)
        group_header_rows = [
            row_index
            for row_index in range(1, frequency_sheet.max_row + 1)
            if str(frequency_sheet.cell(row=row_index, column=1).value or "").startswith("═══ グループ:")
        ]
        self.assertEqual(
            [
                "═══ グループ: Sales ═══",
                "═══ グループ: HR ═══",
                "═══ グループ: (未分類) ═══",
            ],
            [frequency_sheet.cell(row=row_index, column=1).value for row_index in group_header_rows],
        )
        first_group_row = group_header_rows[0]
        self.assertTrue(all((frequency_sheet.cell(row=first_group_row - offset, column=1).value in (None, "")) for offset in (1, 2, 3)))

    def test_report_excel_export_api_groups_frequency_sheet_sections_for_multiple_axes(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time,group_a,group_b,group_c",
                    "C001,Submit,2024-01-01 09:00:00,Sales,Web,A",
                    "C001,Approve,2024-01-02 09:00:00,Sales,Web,A",
                    "C002,Submit,2024-01-03 09:00:00,HR,Mail,B",
                    "C002,Reject,2024-01-04 09:00:00,HR,Mail,B",
                ]
            ),
            analysis_keys=["frequency"],
            extra_data={
                "filter_column_1": "group_a",
                "filter_column_2": "group_b",
            },
        )

        with mock.patch(
            "app.main.build_excel_ai_summary",
            return_value={
                "title": "分析コメント",
                "mode": "ollama",
                "provider": "Ollama (qwen2.5:7b)",
                "generated_at": "2026-04-09T00:00:00+00:00",
                "period": "2024-01-01 09:00 〜 2024-01-04 09:00",
                "text": "既存集計からの要約です。",
                "highlights": ["頻度の高い活動を確認してください。"],
                "note": "ローカルLLMで生成した解説を掲載しています。",
            },
        ):
            response = self.client.get(
                f"/api/runs/{run_id}/report-excel?analysis_key=frequency"
            )

        self.assertEqual(200, response.status_code)
        workbook = load_workbook(BytesIO(response.content))
        frequency_sheet = workbook["頻度分析"]
        group_headers = [
            frequency_sheet.cell(row=row_index, column=1).value
            for row_index in range(1, frequency_sheet.max_row + 1)
            if str(frequency_sheet.cell(row=row_index, column=1).value or "").startswith("═══ グループ:")
        ]
        self.assertIn("═══ グループ: group_a=Sales, group_b=Web ═══", group_headers)
        self.assertIn("═══ グループ: group_a=HR, group_b=Mail ═══", group_headers)

    def test_report_excel_export_parquet_mode_includes_group_comparison_and_group_sections(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time,group_a,group_b,group_c",
                    "C001,Submit,2024-01-01 09:00:00,Sales,Web,A",
                    "C001,Approve,2024-01-02 09:00:00,Sales,Web,A",
                    "C002,Submit,2024-01-03 09:00:00,HR,Mail,B",
                    "C002,Reject,2024-01-04 09:00:00,HR,Mail,B",
                ]
            ),
            analysis_keys=["frequency"],
            extra_data={
                "filter_column_1": "group_a",
                "filter_column_2": "group_b",
                "filter_column_3": "group_c",
            },
        )

        run_data = app_main.get_run_data(run_id)
        self.assertIsNone(run_data["prepared_df"])

        with mock.patch(
            "app.main.build_excel_ai_summary",
            return_value={
                "title": "分析コメント",
                "mode": "ollama",
                "provider": "Ollama (qwen2.5:7b)",
                "generated_at": "2026-04-02T00:00:00+00:00",
                "period": "2024-01-01 09:00 〜 2024-01-04 09:00",
                "text": "既存集計からの要約です。",
                "highlights": ["頻度の高い活動を確認してください。"],
                "note": "ローカルLLMで生成した解説を掲載しています。",
            },
        ):
            response = self.client.get(
                f"/api/runs/{run_id}/report-excel?analysis_key=frequency"
            )

        self.assertEqual(200, response.status_code)
        workbook = load_workbook(BytesIO(response.content))
        summary_sheet = workbook["サマリー"]
        summary_pairs = self.summary_section_pairs(summary_sheet)
        summary_values = [
            summary_sheet.cell(row=row_index, column=1).value
            for row_index in range(1, summary_sheet.max_row + 1)
        ]
        self.assertEqual("group_a、group_b、group_c", summary_pairs["グルーピング条件"])
        self.assertIn("分析ハイライト", summary_values)
        self.assertIn("グループ別比較", summary_values)
        group_table_row = self.find_row_by_value(summary_sheet, "グループ別比較")
        self.assertEqual("全体", summary_sheet.cell(row=group_table_row + 3, column=2).value)

        frequency_sheet = workbook["頻度分析"]
        self.assertEqual("═══ 全体 ═══", frequency_sheet["A1"].value)
        parquet_group_headers = [
            frequency_sheet.cell(row=row_index, column=1).value
            for row_index in range(1, frequency_sheet.max_row + 1)
            if str(frequency_sheet.cell(row=row_index, column=1).value or "").startswith("═══ グループ:")
        ]
        self.assertEqual(
            [
                "═══ グループ: group_a=HR, group_b=Mail, group_c=B ═══",
                "═══ グループ: group_a=Sales, group_b=Web, group_c=A ═══",
            ],
            parquet_group_headers,
        )

    def test_ai_insights_api_restores_cached_output_across_page_reload(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time",
                    "C001,Submit,2024-01-01 09:00:00",
                    "C001,Approve,2024-01-02 09:00:00",
                    "C002,Submit,2024-01-03 09:00:00",
                    "C002,Reject,2024-01-04 09:00:00",
                ]
            ),
            analysis_keys=["frequency", "pattern"],
        )

        empty_response = self.client.get(f"/api/runs/{run_id}/ai-insights/frequency")
        self.assertEqual(200, empty_response.status_code)
        self.assertFalse(empty_response.json()["generated"])

        with mock.patch(
            "app.main.request_ollama_insights_text",
            return_value="頻度分析向けの分析コメントです。",
        ):
            generate_response = self.client.post(f"/api/runs/{run_id}/ai-insights/frequency")

        self.assertEqual(200, generate_response.status_code)
        generate_payload = generate_response.json()
        self.assertTrue(generate_payload["generated"])
        self.assertFalse(generate_payload["cached"])
        self.assertEqual("頻度分析", generate_payload["analysis_name"])
        self.assertEqual("頻度分析向けの分析コメントです。", generate_payload["text"])

        restored_response = self.client.get(f"/api/runs/{run_id}/ai-insights/frequency")
        self.assertEqual(200, restored_response.status_code)
        restored_payload = restored_response.json()
        self.assertTrue(restored_payload["generated"])
        self.assertTrue(restored_payload["cached"])
        self.assertEqual("頻度分析向けの分析コメントです。", restored_payload["text"])

    def test_report_excel_export_uses_analysis_specific_ai_output(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time",
                    "C001,Submit,2024-01-01 09:00:00",
                    "C001,Approve,2024-01-02 09:00:00",
                    "C002,Submit,2024-01-03 09:00:00",
                    "C002,Reject,2024-01-04 09:00:00",
                ]
            ),
            analysis_keys=["frequency", "pattern"],
        )

        def _fake_ai(prompt):
            if "頻度分析" in prompt:
                return "frequency ai"
            if "処理順パターン分析" in prompt:
                return "pattern ai"
            return "generic ai"

        with mock.patch("app.main.request_ollama_insights_text", side_effect=_fake_ai):
            frequency_response = self.client.get(
                f"/api/runs/{run_id}/report-excel?analysis_key=frequency"
            )
            pattern_response = self.client.get(
                f"/api/runs/{run_id}/report-excel?analysis_key=pattern&pattern_display_limit=1"
            )

        self.assertEqual(200, frequency_response.status_code)
        self.assertEqual(200, pattern_response.status_code)

        frequency_workbook = load_workbook(BytesIO(frequency_response.content))
        pattern_workbook = load_workbook(BytesIO(pattern_response.content))
        self.assertEqual(
            ["サマリー", "分析コメント", "頻度分析", "ボトルネック分析", "改善インパクト分析"],
            self.visible_sheetnames(frequency_workbook),
        )
        self.assertEqual(["サマリー", "分析コメント", "結論サマリー", "サマリーダッシュボード", "パターンサマリー", "処理順パターン分析", "パターン01詳細"], self.visible_sheetnames(pattern_workbook))
        frequency_explanation_row = self.find_row_by_value(frequency_workbook["分析コメント"], "解説本文")
        pattern_explanation_row = self.find_row_by_value(pattern_workbook["分析コメント"], "解説本文")
        self.assertEqual("frequency ai", frequency_workbook["分析コメント"].cell(row=frequency_explanation_row + 1, column=1).value)
        self.assertEqual("pattern ai", pattern_workbook["分析コメント"].cell(row=pattern_explanation_row + 1, column=1).value)

    def test_report_excel_retries_ai_when_cached_comment_is_fallback(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time",
                    "C001,Start,2024-01-01 09:00:00",
                    "C001,Review,2024-01-01 10:00:00",
                    "C001,Approve,2024-01-01 11:00:00",
                    "C001,Done,2024-01-01 12:00:00",
                    "C002,Start,2024-01-02 09:00:00",
                    "C002,Review,2024-01-02 10:00:00",
                    "C002,Rework,2024-01-02 11:00:00",
                    "C002,Review,2024-01-02 12:00:00",
                    "C002,Done,2024-01-02 13:00:00",
                ]
            ),
            analysis_keys=["pattern"],
        )

        with mock.patch(
            "app.main.request_ollama_insights_text",
            side_effect=app_main.httpx.ConnectError("offline"),
        ):
            fallback_response = self.client.post(f"/api/runs/{run_id}/ai-insights/pattern")

        self.assertEqual(200, fallback_response.status_code)
        self.assertEqual("fallback", fallback_response.json()["mode"])

        with mock.patch(
            "app.main.request_ollama_insights_text",
            return_value="pattern ai after retry",
        ) as mocked_ai:
            excel_response = self.client.get(
                f"/api/runs/{run_id}/report-excel?analysis_key=pattern&pattern_display_limit=1"
            )

        self.assertEqual(200, excel_response.status_code)
        self.assertEqual(1, mocked_ai.call_count)

        workbook = load_workbook(BytesIO(excel_response.content))
        ai_sheet = workbook["分析コメント"]
        explanation_row = self.find_row_by_value(ai_sheet, "解説本文")
        self.assertEqual(
            "pattern ai after retry",
            ai_sheet.cell(row=explanation_row + 1, column=1).value,
        )

    def test_ai_duration_unit_validation_retries_suspicious_day_conversion(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time",
                    "C001,Submit,2024-01-01 09:00:00",
                    "C001,Done,2024-01-02 09:42:00",
                    "C002,Submit,2024-01-03 09:00:00",
                    "C002,Done,2024-01-04 09:42:00",
                ]
            ),
            analysis_keys=["frequency"],
        )

        with mock.patch(
            "app.main.request_ollama_insights_text",
            side_effect=[
                "【全体傾向】\nSubmit は平均 25日 かかっています。",
                "【全体傾向】\nSubmit は平均 1482分（約24.70時間 / 約1.03日）です。",
            ],
        ) as mocked_ai:
            response = self.client.post(f"/api/runs/{run_id}/ai-insights/frequency")

        self.assertEqual(200, response.status_code)
        self.assertEqual(2, mocked_ai.call_count)
        payload = response.json()
        self.assertEqual("ollama", payload["mode"])
        self.assertIn("約24.70時間", payload["text"])

    def test_report_excel_ai_sheet_shows_generation_mode(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time",
                    "C001,Submit,2024-01-01 09:00:00",
                    "C001,Done,2024-01-01 10:00:00",
                ]
            ),
            analysis_keys=["frequency"],
        )

        with mock.patch(
            "app.main.build_excel_ai_summary",
            return_value={
                "title": "分析コメント",
                "mode": "ollama",
                "provider": "Ollama",
                "generated_at": "2026-04-24T00:00:00+00:00",
                "period": "2024-01-01 09:00 〜 2024-01-01 10:00",
                "text": "AI generated text",
                "highlights": [],
                "recommended_actions": [],
                "note": "",
            },
        ):
            response = self.client.get(f"/api/runs/{run_id}/report-excel?analysis_key=frequency")

        self.assertEqual(200, response.status_code)
        workbook = load_workbook(BytesIO(response.content))
        ai_sheet = workbook["分析コメント"]
        generation_mode_row = self.find_row_by_value(ai_sheet, "生成モード")
        self.assertEqual("AI生成（Ollama）", ai_sheet.cell(row=generation_mode_row, column=2).value)

    def test_pattern_report_excel_adds_links_and_compact_summary_routes(self):
        activities = [f"Step{i:02d}" for i in range(1, 13)]
        csv_lines = ["case_id,activity,start_time"]
        for case_id in ("C001", "C002"):
            for index, activity in enumerate(activities):
                csv_lines.append(f"{case_id},{activity},2024-01-01 {9 + index:02d}:00:00")
        run_id = self.analyze_uploaded_csv("\n".join(csv_lines), analysis_keys=["pattern"])

        with mock.patch("app.main.request_ollama_insights_text", return_value="pattern ai"):
            response = self.client.get(
                f"/api/runs/{run_id}/report-excel?analysis_key=pattern&pattern_display_limit=1"
            )

        self.assertEqual(200, response.status_code)
        workbook = load_workbook(BytesIO(response.content), data_only=False)
        summary_sheet = workbook["パターンサマリー"]
        candidate_count_row = self.find_row_by_value(summary_sheet, "改善候補パターン数")
        self.assertIsNotNone(summary_sheet.cell(row=candidate_count_row, column=2).value)

        top_table_row = self.find_row_by_value(summary_sheet, "上位10パターン")
        header_row = top_table_row + 2
        headers = [
            summary_sheet.cell(row=header_row, column=column_index).value
            for column_index in range(1, summary_sheet.max_column + 1)
        ]
        link_column = headers.index("詳細リンク") + 1
        pattern_column = headers.index("パターン") + 1
        self.assertTrue(str(summary_sheet.cell(row=header_row + 1, column=link_column).value).startswith("=HYPERLINK"))
        self.assertIn("…", str(summary_sheet.cell(row=header_row + 1, column=pattern_column).value))

        detail_sheet = workbook["処理順パターン分析"]
        detail_table_row = self.find_row_by_value(detail_sheet, "処理順パターン分析")
        detail_header_row = detail_table_row + 2
        detail_headers = [
            detail_sheet.cell(row=detail_header_row, column=column_index).value
            for column_index in range(1, detail_sheet.max_column + 1)
        ]
        detail_pattern_column = detail_headers.index("パターン") + 1
        full_pattern = str(detail_sheet.cell(row=detail_header_row + 1, column=detail_pattern_column).value)
        self.assertNotIn("…", full_pattern)
        self.assertIn("Step12", full_pattern)
