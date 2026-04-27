from tests.web_app_test_base import *

class WebAppApiTestCase(WebAppTestCaseBase):
    def test_detail_script_is_served(self):
        response = self.client.get("/static/detail.js")

        self.assertEqual(200, response.status_code)
        self.assertIn("renderDetailPage", response.text)

    def test_index_page_hides_top_excel_export_ui(self):
        response = self.client.get("/")

        self.assertEqual(200, response.status_code)
        self.assertIn('<select name="case_id_column"', response.text)
        self.assertIn('<select name="activity_column"', response.text)
        self.assertIn('<select name="timestamp_column"', response.text)
        self.assertNotIn('id="export-excel-toggle"', response.text)
        self.assertNotIn("Excel ZIP", response.text)
        self.assertNotIn('id="select-output-directory-button"', response.text)
        self.assertIn("/static/css/layout.css?v=", response.text)
        self.assertIn("/static/css/components.css?v=", response.text)
        self.assertIn("/static/app.js?v=", response.text)

    def test_analysis_detail_page_uses_versioned_static_assets(self):
        response = self.client.get("/analysis/pattern")

        self.assertEqual(200, response.status_code)
        self.assertIn("/static/css/layout.css?v=", response.text)
        self.assertIn("/static/css/components.css?v=", response.text)
        self.assertIn("/static/detail.js?v=", response.text)

    def test_analysis_detail_api_returns_full_rows(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["frequency", "transition", "pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        detail_response = self.client.get(f"/api/runs/{run_id}/analyses/frequency")
        self.assertEqual(200, detail_response.status_code)

        payload = detail_response.json()
        frequency_analysis = payload["analyses"]["frequency"]
        dashboard = payload["dashboard"]
        impact = payload["impact"]
        insights = payload["insights"]

        self.assertGreater(frequency_analysis["row_count"], 0)
        self.assertEqual(frequency_analysis["row_count"], len(frequency_analysis["rows"]))
        self.assertTrue(dashboard["has_data"])
        self.assertGreater(dashboard["total_cases"], 0)
        self.assertGreater(dashboard["total_records"], 0)
        self.assertGreater(dashboard["activity_type_count"], 0)
        self.assertIn("top10_variant_coverage_pct", dashboard)
        self.assertIn("top_bottleneck_transition_label", dashboard)
        self.assertTrue(impact["has_data"])
        self.assertTrue(impact["rows"])
        self.assertIn("impact_score", impact["rows"][0])
        self.assertEqual("rule_based", insights["mode"])
        self.assertTrue(insights["has_data"])
        self.assertGreaterEqual(len(insights["items"]), 3)
        self.assertTrue(any(item["id"] == "top_activity" for item in insights["items"]))

        pattern_detail_response = self.client.get(f"/api/runs/{run_id}/analyses/pattern")
        self.assertEqual(200, pattern_detail_response.status_code)
        pattern_payload = pattern_detail_response.json()
        pattern_insights = pattern_payload["insights"]
        self.assertTrue(any(item["id"] == "top_pattern" for item in pattern_insights["items"]))

    def test_analysis_detail_api_supports_row_limit(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["frequency", "transition", "pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        detail_response = self.client.get(f"/api/runs/{run_id}/analyses/pattern?row_limit=2")
        self.assertEqual(200, detail_response.status_code)

        payload = detail_response.json()
        pattern_analysis = payload["analyses"]["pattern"]

        self.assertEqual(2, pattern_analysis["returned_row_count"])
        self.assertEqual(0, pattern_analysis["row_offset"])
        self.assertGreaterEqual(pattern_analysis["row_count"], pattern_analysis["returned_row_count"])
        self.assertEqual(2, len(pattern_analysis["rows"]))

    def test_analysis_detail_api_supports_row_offset_pagination_metadata(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["pattern"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        detail_response = self.client.get(
            f"/api/runs/{run_id}/analyses/pattern?row_limit=1&row_offset=1"
        )
        self.assertEqual(200, detail_response.status_code)

        payload = detail_response.json()
        pattern_analysis = payload["analyses"]["pattern"]

        self.assertEqual(1, pattern_analysis["returned_row_count"])
        self.assertEqual(1, pattern_analysis["row_offset"])
        self.assertEqual(2, pattern_analysis["page_start_row_number"])
        self.assertEqual(2, pattern_analysis["page_end_row_number"])
        self.assertTrue(pattern_analysis["has_previous_page"])
        self.assertTrue(pattern_analysis["has_next_page"])
        self.assertEqual(0, pattern_analysis["previous_row_offset"])
        self.assertEqual(2, pattern_analysis["next_row_offset"])

    def test_analysis_detail_api_supports_deferred_supplement_payload(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["frequency"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        detail_response = self.client.get(
            f"/api/runs/{run_id}/analyses/frequency"
            "?include_dashboard=false"
            "&include_impact=false"
            "&include_root_cause=false"
            "&include_insights=false"
        )
        self.assertEqual(200, detail_response.status_code)

        payload = detail_response.json()
        self.assertIsNone(payload["dashboard"])
        self.assertIsNone(payload["impact"])
        self.assertIsNone(payload["root_cause"])
        self.assertIsNone(payload["insights"])
        self.assertCountEqual(
            ["dashboard", "impact", "root_cause", "insights"],
            payload["deferred_sections"],
        )

    def test_analysis_detail_api_deferred_payload_skips_unfiltered_dataframe_copy(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={"analysis_keys": ["frequency"]},
        )
        self.assertEqual(200, analyze_response.status_code)

        run_id = analyze_response.json()["run_id"]
        with mock.patch("app.main.filter_prepared_df", side_effect=AssertionError("filter_prepared_df should not run")):
            detail_response = self.client.get(
                f"/api/runs/{run_id}/analyses/frequency"
                "?include_dashboard=false"
                "&include_impact=false"
                "&include_root_cause=false"
                "&include_insights=false"
            )

        self.assertEqual(200, detail_response.status_code)

    def test_excel_archive_api_returns_zip_binary(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={
                "analysis_keys": ["frequency", "transition", "pattern"],
                "export_excel": "on",
            },
        )
        self.assertEqual(200, analyze_response.status_code)

        payload = analyze_response.json()
        self.assertIsNone(payload["analyses"]["frequency"]["excel_file"])

        run_id = payload["run_id"]
        with mock.patch(
            "app.main.request_ollama_insights_text",
            side_effect=app_main.httpx.ConnectError("offline"),
        ):
            excel_response = self.client.get(f"/api/runs/{run_id}/excel-archive")

        self.assertEqual(200, excel_response.status_code)
        self.assertEqual(
            "application/zip",
            excel_response.headers["content-type"],
        )
        self.assertIn("attachment;", excel_response.headers["content-disposition"])
        self.assertIn("filename*=UTF-8''", excel_response.headers["content-disposition"])
        self.assertIn("%E5%85%A8%E5%88%86%E6%9E%90%E3%83%AC%E3%83%9D%E3%83%BC%E3%83%88.zip", excel_response.headers["content-disposition"])
        self.assertTrue(excel_response.content.startswith(b"PK"))

        with ZipFile(BytesIO(excel_response.content)) as archive_file:
            archive_names = set(archive_file.namelist())
            self.assertEqual(4, len(archive_names))
            self.assertTrue(all(name.endswith(".xlsx") for name in archive_names))
            self.assertIn("ログ診断.xlsx", archive_names)
            self.assertIn("頻度分析.xlsx", archive_names)
            self.assertIn("前後処理分析.xlsx", archive_names)
            self.assertIn("処理順パターン分析.xlsx", archive_names)

    def test_excel_archive_api_includes_error_log_when_some_analyses_are_missing(self):
        run_id = self.analyze_uploaded_csv(
            self.build_duckdb_validation_csv(),
            analysis_keys=["frequency"],
        )

        with mock.patch(
            "app.main.request_ollama_insights_text",
            side_effect=app_main.httpx.ConnectError("offline"),
        ):
            excel_response = self.client.get(f"/api/runs/{run_id}/excel-archive")

        self.assertEqual(200, excel_response.status_code)
        with ZipFile(BytesIO(excel_response.content)) as archive_file:
            archive_names = set(archive_file.namelist())
            self.assertIn("ログ診断.xlsx", archive_names)
            self.assertIn("頻度分析.xlsx", archive_names)
            self.assertIn("エラーログ.txt", archive_names)
            error_text = archive_file.read("エラーログ.txt").decode("utf-8")
            self.assertIn("前後処理分析", error_text)
            self.assertIn("処理順パターン分析", error_text)


    def test_report_excel_export_api_returns_workbook(self):
        run_id = self.analyze_uploaded_csv(
            "\n".join(
                [
                    "case_id,activity,start_time,group_a,group_b,group_c",
                    "C001,Submit,2024-01-01 09:00:00,Sales,Web,A",
                    "C001,Approve,2024-01-02 09:00:00,Sales,Web,A",
                    "C002,Submit,2024-01-03 09:00:00,Sales,API,A",
                    "C002,Approve,2024-01-04 09:00:00,Sales,API,A",
                    "C003,Submit,2024-01-01 10:00:00,HR,Mail,B",
                    "C003,Reject,2024-01-02 10:00:00,HR,Mail,B",
                ]
            ),
            extra_data={
                "filter_column_1": "group_a",
                "filter_column_2": "group_b",
                "filter_column_3": "group_c",
            },
        )

        with mock.patch(
            "app.main.build_excel_ai_summary",
            return_value={
                "title": "分析コメント",
                "mode": "ollama",
                "provider": "Ollama (qwen2.5:7b)",
                "generated_at": "2026-04-02T00:00:00+00:00",
                "period": "2024-01-01 09:00 〜 2024-01-04 09:00",
                "text": "【全体傾向】\nテスト用の分析コメントです。",
                "highlights": [
                    "Submit が中心のルートです。",
                    "Approve の前後を重点確認してください。",
                ],
                "recommended_actions": [
                    "Submit の前後処理を確認し、遷移パターンの改善ポイントを特定してください。",
                    "Approve の処理時間をケース別にドリルダウンで確認してください。",
                ],
                "note": "ローカルLLMで生成した解説を掲載しています。",
            },
        ):
            response = self.client.get(
                f"/api/runs/{run_id}/report-excel"
                "?analysis_key=pattern"
                "&pattern_display_limit=10"
                "&filter_value_1=Sales"
                "&selected_transition_key=Submit__TO__Approve"
                "&case_id=C001"
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response.headers["content-type"],
        )
        self.assertIn(
            quote("custom_log_処理順パターン分析レポート.xlsx"),
            response.headers["content-disposition"],
        )

        workbook = load_workbook(BytesIO(response.content))
        self.assertEqual(
            ["サマリー", "分析コメント", "結論サマリー", "サマリーダッシュボード", "パターンサマリー", "処理順パターン分析", "パターン01詳細", "ドリルダウン", "ケース追跡"],
            self.visible_sheetnames(workbook),
        )
        summary_sheet = workbook["\u30b5\u30de\u30ea\u30fc"]
        self.assertEqual("\u30b5\u30de\u30ea\u30fc", summary_sheet["A1"].value)
        summary_pairs = self.summary_section_pairs(summary_sheet)
        self.assertNotIn("実行ID", summary_pairs)
        self.assertEqual("pattern", summary_pairs["分析種別"])
        self.assertEqual("処理順パターン分析", summary_pairs["分析名"])
        self.assertIn("Sales", str(summary_pairs["適用条件"]))
        self.assertEqual("2024-01-01 09:00 〜 2024-01-04 09:00", summary_pairs["分析期間"])
        self.assertEqual("group_a、group_b、group_c", summary_pairs["グルーピング条件"])
        self.assertEqual("Submit → Approve", summary_pairs["選択中遷移"])
        self.assertEqual("C001", summary_pairs["選択中ケースID"])
        applied_filters_row = self.find_row_by_value(summary_sheet, "適用条件")
        self.assertEqual("", summary_sheet.cell(row=applied_filters_row + 1, column=1).value or "")
        filter_note = str(summary_sheet.cell(row=applied_filters_row + 1, column=2).value)
        self.assertIn("※ 適用条件の種類:", filter_note)
        self.assertIn("期間フィルター: 開始日 / 終了日", filter_note)
        self.assertIn("グループ/カテゴリーフィルター①②③", filter_note)
        self.assertIn("アクティビティフィルター: 特定アクティビティを含む/除外", filter_note)
        self.assertEqual("分析期間", summary_sheet.cell(row=applied_filters_row + 2, column=1).value)
        grouping_row = self.find_row_by_value(summary_sheet, "グルーピング条件")
        self.assertEqual("", summary_sheet.cell(row=grouping_row + 1, column=1).value or "")
        grouping_note = str(summary_sheet.cell(row=grouping_row + 1, column=2).value)
        self.assertIn("カラムを選択し値を未選択にすると", grouping_note)
        self.assertIn("グルーピング軸（比較用）", grouping_note)
        summary_values = [summary_sheet.cell(row=row_index, column=1).value for row_index in range(1, summary_sheet.max_row + 1)]
        self.assertIn("主要KPI", summary_values)
        self.assertIn("分析ハイライト", summary_values)
        self.assertIn("グループ別比較", summary_values)

        group_table_row = self.find_row_by_value(summary_sheet, "グループ別比較")
        group_headers = [
            summary_sheet.cell(row=group_table_row + 2, column=column_index).value
            for column_index in range(1, 11)
        ]
        self.assertEqual(
            [
                "グルーピング軸",
                "値",
                "ケース数",
                "ケース比率(%)",
                "イベント数",
                "イベント比率(%)",
                "平均所要時間(分)",
                "中央値所要時間(分)",
                "最大所要時間(分)",
                "合計所要時間(分)",
            ],
            group_headers,
        )
        self.assertEqual("全体", summary_sheet.cell(row=group_table_row + 3, column=2).value)

        ai_sheet = workbook["分析コメント"]
        self.assertEqual("分析コメント", ai_sheet["A1"].value)
        self.assertEqual("対象分析", ai_sheet["A4"].value)
        self.assertEqual("\u51e6\u7406\u9806\u30d1\u30bf\u30fc\u30f3\u5206\u6790", ai_sheet["B4"].value)
        analysis_premise_row = self.find_row_by_value(ai_sheet, "分析前提")
        explanation_row = self.find_row_by_value(ai_sheet, "解説本文")
        recommended_actions_row = self.find_row_by_value(ai_sheet, "推奨アクション")
        terminology_row = self.find_row_by_value(ai_sheet, "用語説明")
        self.assertLess(analysis_premise_row, explanation_row)
        self.assertLess(explanation_row, recommended_actions_row)
        self.assertLess(recommended_actions_row, terminology_row)
        self.assertIn("所要時間は、同一ケース内で", str(ai_sheet.cell(row=analysis_premise_row + 1, column=1).value))
        self.assertIn("テスト用の分析コメント", str(ai_sheet.cell(row=explanation_row + 2, column=1).value))
        self.assertTrue(str(ai_sheet.cell(row=recommended_actions_row, column=1).fill.fgColor.rgb).endswith("E8EDF2"))
        self.assertIn(
            "Submit",
            str(ai_sheet.cell(row=recommended_actions_row + 1, column=2).value or ""),
        )
        self.assertEqual("用語", ai_sheet.cell(row=terminology_row + 1, column=1).value)
        self.assertEqual("説明", ai_sheet.cell(row=terminology_row + 1, column=2).value)
        self.assertEqual("ケース", ai_sheet.cell(row=terminology_row + 2, column=1).value)
        self.assertTrue(str(ai_sheet.cell(row=analysis_premise_row, column=1).fill.fgColor.rgb).endswith("E8EDF2"))
        self.assertTrue(str(ai_sheet.cell(row=terminology_row, column=1).fill.fgColor.rgb).endswith("F0F0F0"))
        ai_sheet_values = [
            ai_sheet.cell(row=row_index, column=1).value
            for row_index in range(1, ai_sheet.max_row + 1)
        ]
        self.assertNotIn("補足・免責事項", ai_sheet_values)
        self.assertTrue(
            str(ai_sheet.cell(row=terminology_row + 2, column=1).fill.fgColor.rgb).endswith("F7F7F7")
        )
        self.assertTrue(
            str(ai_sheet.cell(row=terminology_row + 2, column=1).border.left.color.rgb).endswith("D0D0D0")
        )
        self.assertNotIn("\u983b\u5ea6\u5206\u6790", workbook.sheetnames)
        self.assertNotIn("\u30dc\u30c8\u30eb\u30cd\u30c3\u30af\u5206\u6790", workbook.sheetnames)
        self.assertNotIn("\u6539\u5584\u30a4\u30f3\u30d1\u30af\u30c8\u5206\u6790", workbook.sheetnames)

        conclusion_sheet = workbook["\u7d50\u8ad6\u30b5\u30de\u30ea\u30fc"]
        self.assertEqual("結論サマリー", conclusion_sheet["A1"].value)
        self.assertEqual("全体要約", conclusion_sheet["A4"].value)
        conclusion_values = [
            conclusion_sheet.cell(row=row_index, column=1).value
            for row_index in range(1, conclusion_sheet.max_row + 1)
        ]
        self.assertIn("問題点3つ", conclusion_values)
        self.assertGreaterEqual(len(conclusion_sheet._charts), 2)

        dashboard_sheet = workbook["\u30b5\u30de\u30ea\u30fc\u30c0\u30c3\u30b7\u30e5\u30dc\u30fc\u30c9"]
        self.assertEqual("サマリーダッシュボード", dashboard_sheet["A1"].value)
        dashboard_values = [
            dashboard_sheet.cell(row=row_index, column=1).value
            for row_index in range(1, dashboard_sheet.max_row + 1)
        ]
        self.assertIn("改善優先TOP3", dashboard_values)
        self.assertIn("問題点", dashboard_values)
        self.assertEqual(0, len(dashboard_sheet._charts))
        with ZipFile(BytesIO(response.content)) as archive:
            chart_xml_documents = {
                name: archive.read(name).decode("utf-8")
                for name in archive.namelist()
                if name.startswith("xl/charts/chart")
            }
        self.assertGreaterEqual(len(chart_xml_documents), 2)
        self.assertIn("<cat><strRef>", chart_xml_documents["xl/charts/chart1.xml"])
        self.assertIn("<cat><strRef>", chart_xml_documents["xl/charts/chart2.xml"])
        self.assertNotIn("<cat><numRef>", chart_xml_documents["xl/charts/chart1.xml"])
        self.assertNotIn("<cat><numRef>", chart_xml_documents["xl/charts/chart2.xml"])
        self.assertIn("showPercent val=\"1\"", chart_xml_documents["xl/charts/chart1.xml"])
        self.assertIn("平均所要時間の比較（長い順）", chart_xml_documents["xl/charts/chart2.xml"])
        self.assertIn("showVal val=\"1\"", chart_xml_documents["xl/charts/chart2.xml"])
        self.assertIn("<orientation val=\"maxMin\"/>", chart_xml_documents["xl/charts/chart2.xml"])
        self.assertIn("<crosses val=\"autoZero\"/>", chart_xml_documents["xl/charts/chart2.xml"])
        conclusion_chart_anchors = [
            (chart.anchor._from.col, chart.anchor._from.row)
            for chart in conclusion_sheet._charts
        ]
        self.assertEqual(2, len(conclusion_chart_anchors))
        self.assertEqual(conclusion_chart_anchors[0][1], conclusion_chart_anchors[1][1])
        self.assertGreater(conclusion_chart_anchors[1][0], conclusion_chart_anchors[0][0])

        pattern_summary_sheet = workbook["\u30d1\u30bf\u30fc\u30f3\u30b5\u30de\u30ea\u30fc"]
        self.assertEqual("\u30d1\u30bf\u30fc\u30f3\u30b5\u30de\u30ea\u30fc", pattern_summary_sheet["A1"].value)
        self.assertEqual("上位3パターン累積カバー率(%)", pattern_summary_sheet["A4"].value)
        pattern_summary_values = [
            pattern_summary_sheet.cell(row=row_index, column=1).value
            for row_index in range(1, pattern_summary_sheet.max_row + 1)
        ]
        self.assertIn("上位10パターン", pattern_summary_values)
        self.assertIn("カバー率要約", pattern_summary_values)
        self.assertIn("最短処理パターン", pattern_summary_values)
        self.assertNotIn("上位3パターン", pattern_summary_values)
        self.assertNotIn("パターン比較（上位10件）", pattern_summary_values)
        self.assertIn("要確認パターン一覧", pattern_summary_values)
        self.assertIn("改善対象パターンTOP3", pattern_summary_values)

        pattern_sheet = workbook["\u51e6\u7406\u9806\u30d1\u30bf\u30fc\u30f3\u5206\u6790"]
        self.assertEqual("\u51e6\u7406\u9806\u30d1\u30bf\u30fc\u30f3\u5206\u6790", pattern_sheet["A1"].value)
        pattern_headers = [pattern_sheet.cell(row=3, column=column_index).value for column_index in range(1, pattern_sheet.max_column + 1)]
        self.assertIn("\u9806\u4f4d", pattern_headers)
        self.assertIn("パターン / バリアント", pattern_headers)
        self.assertIn("繰り返し", pattern_headers)
        self.assertIn("繰り返し回数", pattern_headers)
        self.assertIn("繰り返し率(%)", pattern_headers)
        self.assertIn("繰り返し率区分", pattern_headers)
        self.assertIn("確認区分", pattern_headers)
        self.assertIn("平均所要時間差分(分)", pattern_headers)
        self.assertIn("改善優先度スコア", pattern_headers)
        self.assertIn("全体影響度(%)", pattern_headers)
        self.assertIn("最短処理", pattern_headers)
        self.assertIn("標準偏差ケース所要時間(分)", pattern_headers)
        self.assertIn("75%点ケース所要時間(分)", pattern_headers)
        self.assertIn("90%点ケース所要時間(分)", pattern_headers)
        self.assertIn("95%点ケース所要時間(分)", pattern_headers)
        self.assertIn("簡易コメント", pattern_headers)
        self.assertIn("ステップ数", pattern_headers)
        self.assertIn("繰り返しアクティビティ", pattern_headers)
        self.assertIn("パターン", pattern_headers)
        comment_column_index = pattern_headers.index("簡易コメント") + 1
        comment_column_letter = get_column_letter(comment_column_index)
        self.assertFalse(pattern_sheet[f"{comment_column_letter}4"].alignment.wrap_text)
        self.assertGreaterEqual(pattern_sheet.column_dimensions[comment_column_letter].width, 48)

        pattern_detail_sheet = workbook["\u30d1\u30bf\u30fc\u30f301\u8a73\u7d30"]
        self.assertEqual("Pattern #1 詳細", pattern_detail_sheet["A1"].value)
        self.assertEqual("項目", pattern_detail_sheet["A3"].value)
        pattern_detail_values = [cell for row in pattern_detail_sheet.iter_rows(values_only=True) for cell in row if cell]
        self.assertIn("ステップ別所要時間", pattern_detail_values)

        drilldown_sheet = workbook["\u30c9\u30ea\u30eb\u30c0\u30a6\u30f3"]
        self.assertEqual("\u9077\u79fb\u30c9\u30ea\u30eb\u30c0\u30a6\u30f3: Submit \u2192 Approve", drilldown_sheet["A1"].value)
        self.assertEqual("\u30b1\u30fc\u30b9ID", drilldown_sheet["A3"].value)
        self.assertEqual("C001", drilldown_sheet["A4"].value)

        case_trace_sheet = workbook["\u30b1\u30fc\u30b9\u8ffd\u8de1"]
        self.assertEqual("\u30b1\u30fc\u30b9\u6982\u8981", case_trace_sheet["A1"].value)
        self.assertEqual("\u901a\u904e\u30a4\u30d9\u30f3\u30c8", case_trace_sheet["A10"].value)

