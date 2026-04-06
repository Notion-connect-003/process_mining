from io import BytesIO
import unittest
from unittest import mock
from urllib.parse import quote
from zipfile import ZipFile

import inspect
import httpx
from fastapi.testclient import TestClient
from openpyxl import load_workbook

import web_app


if "app" not in inspect.signature(httpx.Client.__init__).parameters:
    _original_httpx_client_init = httpx.Client.__init__

    def _compat_httpx_client_init(self, *args, app=None, **kwargs):
        return _original_httpx_client_init(self, *args, **kwargs)

    httpx.Client.__init__ = _compat_httpx_client_init


class WebAppTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(web_app.app)

    def analyze_uploaded_csv(self, csv_text, analysis_keys=None, extra_data=None):
        response = self.client.post(
            "/api/analyze",
            data={
                "analysis_keys": analysis_keys or ["frequency", "pattern"],
                **(extra_data or {}),
            },
            files={"csv_file": ("custom_log.csv", BytesIO(csv_text.encode("utf-8")), "text/csv")},
        )
        self.assertEqual(200, response.status_code)
        return response.json()["run_id"]

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
        self.assertIn("/static/style.css?v=", response.text)
        self.assertIn("/static/app.js?v=", response.text)

    def test_analysis_detail_page_uses_versioned_static_assets(self):
        response = self.client.get("/analysis/pattern")

        self.assertEqual(200, response.status_code)
        self.assertIn("/static/style.css?v=", response.text)
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

    def test_excel_archive_api_returns_zip_binary(self):
        analyze_response = self.client.post(
            "/api/analyze",
            data={
                "analysis_keys": ["frequency", "transition"],
                "export_excel": "on",
            },
        )
        self.assertEqual(200, analyze_response.status_code)

        payload = analyze_response.json()
        self.assertIsNone(payload["analyses"]["frequency"]["excel_file"])

        run_id = payload["run_id"]
        excel_response = self.client.get(f"/api/runs/{run_id}/excel-archive")

        self.assertEqual(200, excel_response.status_code)
        self.assertEqual(
            "application/zip",
            excel_response.headers["content-type"],
        )
        self.assertIn("attachment;", excel_response.headers["content-disposition"])
        self.assertTrue(excel_response.content.startswith(b"PK"))

        with ZipFile(BytesIO(excel_response.content)) as archive_file:
            archive_names = set(archive_file.namelist())
            self.assertEqual(2, len(archive_names))
            self.assertTrue(all(name.endswith(".xlsx") for name in archive_names))
            self.assertIn("sample_event_log_頻度分析.xlsx", archive_names)
            self.assertIn("sample_event_log_前後処理分析.xlsx", archive_names)


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
            "web_app.build_excel_ai_summary",
            return_value={
                "title": "AI解説",
                "mode": "ollama",
                "provider": "Ollama (qwen2.5:7b)",
                "generated_at": "2026-04-02T00:00:00+00:00",
                "period": "2024-01-01 09:00 〜 2024-01-04 09:00",
                "text": "【全体サマリー】\nテスト用のAI解説です。",
                "highlights": [
                    "Submit が中心のルートです。",
                    "Approve の前後を重点確認してください。",
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
            ["\u30b5\u30de\u30ea\u30fc", "AI\u89e3\u8aac", "\u51e6\u7406\u9806\u30d1\u30bf\u30fc\u30f3\u5206\u6790", "Pattern01\u8a73\u7d30", "\u30c9\u30ea\u30eb\u30c0\u30a6\u30f3", "\u30b1\u30fc\u30b9\u8ffd\u8de1"],
            workbook.sheetnames,
        )
        summary_sheet = workbook["\u30b5\u30de\u30ea\u30fc"]
        self.assertEqual("\u30b5\u30de\u30ea\u30fc", summary_sheet["A1"].value)
        self.assertEqual("\u5b9f\u884cID", summary_sheet["A4"].value)
        self.assertEqual(run_id, summary_sheet["B4"].value)
        self.assertEqual("\u9069\u7528\u30d5\u30a3\u30eb\u30bf\u6761\u4ef6", summary_sheet["A12"].value)
        self.assertIn("Sales", str(summary_sheet["B12"].value))
        self.assertEqual("主要KPI", summary_sheet["A18"].value)
        self.assertEqual("AIハイライト", summary_sheet["A27"].value)

        ai_sheet = workbook["AI\u89e3\u8aac"]
        self.assertEqual("AI\u89e3\u8aac", ai_sheet["A1"].value)
        self.assertEqual("対象分析", ai_sheet["A4"].value)
        self.assertEqual("\u51e6\u7406\u9806\u30d1\u30bf\u30fc\u30f3\u5206\u6790", ai_sheet["B4"].value)
        self.assertEqual("解説本文", ai_sheet["A10"].value)
        self.assertIn("テスト用のAI解説", str(ai_sheet["A11"].value))
        self.assertEqual("要点一覧", ai_sheet["A13"].value)
        self.assertNotIn("\u983b\u5ea6\u5206\u6790", workbook.sheetnames)
        self.assertNotIn("\u30dc\u30c8\u30eb\u30cd\u30c3\u30af\u5206\u6790", workbook.sheetnames)
        self.assertNotIn("\u6539\u5584\u30a4\u30f3\u30d1\u30af\u30c8\u5206\u6790", workbook.sheetnames)

        pattern_sheet = workbook["\u51e6\u7406\u9806\u30d1\u30bf\u30fc\u30f3\u5206\u6790"]
        self.assertEqual("\u51e6\u7406\u9806\u30d1\u30bf\u30fc\u30f3\u5206\u6790", pattern_sheet["A1"].value)
        self.assertEqual("\u9806\u4f4d", pattern_sheet["A3"].value)
        self.assertEqual("Pattern / Variant", pattern_sheet["B3"].value)
        self.assertFalse(pattern_sheet["F4"].alignment.wrap_text)
        self.assertGreaterEqual(pattern_sheet.column_dimensions["F"].width, 80)

        pattern_detail_sheet = workbook["Pattern01\u8a73\u7d30"]
        self.assertEqual("Pattern #1 詳細", pattern_detail_sheet["A1"].value)
        self.assertEqual("項目", pattern_detail_sheet["A3"].value)
        self.assertEqual("ステップ別処理時間", pattern_detail_sheet["A15"].value)

        drilldown_sheet = workbook["\u30c9\u30ea\u30eb\u30c0\u30a6\u30f3"]
        self.assertEqual("\u9077\u79fb\u30c9\u30ea\u30eb\u30c0\u30a6\u30f3: Submit \u2192 Approve", drilldown_sheet["A1"].value)
        self.assertEqual("\u30b1\u30fc\u30b9ID", drilldown_sheet["A3"].value)
        self.assertEqual("C001", drilldown_sheet["A4"].value)

        case_trace_sheet = workbook["\u30b1\u30fc\u30b9\u8ffd\u8de1"]
        self.assertEqual("\u30b1\u30fc\u30b9\u6982\u8981", case_trace_sheet["A1"].value)
        self.assertEqual("\u901a\u904e\u30a4\u30d9\u30f3\u30c8", case_trace_sheet["A10"].value)

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
            "web_app.request_ollama_insights_text",
            side_effect=web_app.httpx.ConnectError("offline"),
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
        ai_sheet = workbook["AI\u89e3\u8aac"]
        self.assertEqual("ルールベース要約", ai_sheet["B5"].value)
        self.assertIn("Ollama が起動していない", str(ai_sheet["B8"].value))
        self.assertTrue(str(ai_sheet["A11"].value).strip())

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
            "web_app.build_excel_ai_summary",
            return_value={
                "title": "AI解説",
                "mode": "ollama",
                "provider": "Ollama (qwen2.5:7b)",
                "generated_at": "2026-04-02T00:00:00+00:00",
                "period": "2024-01-01 09:00 〜 2024-01-04 09:00",
                "text": "前後処理向けのAI解説です。",
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
            ["\u30b5\u30de\u30ea\u30fc", "AI\u89e3\u8aac", "\u524d\u5f8c\u51e6\u7406\u5206\u6790", "\u30dc\u30c8\u30eb\u30cd\u30c3\u30af\u5206\u6790", "\u6539\u5584\u30a4\u30f3\u30d1\u30af\u30c8\u5206\u6790", "\u30c9\u30ea\u30eb\u30c0\u30a6\u30f3"],
            workbook.sheetnames,
        )
        self.assertNotIn("\u983b\u5ea6\u5206\u6790", workbook.sheetnames)
        self.assertNotIn("\u51e6\u7406\u9806\u30d1\u30bf\u30fc\u30f3\u5206\u6790", workbook.sheetnames)
        self.assertNotIn("Variant\u5206\u6790", workbook.sheetnames)
        transition_sheet = workbook["\u524d\u5f8c\u51e6\u7406\u5206\u6790"]
        self.assertEqual("\u524d\u5f8c\u51e6\u7406\u5206\u6790", transition_sheet["A1"].value)
        self.assertEqual("\u9806\u4f4d", transition_sheet["A3"].value)

        bottleneck_sheet = workbook["\u30dc\u30c8\u30eb\u30cd\u30c3\u30af\u5206\u6790"]
        self.assertEqual("Activityボトルネック", bottleneck_sheet["A1"].value)
        self.assertEqual("平均処理時間", bottleneck_sheet["E3"].value)
        transition_title_row = next(
            row_index
            for row_index in range(1, bottleneck_sheet.max_row + 1)
            if bottleneck_sheet.cell(row=row_index, column=1).value == "Transitionボトルネック"
        )
        self.assertEqual("Transitionボトルネック", bottleneck_sheet.cell(row=transition_title_row, column=1).value)
        self.assertEqual("平均処理時間", bottleneck_sheet.cell(row=transition_title_row + 2, column=5).value)

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
            "web_app.request_ollama_insights_text",
            return_value="頻度分析向けの AI 解説です。",
        ):
            generate_response = self.client.post(f"/api/runs/{run_id}/ai-insights/frequency")

        self.assertEqual(200, generate_response.status_code)
        generate_payload = generate_response.json()
        self.assertTrue(generate_payload["generated"])
        self.assertFalse(generate_payload["cached"])
        self.assertEqual("頻度分析", generate_payload["analysis_name"])
        self.assertEqual("頻度分析向けの AI 解説です。", generate_payload["text"])

        restored_response = self.client.get(f"/api/runs/{run_id}/ai-insights/frequency")
        self.assertEqual(200, restored_response.status_code)
        restored_payload = restored_response.json()
        self.assertTrue(restored_payload["generated"])
        self.assertTrue(restored_payload["cached"])
        self.assertEqual("頻度分析向けの AI 解説です。", restored_payload["text"])

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

        with mock.patch("web_app.request_ollama_insights_text", side_effect=_fake_ai):
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
        self.assertEqual(["\u30b5\u30de\u30ea\u30fc", "AI\u89e3\u8aac", "\u983b\u5ea6\u5206\u6790"], frequency_workbook.sheetnames)
        self.assertEqual(["\u30b5\u30de\u30ea\u30fc", "AI\u89e3\u8aac", "\u51e6\u7406\u9806\u30d1\u30bf\u30fc\u30f3\u5206\u6790", "Pattern01\u8a73\u7d30"], pattern_workbook.sheetnames)
        self.assertEqual("frequency ai", frequency_workbook["AI\u89e3\u8aac"]["A11"].value)
        self.assertEqual("pattern ai", pattern_workbook["AI\u89e3\u8aac"]["A11"].value)

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
            "web_app.request_ollama_insights_text",
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
            if sheet_name.startswith("Pattern") and sheet_name.endswith("詳細")
        ]
        self.assertEqual(20, len(detail_sheet_names))

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
            "web_app.create_pattern_flow_snapshot",
            wraps=web_app.create_pattern_flow_snapshot,
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
        self.assertEqual(["\u53d7\u4ed8", "\u78ba\u8a8d", "\u5b8c\u4e86"], payload["variants"][0]["activities"])
        self.assertEqual(3, payload["variants"][0]["activity_count"])
        self.assertEqual(0, payload["variants"][0]["pattern_index"])
        self.assertGreater(payload["variants"][0]["avg_case_duration_sec"], 0)
        self.assertEqual(2, payload["coverage"]["displayed_variant_count"])
        self.assertEqual(4, payload["coverage"]["covered_case_count"])
        self.assertEqual(0.6667, payload["coverage"]["ratio"])

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
        run_data = web_app.get_run_data(run_id)
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
        self.assertIn("Case ID / Activity / Timestamp", payload["error"])


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
        self.assertEqual("", payload["default_selection"]["case_id_column"])


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
        for col in (
            "平均時間(分)",
            "中央値時間(分)",
            "標準偏差(分)",
            "最小時間(分)",
            "最大時間(分)",
            "75%点(分)",
            "90%点(分)",
            "95%点(分)",
        ):
            self.assertIn(col, first_row, f"列 '{col}' が前後処理分析の結果に存在しません。")

    def test_pattern_analysis_has_std_and_percentile_columns(self):
        """処理順パターン分析の結果に標準偏差・P75/P90/P95 列が含まれること。"""
        run_id = self._run_sample_analysis(["pattern"])
        response = self.client.get(f"/api/runs/{run_id}/analyses/pattern")
        self.assertEqual(200, response.status_code)

        rows = response.json()["analyses"]["pattern"]["rows"]
        self.assertTrue(rows, "処理順パターン分析の結果行が空です。")

        first_row = rows[0]
        for col in (
            "標準偏差ケース時間(分)",
            "75%点ケース時間(分)",
            "90%点ケース時間(分)",
            "95%点ケース時間(分)",
        ):
            self.assertIn(col, first_row, f"列 '{col}' が処理順パターン分析の結果に存在しません。")

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
            self.assertEqual("-", row["標準偏差ケース時間(分)"],
                             f"パターン '{row.get('処理順パターン')}' の標準偏差が '-' ではありません。")


if __name__ == "__main__":
    unittest.main()




