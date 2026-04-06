from pathlib import Path
import unittest

import pandas as pd

from 共通スクリプト.analysis_service import (
    create_activity_case_drilldown,
    create_bottleneck_summary,
    create_case_trace_details,
    create_dashboard_summary,
    create_impact_summary,
    create_log_diagnostics,
    create_pattern_bottleneck_details,
    create_rule_based_insights,
    create_root_cause_summary,
    create_transition_case_drilldown,
    create_variant_flow_snapshot,
    create_variant_summary,
    filter_prepared_df,
    get_filter_options,
    normalize_filter_column_settings,
    normalize_filter_params,
)
from 共通スクリプト.data_loader import load_and_prepare_data, prepare_event_log
from 共通スクリプト.分析.前後処理分析.transition_analysis import create_transition_analysis
from 共通スクリプト.分析.処理順パターン分析.pattern_analysis import create_pattern_analysis
from 共通スクリプト.分析.頻度分析.frequency_analysis import create_frequency_analysis


ROOT_DIR = Path(__file__).resolve().parents[1]
SAMPLE_FILE = ROOT_DIR / "sample_event_log.csv"


class ProcessMiningTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.prepared_df = load_and_prepare_data(
            file_path=SAMPLE_FILE,
            case_id_column="case_id",
            activity_column="activity",
            timestamp_column="start_time",
        )

    def test_prepare_event_log_adds_analysis_columns(self):
        expected_columns = {
            "case_id",
            "activity",
            "timestamp",
            "start_time",
            "next_time",
            "duration_sec",
            "duration_min",
            "sequence_no",
            "event_count_in_case",
        }
        self.assertTrue(expected_columns.issubset(set(self.prepared_df.columns.tolist())))
        self.assertIn("end_time", self.prepared_df.columns.tolist())

        first_row = self.prepared_df.iloc[0]
        self.assertEqual("C001", first_row["case_id"])
        self.assertEqual("受付", first_row["activity"])
        self.assertEqual(120.0, first_row["duration_sec"])
        self.assertEqual(2.0, first_row["duration_min"])
        self.assertEqual(1, first_row["sequence_no"])
        self.assertEqual(4, first_row["event_count_in_case"])

        last_case_row = self.prepared_df[self.prepared_df["case_id"] == "C001"].iloc[-1]
        self.assertEqual(0.0, last_case_row["duration_sec"])
        self.assertEqual(last_case_row["start_time"], last_case_row["next_time"])

    def test_prepare_event_log_raises_when_required_column_is_missing(self):
        invalid_df = pd.DataFrame(
            [
                {"case_id": "C001", "activity": "受付"},
            ]
        )

        with self.assertRaisesRegex(ValueError, "入力CSVに必要な列がありません"):
            prepare_event_log(
                df=invalid_df,
                case_id_column="case_id",
                activity_column="activity",
                timestamp_column="start_time",
            )

    def test_frequency_analysis_returns_expected_summary(self):
        result = create_frequency_analysis(self.prepared_df)

        self.assertEqual(["確認", "受付", "完了", "承認", "差戻し"], result["activity"].tolist())

        top_row = result.iloc[0]
        self.assertEqual("確認", top_row["activity"])
        self.assertEqual(8, top_row["event_count"])
        self.assertEqual(6, top_row["case_count"])
        self.assertEqual(77.0, top_row["total_duration_min"])
        self.assertEqual(30.77, top_row["event_ratio_pct"])

    def test_transition_analysis_returns_expected_transitions(self):
        result = create_transition_analysis(self.prepared_df)

        first_row = result.iloc[0]
        self.assertEqual("受付", first_row["from_activity"])
        self.assertEqual("確認", first_row["to_activity"])
        self.assertEqual(6, first_row["transition_count"])
        self.assertEqual(6, first_row["case_count"])
        self.assertEqual(30.0, first_row["transition_ratio_pct"])

        confirm_to_completion = result[
            (result["from_activity"] == "確認") & (result["to_activity"] == "完了")
        ].iloc[0]
        self.assertEqual(2, confirm_to_completion["transition_count"])
        self.assertEqual(0.0, confirm_to_completion["to_total_duration_min"])

    def test_pattern_analysis_returns_expected_patterns(self):
        result = create_pattern_analysis(self.prepared_df)

        self.assertEqual(3, len(result))

        first_row = result.iloc[0]
        self.assertEqual("受付→確認→完了", first_row["pattern"])
        self.assertEqual(2, first_row["case_count"])
        self.assertEqual(6.5, first_row["avg_case_duration_min"])
        self.assertEqual(33.33, first_row["case_ratio_pct"])

    def test_pattern_bottleneck_details_returns_transition_metrics(self):
        pattern = "受付→確認→承認→完了"
        detail = create_pattern_bottleneck_details(self.prepared_df, pattern)

        self.assertEqual(pattern, detail["pattern"])
        self.assertEqual(2, detail["case_count"])
        self.assertEqual(33.33, detail["case_ratio_pct"])
        self.assertEqual(18.5, detail["avg_case_duration_min"])
        self.assertEqual(3, len(detail["step_metrics"]))
        self.assertEqual("確認", detail["bottleneck_transition"]["from_activity"])
        self.assertEqual("承認", detail["bottleneck_transition"]["to_activity"])
        self.assertEqual(10.0, detail["bottleneck_transition"]["avg_duration_min"])
        self.assertEqual(["C004", "C001"], [row["case_id"] for row in detail["case_examples"][:2]])

    def test_variant_summary_returns_ranked_variants(self):
        variants = create_variant_summary(self.prepared_df, limit=10)

        self.assertEqual(3, len(variants))
        self.assertEqual(1, variants[0]["variant_id"])
        self.assertEqual(["受付", "確認", "完了"], variants[0]["activities"])
        self.assertEqual(3, variants[0]["activity_count"])
        self.assertEqual(2, variants[0]["count"])
        self.assertEqual(0.3333, variants[0]["ratio"])
        self.assertGreater(variants[0]["avg_case_duration_sec"], 0)

    def test_bottleneck_summary_returns_ranked_activity_and_transition_rows(self):
        summary = create_bottleneck_summary(self.prepared_df, limit=3)

        self.assertEqual(3, len(summary["activity_bottlenecks"]))
        self.assertEqual(3, len(summary["transition_bottlenecks"]))

        top_activity = summary["activity_bottlenecks"][0]
        self.assertEqual("確認", top_activity["activity"])
        self.assertEqual(8, top_activity["count"])
        self.assertEqual(6, top_activity["case_count"])
        self.assertEqual(577.5, top_activity["avg_duration_sec"])
        self.assertEqual(0.16, top_activity["avg_duration_hours"])
        self.assertEqual(0.13, top_activity["median_duration_hours"])
        self.assertEqual(0.28, top_activity["max_duration_hours"])
        self.assertEqual("heat-5", summary["activity_heatmap"]["確認"]["heat_class"])

        top_transition = summary["transition_bottlenecks"][0]
        self.assertEqual("確認", top_transition["from_activity"])
        self.assertEqual("差戻し", top_transition["to_activity"])
        self.assertEqual("確認__TO__差戻し", top_transition["transition_key"])
        self.assertEqual(2, top_transition["count"])
        self.assertEqual(2, top_transition["case_count"])
        self.assertEqual(720.0, top_transition["avg_duration_sec"])
        self.assertEqual(0.2, top_transition["avg_duration_hours"])
        self.assertEqual(0.2, top_transition["median_duration_hours"])
        self.assertEqual(0.28, top_transition["max_duration_hours"])
        self.assertEqual("heat-5", summary["transition_heatmap"]["確認__TO__差戻し"]["heat_class"])

    def test_dashboard_summary_returns_overview_metrics(self):
        dashboard = create_dashboard_summary(
            self.prepared_df,
            variant_items=create_variant_summary(self.prepared_df, limit=10),
            bottleneck_summary=create_bottleneck_summary(self.prepared_df, limit=10),
        )
        case_durations = self.prepared_df.groupby("case_id")["duration_sec"].sum().astype(float)

        self.assertTrue(dashboard["has_data"])
        self.assertEqual(6, dashboard["total_cases"])
        self.assertEqual(len(self.prepared_df), dashboard["total_records"])
        self.assertEqual(self.prepared_df["activity"].nunique(), dashboard["activity_type_count"])
        self.assertEqual(round(float(case_durations.mean()), 2), dashboard["avg_case_duration_sec"])
        self.assertEqual(round(float(case_durations.median()), 2), dashboard["median_case_duration_sec"])
        self.assertEqual(round(float(case_durations.max()), 2), dashboard["max_case_duration_sec"])
        self.assertEqual(1.0, dashboard["top10_variant_coverage_ratio"])
        self.assertEqual("確認 → 差戻し", dashboard["top_bottleneck_transition_label"])
        self.assertEqual(720.0, dashboard["top_bottleneck_avg_wait_sec"])

    def test_rule_based_insights_returns_key_points(self):
        dashboard = create_dashboard_summary(
            self.prepared_df,
            variant_items=create_variant_summary(self.prepared_df, limit=10),
            bottleneck_summary=create_bottleneck_summary(self.prepared_df, limit=10),
        )
        bottleneck_summary = create_bottleneck_summary(self.prepared_df, limit=10)
        impact_summary = create_impact_summary(self.prepared_df, limit=10)
        insights = create_rule_based_insights(
            self.prepared_df,
            dashboard_summary=dashboard,
            bottleneck_summary=bottleneck_summary,
            impact_summary=impact_summary,
            max_items=5,
        )

        self.assertEqual("rule_based", insights["mode"])
        self.assertTrue(insights["has_data"])
        self.assertGreaterEqual(len(insights["items"]), 3)
        self.assertLessEqual(len(insights["items"]), 5)
        self.assertEqual("scope", insights["items"][0]["id"])
        self.assertIn("対象は", insights["items"][0]["text"])
        self.assertTrue(any(item["id"] == "top_impact_transition" for item in insights["items"]))

    def test_rule_based_insights_support_analysis_specific_content(self):
        frequency_rows = create_frequency_analysis(self.prepared_df).to_dict(orient="records")
        pattern_rows = create_pattern_analysis(self.prepared_df).to_dict(orient="records")

        frequency_insights = create_rule_based_insights(
            self.prepared_df,
            analysis_key="frequency",
            analysis_rows=frequency_rows,
            max_items=5,
        )
        pattern_insights = create_rule_based_insights(
            self.prepared_df,
            analysis_key="pattern",
            analysis_rows=pattern_rows,
            max_items=5,
        )

        self.assertTrue(any(item["id"] == "top_activity" for item in frequency_insights["items"]))
        self.assertTrue(any(item["id"] == "event_distribution" for item in frequency_insights["items"]))
        self.assertTrue(any(item["id"] == "top_pattern" for item in pattern_insights["items"]))
        self.assertTrue(any(item["id"] == "pattern_variability" for item in pattern_insights["items"]))

    def test_impact_summary_returns_ranked_transition_rows(self):
        impact = create_impact_summary(self.prepared_df, limit=10)

        self.assertTrue(impact["has_data"])
        self.assertGreaterEqual(impact["total_transition_count"], impact["returned_transition_count"])
        self.assertTrue(impact["rows"])

        top_row = impact["rows"][0]
        self.assertEqual(1, top_row["rank"])
        self.assertEqual("確認__TO__承認", top_row["transition_key"])
        self.assertEqual(4, top_row["case_count"])
        self.assertEqual(630.0, top_row["avg_duration_sec"])
        self.assertEqual(840.0, top_row["max_duration_sec"])
        self.assertGreater(top_row["wait_share_pct"], 0)
        self.assertEqual(2520.0, top_row["impact_score"])
        self.assertGreater(top_row["impact_share_pct"], 0)

    def test_root_cause_summary_returns_ranked_groups(self):
        raw_df = pd.DataFrame(
            [
                {"case_id": "C001", "activity": "受付", "start_time": "2024-01-01 09:00:00", "group_a": "営業"},
                {"case_id": "C001", "activity": "完了", "start_time": "2024-01-01 10:00:00", "group_a": "営業"},
                {"case_id": "C002", "activity": "受付", "start_time": "2024-01-01 09:00:00", "group_a": "経理"},
                {"case_id": "C002", "activity": "完了", "start_time": "2024-01-03 09:00:00", "group_a": "経理"},
                {"case_id": "C003", "activity": "受付", "start_time": "2024-01-01 09:00:00", "group_a": "IT"},
                {"case_id": "C003", "activity": "完了", "start_time": "2024-01-01 11:00:00", "group_a": "IT"},
            ]
        )
        prepared_df = prepare_event_log(
            df=raw_df,
            case_id_column="case_id",
            activity_column="activity",
            timestamp_column="start_time",
        )

        root_cause = create_root_cause_summary(
            prepared_df,
            filter_column_settings={
                "filter_value_1": {"column_name": "group_a", "label": "グループ/カテゴリー フィルター①"},
            },
            limit=10,
        )

        self.assertTrue(root_cause["has_data"])
        self.assertEqual(1, root_cause["configured_group_count"])
        self.assertEqual("group_a", root_cause["groups"][0]["column_name"])
        self.assertEqual("経理", root_cause["groups"][0]["rows"][0]["value"])
        self.assertEqual(1, root_cause["groups"][0]["rows"][0]["case_count"])
        self.assertEqual(33.33, root_cause["groups"][0]["rows"][0]["case_ratio_pct"])

    def test_transition_case_drilldown_returns_slowest_cases(self):
        rows = create_transition_case_drilldown(
            self.prepared_df,
            from_activity="確認",
            to_activity="差戻し",
            limit=5,
        )

        self.assertEqual(2, len(rows))
        self.assertEqual("C002", rows[0]["case_id"])
        self.assertEqual(1020.0, rows[0]["duration_sec"])
        self.assertEqual("17m 0s", rows[0]["duration_text"])
        self.assertEqual("C005", rows[1]["case_id"])
        self.assertEqual(420.0, rows[1]["duration_sec"])

    def test_activity_case_drilldown_returns_slowest_cases(self):
        rows = create_activity_case_drilldown(
            self.prepared_df,
            activity="確認",
            limit=5,
        )

        self.assertEqual(5, len(rows))
        self.assertEqual("C002", rows[0]["case_id"])
        self.assertEqual(1020.0, rows[0]["duration_sec"])
        self.assertEqual("17m 0s", rows[0]["duration_text"])
        self.assertEqual("差戻し", rows[0]["next_activity"])

    def test_variant_flow_snapshot_returns_single_variant_graph(self):
        snapshot = create_variant_flow_snapshot(
            self.prepared_df,
            "受付→確認→承認→完了",
            activity_percent=100,
            connection_percent=100,
        )

        self.assertEqual(1, snapshot["pattern_window"]["used_pattern_count"])
        self.assertTrue(snapshot["flow_data"]["nodes"])
        self.assertTrue(snapshot["flow_data"]["edges"])


    def test_pattern_flow_snapshot_filters_top_patterns_nodes_and_edges(self):
        import importlib.util

        module_path = ROOT_DIR / "共通スクリプト" / "analysis_service.py"
        spec = importlib.util.spec_from_file_location("analysis_service_local", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        pattern_rows = [
            {"ケース数": 90, "処理順パターン": "申請受付→内容確認→処理完了"},
            {"ケース数": 70, "処理順パターン": "申請受付→一次承認→処理完了"},
            {"ケース数": 50, "処理順パターン": "申請受付→差戻し→再提出→処理完了"},
            {"ケース数": 30, "処理順パターン": "申請受付→自動処理→処理完了"},
        ]

        snapshot = module.create_pattern_flow_snapshot(
            pattern_rows=pattern_rows,
            pattern_percent=50,
            activity_percent=60,
            connection_percent=50,
            pattern_cap=4,
        )

        self.assertEqual(2, snapshot["pattern_window"]["used_pattern_count"])
        self.assertEqual(4, snapshot["activity_window"]["available_activity_count"])
        self.assertEqual(4, snapshot["connection_window"]["available_connection_count"])
        self.assertTrue(snapshot["flow_data"]["nodes"])
        self.assertTrue(snapshot["flow_data"]["edges"])
        self.assertTrue(
            all("layer" in node and "orderScore" in node for node in snapshot["flow_data"]["nodes"])
        )

    def test_case_trace_details_returns_case_timeline(self):
        trace = create_case_trace_details(self.prepared_df, "C001")

        self.assertTrue(trace["found"])
        self.assertEqual("C001", trace["case_id"])
        self.assertEqual(4, trace["summary"]["event_count"])
        self.assertEqual(900.0, trace["summary"]["total_duration_sec"])
        self.assertEqual("15m 0s", trace["summary"]["total_duration_text"])
        self.assertEqual(4, len(trace["events"]))
        self.assertEqual(1, trace["events"][0]["sequence_no"])
        self.assertEqual(trace["events"][1]["activity"], trace["events"][0]["next_activity"])
        self.assertEqual(120.0, trace["events"][0]["wait_to_next_sec"])
        self.assertEqual("", trace["events"][-1]["wait_to_next_text"])
        self.assertIsNone(trace["events"][-1]["next_activity"])

    def test_case_trace_details_returns_not_found_payload(self):
        trace = create_case_trace_details(self.prepared_df, "C999")

        self.assertFalse(trace["found"])
        self.assertEqual("C999", trace["case_id"])
        self.assertIsNone(trace["summary"])
        self.assertEqual([], trace["events"])

    def test_create_log_diagnostics_reports_duplicate_counts(self):
        raw_df = pd.DataFrame(
            [
                {"case_id": "C001", "activity": "申請", "start_time": "2024-01-01 09:00:00", "group_a": "Sales"},
                {"case_id": "C001", "activity": "申請", "start_time": "2024-01-01 09:00:00", "group_a": "Sales"},
                {"case_id": "C002", "activity": "承認", "start_time": "2024-01-02 10:00:00", "group_a": "HR"},
            ]
        )

        diagnostics = create_log_diagnostics(
            raw_df,
            case_id_column="case_id",
            activity_column="activity",
            timestamp_column="start_time",
        )

        self.assertEqual(3, diagnostics["record_count"])
        self.assertEqual(2, diagnostics["activity_type_count"])
        self.assertEqual(1, diagnostics["duplicate_row_count"])
        self.assertEqual("あり", diagnostics["duplicate_status"])
        self.assertEqual(2, diagnostics["deduplicated_record_count"])
        self.assertEqual(0.3333, diagnostics["duplicate_rate"])

    def test_normalize_filter_params_trims_blank_values(self):
        normalized = normalize_filter_params(
            date_from=" 2024-01-01 ",
            date_to="",
            filter_value_1=" Sales ",
            filter_value_2="   ",
            filter_value_3=None,
            activity_mode=" Exclude ",
            activity_values=[" Submit ", "", "Approve", "Submit"],
        )

        self.assertEqual("2024-01-01", normalized["date_from"])
        self.assertIsNone(normalized["date_to"])
        self.assertEqual("Sales", normalized["filter_value_1"])
        self.assertIsNone(normalized["filter_value_2"])
        self.assertIsNone(normalized["filter_value_3"])
        self.assertEqual("exclude", normalized["activity_mode"])
        self.assertEqual("Submit,Approve", normalized["activity_values"])

    def test_filter_prepared_df_filters_by_date_and_attributes(self):
        raw_df = pd.DataFrame(
            [
                {"case": "C001", "step": "Submit", "ts": "2024-01-01 09:00:00", "group_a": "Sales", "group_b": "Web", "group_c": "A"},
                {"case": "C001", "step": "Approve", "ts": "2024-01-03 09:00:00", "group_a": "Sales", "group_b": "Web", "group_c": "A"},
                {"case": "C002", "step": "Submit", "ts": "2024-01-02 10:00:00", "group_a": "HR", "group_b": "Mail", "group_c": "B"},
                {"case": "C002", "step": "Reject", "ts": "2024-01-04 10:00:00", "group_a": "HR", "group_b": "Mail", "group_c": "B"},
            ]
        )
        prepared_df = prepare_event_log(
            df=raw_df,
            case_id_column="case",
            activity_column="step",
            timestamp_column="ts",
        )

        filtered_df = filter_prepared_df(
            prepared_df,
            {
                "date_from": "2024-01-02",
                "date_to": "2024-01-03",
                "filter_value_1": "Sales",
                "filter_value_2": "Web",
                "filter_value_3": "A",
            },
            {
                "filter_column_1": "group_a",
                "filter_column_2": "group_b",
                "filter_column_3": "group_c",
            },
        )

        self.assertEqual(1, len(filtered_df))
        self.assertEqual(["C001"], filtered_df["case_id"].tolist())
        self.assertEqual(["Approve"], filtered_df["activity"].tolist())

    def test_filter_prepared_df_filters_by_activity_mode(self):
        raw_df = pd.DataFrame(
            [
                {"case": "C001", "step": "Submit", "ts": "2024-01-01 09:00:00", "group_a": "Sales"},
                {"case": "C001", "step": "Approve", "ts": "2024-01-02 09:00:00", "group_a": "Sales"},
                {"case": "C002", "step": "Submit", "ts": "2024-01-01 10:00:00", "group_a": "HR"},
                {"case": "C002", "step": "Reject", "ts": "2024-01-02 10:00:00", "group_a": "HR"},
            ]
        )
        prepared_df = prepare_event_log(
            df=raw_df,
            case_id_column="case",
            activity_column="step",
            timestamp_column="ts",
        )

        included_df = filter_prepared_df(
            prepared_df,
            {
                "activity_mode": "include",
                "activity_values": "Submit",
            },
            {},
        )
        excluded_df = filter_prepared_df(
            prepared_df,
            {
                "activity_mode": "exclude",
                "activity_values": "Submit",
            },
            {},
        )

        self.assertEqual(["Submit", "Submit"], included_df["activity"].tolist())
        self.assertEqual(["Approve", "Reject"], excluded_df["activity"].tolist())

    def test_get_filter_options_returns_sorted_unique_values(self):
        raw_df = pd.DataFrame(
            [
                {"case": "C001", "step": "Submit", "ts": "2024-01-01 09:00:00", "group_a": "Sales", "group_b": "Web", "group_c": "A"},
                {"case": "C002", "step": "Review", "ts": "2024-01-01 10:00:00", "group_a": "HR", "group_b": "Mail", "group_c": "B"},
                {"case": "C003", "step": "Approve", "ts": "2024-01-01 11:00:00", "group_a": "Sales", "group_b": "API", "group_c": "A"},
            ]
        )
        prepared_df = prepare_event_log(
            df=raw_df,
            case_id_column="case",
            activity_column="step",
            timestamp_column="ts",
        )

        options = get_filter_options(
            prepared_df,
            {
                "filter_column_1": "group_a",
                "filter_column_2": "group_b",
                "filter_column_3": "group_c",
            },
        )

        self.assertEqual(["HR", "Sales"], options["filters"][0]["options"])
        self.assertEqual(["API", "Mail", "Web"], options["filters"][1]["options"])
        self.assertEqual(["A", "B"], options["filters"][2]["options"])

    def test_normalize_filter_column_settings_accepts_stored_slot_shape(self):
        normalized = normalize_filter_column_settings(
            filter_value_1={"column_name": "group_a", "label": "分類1"},
            filter_value_2={"column_name": "group_b", "label": "分類2"},
        )

        self.assertEqual("group_a", normalized["filter_value_1"]["column_name"])
        self.assertEqual("分類1", normalized["filter_value_1"]["label"])
        self.assertEqual("group_b", normalized["filter_value_2"]["column_name"])
        self.assertEqual("分類2", normalized["filter_value_2"]["label"])
        self.assertIsNone(normalized["filter_value_3"]["column_name"])


if __name__ == "__main__":
    unittest.main()
