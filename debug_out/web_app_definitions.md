# web_app.py 定義一覧

- 対象: `D:\歩\IT勉強\Notion_接続プロジェクト\プロセスマイニング\process_mining\web_app.py`
- 開始行は「定義ブロックの先頭」です。デコレータがある関数は `@app.get(...)` 等の行を開始行にしています。
- 終了行は Python AST の `end_lineno` を使っています。
- 分類は主責務ベースの簡易分類です。複数責務を持つ関数は代表カテゴリに寄せています。
- クラス定義数: 0
- 関数定義数: 139
- ネスト定義数: 1
- カテゴリ内訳: ルーティング 22 / Excel出力 50 / AI連携 11 / データ処理 45 / ユーティリティ 11

| 種別 | 名前 | 親スコープ | 開始行 | `def/class`行 | 終了行 | 分類 |
|---|---|---|---:|---:|---:|---|
| function | `_template_response` | `(module)` | 343 | 343 | 347 | ユーティリティ |
| function | `_normalize_header_name` | `(module)` | 353 | 353 | 354 | ユーティリティ |
| function | `_build_header_lookup` | `(module)` | 357 | 357 | 368 | ユーティリティ |
| function | `suggest_column_name` | `(module)` | 371 | 371 | 397 | データ処理 |
| function | `resolve_required_column_name` | `(module)` | 400 | 400 | 419 | データ処理 |
| function | `build_column_selection_payload` | `(module)` | 422 | 422 | 433 | データ処理 |
| function | `validate_selected_columns` | `(module)` | 436 | 436 | 453 | データ処理 |
| function | `validate_filter_column_settings` | `(module)` | 456 | 456 | 464 | データ処理 |
| function | `read_raw_log_dataframe` | `(module)` | 467 | 467 | 477 | データ処理 |
| function | `resolve_profile_file_source` | `(module)` | 480 | 480 | 487 | データ処理 |
| function | `get_static_version` | `(module)` | 490 | 490 | 498 | ユーティリティ |
| function | `get_run_storage_dir` | `(module)` | 501 | 501 | 502 | ユーティリティ |
| function | `get_run_prepared_parquet_path` | `(module)` | 505 | 505 | 506 | ユーティリティ |
| function | `cleanup_run_storage` | `(module)` | 509 | 509 | 512 | ユーティリティ |
| function | `save_run_data` | `(module)` | 515 | 515 | 560 | ユーティリティ |
| function | `get_run_data` | `(module)` | 563 | 563 | 570 | ユーティリティ |
| function | `get_run_group_columns` | `(module)` | 573 | 573 | 578 | データ処理 |
| function | `get_run_variant_pattern` | `(module)` | 581 | 581 | 590 | データ処理 |
| function | `get_request_filter_params` | `(module)` | 593 | 593 | 599 | データ処理 |
| function | `get_form_filter_params` | `(module)` | 602 | 602 | 608 | データ処理 |
| function | `get_form_filter_column_settings` | `(module)` | 611 | 611 | 616 | データ処理 |
| function | `get_effective_filter_params` | `(module)` | 619 | 619 | 620 | データ処理 |
| function | `build_filter_cache_key` | `(module)` | 623 | 623 | 628 | データ処理 |
| function | `get_filtered_meta_for_run` | `(module)` | 631 | 631 | 637 | データ処理 |
| function | `build_column_settings_payload` | `(module)` | 640 | 640 | 668 | データ処理 |
| function | `build_analysis_payload` | `(module)` | 671 | 671 | 704 | データ処理 |
| function | `extract_pattern_text_from_row` | `(module)` | 707 | 707 | 715 | データ処理 |
| function | `build_pattern_index_entries_from_rows` | `(module)` | 718 | 718 | 725 | データ処理 |
| function | `get_pattern_index_for_pattern` | `(module)` | 728 | 728 | 757 | データ処理 |
| function | `build_variant_response_item` | `(module)` | 760 | 760 | 779 | データ処理 |
| function | `build_variant_coverage_payload` | `(module)` | 782 | 782 | 789 | データ処理 |
| function | `sanitize_workbook_sheet_name` | `(module)` | 792 | 792 | 796 | Excel出力 |
| function | `sanitize_file_name_component` | `(module)` | 799 | 799 | 802 | Excel出力 |
| function | `resolve_analysis_display_name` | `(module)` | 805 | 805 | 809 | Excel出力 |
| function | `build_analysis_excel_file_name` | `(module)` | 812 | 812 | 816 | Excel出力 |
| function | `normalize_excel_cell_value` | `(module)` | 819 | 819 | 838 | Excel出力 |
| function | `style_excel_cell` | `(module)` | 841 | 841 | 849 | Excel出力 |
| function | `merge_excel_row` | `(module)` | 852 | 852 | 860 | Excel出力 |
| function | `estimate_wrapped_row_height` | `(module)` | 863 | 863 | 874 | Excel出力 |
| function | `initialize_excel_worksheet` | `(module)` | 877 | 877 | 879 | Excel出力 |
| function | `estimate_excel_text_width` | `(module)` | 882 | 882 | 894 | Excel出力 |
| function | `get_autosize_ignored_cells` | `(module)` | 897 | 897 | 907 | Excel出力 |
| function | `autosize_worksheet_columns` | `(module)` | 910 | 910 | 933 | Excel出力 |
| function | `append_table_to_worksheet` | `(module)` | 936 | 936 | 1037 | Excel出力 |
| function | `append_key_value_rows` | `(module)` | 1040 | 1040 | 1133 | Excel出力 |
| function | `append_bullet_rows` | `(module)` | 1136 | 1136 | 1191 | Excel出力 |
| function | `append_text_block_to_worksheet` | `(module)` | 1194 | 1194 | 1220 | Excel出力 |
| function | `append_custom_text_section_to_worksheet` | `(module)` | 1223 | 1223 | 1267 | Excel出力 |
| function | `append_definition_table_to_worksheet` | `(module)` | 1270 | 1270 | 1322 | Excel出力 |
| function | `build_ranked_rows` | `(module)` | 1325 | 1325 | 1332 | Excel出力 |
| function | `_normalize_group_section_value` | `(module)` | 1335 | 1335 | 1339 | Excel出力 |
| function | `_write_section_header` | `(module)` | 1342 | 1342 | 1361 | Excel出力 |
| function | `_iter_groups` | `(module)` | 1364 | 1364 | 1387 | Excel出力 |
| function | `_iter_groups_from_parquet` | `(module)` | 1390 | 1390 | 1514 | Excel出力 |
| function | `_write_frequency_data` | `(module)` | 1517 | 1517 | 1570 | Excel出力 |
| function | `localize_report_headers` | `(module)` | 1573 | 1573 | 1574 | Excel出力 |
| function | `localize_report_rows` | `(module)` | 1577 | 1577 | 1589 | Excel出力 |
| function | `build_filter_summary_text` | `(module)` | 1592 | 1592 | 1617 | Excel出力 |
| function | `build_transition_display_label` | `(module)` | 1620 | 1620 | 1632 | Excel出力 |
| function | `format_duration_text_for_report` | `(module)` | 1635 | 1635 | 1649 | Excel出力 |
| function | `build_bottleneck_export_rows` | `(module)` | 1652 | 1652 | 1672 | Excel出力 |
| function | `resolve_pattern_detail_sheet_count` | `(module)` | 1675 | 1675 | 1690 | Excel出力 |
| function | `build_pattern_overview_rows` | `(module)` | 1693 | 1693 | 1758 | Excel出力 |
| function | `coerce_report_number` | `(module)` | 1761 | 1761 | 1765 | Excel出力 |
| function | `build_pattern_export_summary` | `(module)` | 1768 | 1768 | 1877 | Excel出力 |
| function | `calculate_pattern_time_impact_minutes` | `(module)` | 1880 | 1880 | 1885 | Excel出力 |
| function | `build_pattern_issue_row` | `(module)` | 1888 | 1888 | 1912 | Excel出力 |
| function | `build_pattern_conclusion_summary` | `(module)` | 1915 | 1915 | 1939 | Excel出力 |
| function | `build_pattern_dashboard_summary` | `(module)` | 1942 | 1942 | 1962 | Excel出力 |
| function | `_set_chart_str_categories` | `(module)` | 1965 | 1965 | 1969 | Excel出力 |
| function | `_ensure_chart_data_sheet` | `(module)` | 1972 | 1972 | 1978 | Excel出力 |
| function | `_write_chart_data_block` | `(module)` | 1981 | 1981 | 2001 | Excel出力 |
| function | `build_excel_anchor` | `(module)` | 2004 | 2004 | 2005 | Excel出力 |
| function | `sort_pattern_rows_by_avg_duration_desc` | `(module)` | 2008 | 2008 | 2015 | Excel出力 |
| function | `append_pattern_dashboard_pie_chart` | `(module)` | 2018 | 2018 | 2045 | Excel出力 |
| function | `append_pattern_conclusion_charts` | `(module)` | 2048 | 2048 | 2108 | Excel出力 |
| function | `append_pattern_detail_sheet` | `(module)` | 2111 | 2111 | 2224 | Excel出力 |
| function | `serialize_ai_prompt_rows` | `(module)` | 2227 | 2227 | 2241 | AI連携 |
| function | `build_analysis_ai_prompt` | `(module)` | 2244 | 2244 | 2327 | AI連携 |
| function | `build_ai_fallback_text` | `(module)` | 2330 | 2330 | 2443 | AI連携 |
| function | `request_ollama_insights_text` | `(module)` | 2446 | 2446 | 2455 | AI連携 |
| function | `build_empty_ai_summary` | `(module)` | 2458 | 2458 | 2472 | AI連携 |
| function | `get_cached_ai_summary` | `(module)` | 2475 | 2475 | 2488 | AI連携 |
| function | `build_ai_context_summary` | `(module)` | 2491 | 2491 | 2600 | AI連携 |
| function | `build_ai_insights_summary` | `(module)` | 2603 | 2603 | 2720 | AI連携 |
| function | `get_analysis_export_sheet_keys` | `(module)` | 2723 | 2723 | 2736 | Excel出力 |
| function | `build_detail_summary_kpi_rows` | `(module)` | 2739 | 2739 | 2810 | データ処理 |
| function | `build_excel_ai_summary` | `(module)` | 2813 | 2813 | 2845 | Excel出力 |
| function | `parse_transition_selection` | `(module)` | 2848 | 2848 | 2853 | データ処理 |
| function | `build_detail_export_workbook_bytes` | `(module)` | 2856 | 2856 | 3493 | Excel出力 |
| function | `get_filter_options_payload` | `(module)` | 3496 | 3496 | 3511 | データ処理 |
| function | `_to_int` | `(module)` | 3514 | 3514 | 3518 | ユーティリティ |
| function | `_to_float` | `(module)` | 3521 | 3521 | 3525 | ユーティリティ |
| function | `build_variant_items_from_pattern_rows` | `(module)` | 3528 | 3528 | 3571 | データ処理 |
| function | `is_unfiltered_request` | `(module)` | 3574 | 3574 | 3575 | データ処理 |
| function | `get_variant_items` | `(module)` | 3578 | 3578 | 3599 | データ処理 |
| function | `get_variant_item` | `(module)` | 3602 | 3602 | 3609 | データ処理 |
| function | `get_pattern_summary_row` | `(module)` | 3612 | 3612 | 3638 | データ処理 |
| function | `get_analysis_data` | `(module)` | 3641 | 3641 | 3672 | データ処理 |
| function | `get_bottleneck_summary` | `(module)` | 3675 | 3675 | 3698 | データ処理 |
| function | `get_dashboard_summary` | `(module)` | 3701 | 3701 | 3730 | データ処理 |
| function | `get_root_cause_summary` | `(module)` | 3733 | 3733 | 3746 | データ処理 |
| function | `get_impact_summary` | `(module)` | 3749 | 3749 | 3762 | データ処理 |
| function | `get_rule_based_insights_summary` | `(module)` | 3765 | 3765 | 3831 | AI連携 |
| function | `get_pattern_flow_snapshot` | `(module)` | 3834 | 3834 | 3971 | データ処理 |
| function | `build_preview_response` | `(module)` | 3974 | 3974 | 3991 | データ処理 |
| function | `build_log_profile_payload` | `(module)` | 3994 | 3994 | 4036 | データ処理 |
| function | `resolve_log_diagnostic_sample_row_limit` | `(module)` | 4039 | 4039 | 4048 | データ処理 |
| function | `build_log_diagnostic_period_text` | `(module)` | 4051 | 4051 | 4057 | データ処理 |
| function | `build_log_diagnostic_missing_count_text` | `(module)` | 4060 | 4060 | 4066 | データ処理 |
| function | `build_log_diagnostic_duplicate_rate_text` | `(module)` | 4069 | 4069 | 4071 | データ処理 |
| function | `build_log_diagnostic_filter_rows` | `(module)` | 4074 | 4074 | 4104 | データ処理 |
| function | `build_log_diagnostic_sample_rows` | `(module)` | 4107 | 4107 | 4120 | データ処理 |
| function | `build_log_diagnostic_workbook_bytes` | `(module)` | 4123 | 4123 | 4201 | Excel出力 |
| function | `get_analysis_options` | `(module)` | 4204 | 4204 | 4216 | データ処理 |
| function | `index` | `(module)` | 4219 | 4220 | 4237 | ルーティング |
| function | `pattern_detail_page` | `(module)` | 4240 | 4241 | 4249 | ルーティング |
| function | `analysis_detail` | `(module)` | 4252 | 4253 | 4267 | ルーティング |
| function | `pattern_detail_api` | `(module)` | 4270 | 4271 | 4299 | ルーティング |
| function | `analysis_detail_api` | `(module)` | 4302 | 4303 | 4391 | ルーティング |
| function | `ai_insights_state_api` | `(module)` | 4394 | 4395 | 4417 | ルーティング |
| function | `ai_insights_generate_api` | `(module)` | 4420 | 4421 | 4442 | ルーティング |
| function | `analysis_excel_file_api` | `(module)` | 4445 | 4446 | 4468 | ルーティング |
| function | `analysis_excel_archive_api` | `(module)` | 4471 | 4472 | 4496 | ルーティング |
| function | `detail_excel_export_api` | `(module)` | 4499 | 4501 | 4540 | ルーティング |
| function | `filter_options_api` | `(module)` | 4543 | 4544 | 4554 | ルーティング |
| function | `pattern_flow_api` | `(module)` | 4557 | 4558 | 4596 | ルーティング |
| function | `variant_list_api` | `(module)` | 4599 | 4600 | 4626 | ルーティング |
| function | `bottleneck_list_api` | `(module)` | 4629 | 4630 | 4672 | ルーティング |
| function | `transition_case_drilldown_api` | `(module)` | 4675 | 4676 | 4717 | ルーティング |
| function | `activity_case_drilldown_api` | `(module)` | 4720 | 4721 | 4757 | ルーティング |
| function | `case_trace_api` | `(module)` | 4760 | 4761 | 4787 | ルーティング |
| async function | `csv_headers` | `(module)` | 4790 | 4791 | 4822 | ルーティング |
| async function | `log_diagnostics` | `(module)` | 4825 | 4826 | 4857 | ルーティング |
| async function | `log_diagnostics_excel` | `(module)` | 4860 | 4861 | 4909 | ルーティング |
| async function | `analyze` | `(module)` | 4912 | 4913 | 5025 | ルーティング |
| function | `build_bottleneck_prompt` | `(module)` | 5028 | 5028 | 5068 | AI連携 |
| async function | `ai_insights` | `(module)` | 5071 | 5072 | 5105 | ルーティング |
| async function | `generate` | `ai_insights` | 5079 | 5079 | 5103 | AI連携 |
