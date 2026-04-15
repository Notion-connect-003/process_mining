from 共通スクリプト.data_loader import prepare_event_log, read_csv_data
from 共通スクリプト.Excel出力.excel_exporter import (
    convert_analysis_result_to_records,
    export_analysis_to_excel,
)
from 共通スクリプト.分析.前後処理分析.transition_analysis import (
    ANALYSIS_CONFIG as TRANSITION_ANALYSIS_CONFIG,
    create_transition_analysis,
)
from 共通スクリプト.分析.処理順パターン分析.pattern_analysis import (
    ANALYSIS_CONFIG as PATTERN_ANALYSIS_CONFIG,
    create_pattern_analysis,
)
from 共通スクリプト.分析.頻度分析.frequency_analysis import (
    ANALYSIS_CONFIG as FREQUENCY_ANALYSIS_CONFIG,
    create_frequency_analysis,
)


ANALYSIS_DEFINITIONS = {
    "frequency": {
        "create_function": create_frequency_analysis,
        "config": FREQUENCY_ANALYSIS_CONFIG,
    },
    "transition": {
        "create_function": create_transition_analysis,
        "config": TRANSITION_ANALYSIS_CONFIG,
    },
    "pattern": {
        "create_function": create_pattern_analysis,
        "config": PATTERN_ANALYSIS_CONFIG,
    },
}

DEFAULT_ANALYSIS_KEYS = ["frequency", "transition", "pattern"]

FLOW_FREQUENCY_ACTIVITY_COLUMN = "アクティビティ"
FLOW_FREQUENCY_EVENT_COUNT_COLUMN = "イベント件数"
FLOW_FREQUENCY_CASE_COUNT_COLUMN = "ケース数"
FLOW_FREQUENCY_AVG_DURATION_COLUMN = "平均処理時間(分)"
FLOW_FREQUENCY_RATIO_COLUMN = "イベント比率(%)"
FLOW_TRANSITION_FROM_COLUMN = "前処理アクティビティ名"
FLOW_TRANSITION_TO_COLUMN = "後処理アクティビティ名"
FLOW_TRANSITION_COUNT_COLUMN = "遷移件数"
FLOW_TRANSITION_AVG_WAIT_COLUMN = "平均所要時間(分)"
FLOW_TRANSITION_RATIO_COLUMN = "遷移比率(%)"
FLOW_PATTERN_CASE_COUNT_COLUMN = "ケース数"
FLOW_PATTERN_COLUMN = "処理順パターン"
FLOW_PATTERN_CASE_RATIO_COLUMN = "ケース比率(%)"
FLOW_PATTERN_AVG_CASE_DURATION_COLUMN = "平均ケース処理時間(分)"
FLOW_PATH_SEPARATOR = "→"
FLOW_PATTERN_CAP = 500
FLOW_LAYOUT_SWEEP_ITERATIONS = 4
FILTER_SLOT_KEYS = ("filter_value_1", "filter_value_2", "filter_value_3")
FILTER_PARAM_KEYS = (
    "date_from",
    "date_to",
    *FILTER_SLOT_KEYS,
    "activity_mode",
    "activity_values",
    "start_activity_values",
    "end_activity_values",
)
DEFAULT_FILTER_LABELS = {
    "filter_value_1": "グループ/カテゴリー フィルター①",
    "filter_value_2": "グループ/カテゴリー フィルター②",
    "filter_value_3": "グループ/カテゴリー フィルター③",
}
INSIGHT_ATTENTION_ACTIVITY_KEYWORDS = (
    "差戻し",
    "差し戻し",
    "再提出",
    "再申請",
    "手戻り",
    "再確認",
)
