from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
PROFILE_SAMPLE_FILE = BASE_DIR / "sample_event_log.csv"
SAMPLE_FILE = PROFILE_SAMPLE_FILE
RUN_STORAGE_DIR = BASE_DIR / "storage" / "runs"

MAX_STORED_RUNS = 5
PREVIEW_ROW_COUNT = 10
LARGE_DATASET_FLOW_FAST_PATH_THRESHOLD = 1_000_000

DEFAULT_HEADERS = {
    "case_id_column": "case_id",
    "activity_column": "activity",
    "timestamp_column": "timestamp",
}

CASE_ID_COLUMN_CANDIDATES = [
    "case_id",
    "Case ID",
    "CASE_ID",
    "caseId",
    "CaseID",
    "caseid",
    "ケースID",
    "事案ID",
    "case_no",
    "CaseNo",
    "case_number",
    "CaseNumber",
]

ACTIVITY_COLUMN_CANDIDATES = [
    "activity",
    "Activity",
    "ACTIVITY",
    "activity_name",
    "ActivityName",
    "task",
    "Task",
    "event",
    "Event",
    "action",
    "Action",
    "アクティビティ",
    "アクティビティ名",
    "処理",
    "工程",
    "ステップ",
]

TIMESTAMP_COLUMN_CANDIDATES = [
    "timestamp",
    "Timestamp",
    "TIMESTAMP",
    "start_time",
    "StartTime",
    "start_timestamp",
    "日時",
    "タイムスタンプ",
    "開始日時",
    "event_timestamp",
    "EventTimestamp",
    "time",
    "Time",
    "datetime",
    "DateTime",
    "date",
    "Date",
]

COLUMN_CANDIDATES = {
    "case_id_column": CASE_ID_COLUMN_CANDIDATES,
    "activity_column": ACTIVITY_COLUMN_CANDIDATES,
    "timestamp_column": TIMESTAMP_COLUMN_CANDIDATES,
}

COLUMN_DISPLAY_LABELS = {
    "case_id_column": "ケースID",
    "activity_column": "アクティビティ",
    "timestamp_column": "タイムスタンプ",
}
