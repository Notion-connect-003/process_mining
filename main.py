from pathlib import Path

from core.analysis_service import analyze_event_log

INPUT_FILE = Path("process_mining_sample_10000行.csv")
OUTPUT_ROOT_DIR = Path("出力ファイル")
# 入力CSVのヘッダー名をここで合わせます。
CASE_ID_COLUMN = "case_id"
ACTIVITY_COLUMN = "activity"
TIMESTAMP_COLUMN = "timestamp"


def main():
    selected_analysis_keys = []

    # 実行しない分析は、下の行をコメントアウトしてください。
    selected_analysis_keys.append("frequency")
    selected_analysis_keys.append("transition")
    selected_analysis_keys.append("pattern")

    result = analyze_event_log(
        file_source=INPUT_FILE,
        case_id_column=CASE_ID_COLUMN,
        activity_column=ACTIVITY_COLUMN,
        timestamp_column=TIMESTAMP_COLUMN,
        selected_analysis_keys=selected_analysis_keys,
        output_root_dir=OUTPUT_ROOT_DIR,
        export_excel=True,
    )

    for analysis_result in result["analyses"].values():
        print(f'{analysis_result["analysis_name"]} 出力完了: {analysis_result["excel_file"]}')


if __name__ == "__main__":
    main()
