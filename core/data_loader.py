import pandas as pd


def read_csv_data(
    file_path,
    case_id_column,
    activity_column,
):
    return pd.read_csv(
        file_path,
        dtype={case_id_column: str, activity_column: str},
    )


def prepare_event_log(
    df,
    case_id_column,
    activity_column,
    timestamp_column,
):
    work = df.copy()

    required_columns = [case_id_column, activity_column, timestamp_column]
    for column_name in required_columns:
        if column_name not in work.columns:
            raise ValueError(f"入力CSVに必要な列がありません: {column_name}")

    # 指定ヘッダーを内部で使う共通列名へそろえます。
    work = work.rename(
        columns={
            case_id_column: "case_id",
            activity_column: "activity",
            timestamp_column: "timestamp",
        }
    )

    if work[["case_id", "activity", "timestamp"]].isna().any().any():
        raise ValueError("case_id, activity, timestamp に空欄があります。")

    if (
        (work["case_id"].str.strip() == "").any()
        or (work["activity"].str.strip() == "").any()
        or (work["timestamp"].astype(str).str.strip() == "").any()
    ):
        raise ValueError("case_id, activity, timestamp いずれかに空文字があります。")

    # 同一timestampがある場合に入力順を保てるように元の並びを持っておく
    work["input_order"] = range(len(work))

    work["timestamp"] = pd.to_datetime(work["timestamp"], errors="coerce")

    if work["timestamp"].isna().any():
        raise ValueError("timestamp に日付変換できない値があります。")

    work = work.sort_values(["case_id", "timestamp", "input_order"]).reset_index(drop=True)

    # 次イベント時刻との差分から分析用の時間列を作ります。
    work["start_time"] = work["timestamp"]
    work["next_time"] = work.groupby("case_id")["timestamp"].shift(-1).fillna(work["timestamp"])

    work["duration_sec"] = (work["next_time"] - work["start_time"]).dt.total_seconds()
    work["duration_min"] = (work["duration_sec"] / 60).round(2)

    if (work["duration_sec"] < 0).any():
        raise ValueError("case内のtimestampの並びに不正があります。")

    work["sequence_no"] = work.groupby("case_id").cumcount() + 1
    work["event_count_in_case"] = work.groupby("case_id")["activity"].transform("count")

    return work.drop(columns=["input_order"])


def load_and_prepare_data(
    file_path,
    case_id_column,
    activity_column,
    timestamp_column,
):
    raw_df = read_csv_data(
        file_path=file_path,
        case_id_column=case_id_column,
        activity_column=activity_column,
    )
    return prepare_event_log(
        df=raw_df,
        case_id_column=case_id_column,
        activity_column=activity_column,
        timestamp_column=timestamp_column,
    )
