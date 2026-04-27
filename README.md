# Process Mining Workbench

FastAPI 製のプロセスマイニング分析ツールです。イベントログ (CSV/Excel) を取り込み、バリアント分析、ボトルネック分析、フロー可視化、Excel レポート出力などを行います。

## 必要環境

- Python 3.10 以上
- Node.js（フロー図描画に使う `elkjs` をインストールするため）
- Windows / macOS / Linux

## セットアップ

プロジェクトルート（このファイルがある場所）で以下を実行します。

```bash
# 1. Python 仮想環境を作成（任意だが推奨）
python -m venv .venv

# Windows の場合
.venv\Scripts\activate
# macOS / Linux の場合
source .venv/bin/activate

# 2. Python 依存をインストール
pip install -r requirements.txt

# 3. フロント側の依存 (elkjs) をインストール
npm install
```

## 実行方法

### 方法 A: Python から直接起動

```bash
python -m app.main
```

`app/main.py` 末尾の `uvicorn.run(...)` が動き、`http://127.0.0.1:5000` で起動します。

### 方法 B: uvicorn コマンドで起動（開発時の自動リロード付き）

```bash
uvicorn app.main:app --host 127.0.0.1 --port 5000 --reload
```

起動後、ブラウザで以下にアクセスしてください。

```
http://127.0.0.1:5000/
```

## 使い方（概要）

1. トップ画面で CSV / Excel のイベントログをアップロード（またはサンプル `sample_event_log.csv` を利用）。
2. ケース ID / アクティビティ / タイムスタンプの列を指定して「分析」を実行。
3. バリアント、ボトルネック、フロー図などのダッシュボードを閲覧。
4. 必要に応じて Excel レポートをダウンロード。

サンプルデータ:
- `sample_event_log.csv`
- `process_mining_sample_10000行.csv`

## テスト

```bash
pytest
```

## ディレクトリ構成（主なもの）

- `app/` — FastAPI アプリ本体（`app/main.py` がエントリーポイント）
- `core/` — 分析ロジック、DuckDB クエリ
- `excel/` — Excel レポート生成
- `templates/`, `static/` — 画面テンプレートと静的ファイル
- `storage/` — 実行結果のキャッシュ（自動生成）
- `tests/` — テストコード

## よくあるトラブル

- **ポート 5000 が使用中**: `uvicorn app.main:app --port 8000` のように別ポートで起動してください。
- **`ModuleNotFoundError`**: `pip install -r requirements.txt` を実行した仮想環境が有効になっているか確認してください。
- **フロー図が表示されない**: `npm install` が成功しているか、`node_modules/elkjs` が存在するか確認してください。
