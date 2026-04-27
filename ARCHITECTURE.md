# Process Mining Responsibility Map

## Overview

このプロジェクトは、イベントログを取り込み、

1. pandas で初回分析を作る
2. Parquet に保存する
3. DuckDB で再集計・詳細分析・ドリルダウンを返す
4. 画面表示と Excel レポートへ展開する

という責務分離で構成されている。

## Responsibility Map

```mermaid
flowchart TB
    subgraph UI["UI Layer"]
        TPL["templates/\nindex.html\nanalysis_detail.html\npattern_detail.html"]
        JS_TOP["static/top/\nTOP画面状態管理・分析実行"]
        JS_DETAIL["static/detail/\n詳細画面・チャート・AI表示"]
        JS_PATTERN["static/pattern/\nパターン詳細画面"]
        SHARED["static/process_mining_shared.js\n共通UIユーティリティ"]
    end

    subgraph APP["App Layer"]
        MAIN["app/main.py\nFastAPI composition root"]
        ROUTES["app/routes/\ningest.py\nflow.py\ndetail.py"]
        CONFIG["app/config/\napp_settings.py\nllm_config.py"]
    end

    subgraph SERVICE["Service Layer"]
        PIPE["analyze_pipeline.py\nフォーム解釈・分析実行パイプライン"]
        RUN["run_helpers.py\nRUN_STORE・run管理・filter統合"]
        SUPPORT["support_helpers.py\n列推定・プレビュー・入力補助"]
        QUERY["analysis_queries.py\n画面向け集約・キャッシュ司令塔"]
        DETAILCTX["detail_context.py\n詳細API/Excel向け文脈組み立て"]
        AI["ai_helpers.py\nai_context.py\nai_actions.py\nai_fallback.py\nllm_helpers.py"]
    end

    subgraph CORE["Core Analysis Layer"]
        LOADER["data_loader.py\nイベントログ正規化"]
        FILTER["analysis_filters.py\nfilter正規化・grouping判定"]
        ACORE["analysis_core.py\n初回分析実行\nvariant/pattern flow"]
        CONST["analysis_constants.py\n分析定義・列定義"]
        ANALYSISJP["core/分析/\n頻度分析\n前後処理分析\n処理順パターン分析"]
    end

    subgraph QUERYLAYER["DuckDB Query Layer"]
        DCORE["duckdb_core.py\nParquet保存\nスコープCTE生成"]
        DA["duckdb_analysis_queries.py\n再集計・variant・case trace"]
        DD["duckdb_detail_queries.py\nbottleneck\nimpact\ndashboard\nroot cause"]
    end

    subgraph EXPORT["Export Layer"]
        EXCELAPI["excel/\n互換エントリポイント"]
        REPORT["reports/excel/exports/\n実Workbook生成本体"]
        COMMON["reports/excel/common/\n共通スタイル・命名・表整形"]
    end

    subgraph STORAGE["Storage"]
        MEM["RUN_STORE\nメモリ上の分析セッション"]
        PARQUET["storage/runs/<run_id>/\nprepared.parquet\nraw_upload.parquet"]
    end

    TPL --> JS_TOP
    TPL --> JS_DETAIL
    TPL --> JS_PATTERN
    JS_TOP --> SHARED
    JS_DETAIL --> SHARED
    JS_PATTERN --> SHARED

    JS_TOP --> ROUTES
    JS_DETAIL --> ROUTES
    JS_PATTERN --> ROUTES
    MAIN --> ROUTES
    MAIN --> CONFIG

    ROUTES --> PIPE
    ROUTES --> RUN
    ROUTES --> SUPPORT
    ROUTES --> QUERY
    ROUTES --> DETAILCTX
    ROUTES --> AI

    PIPE --> LOADER
    PIPE --> FILTER
    PIPE --> ACORE
    PIPE --> RUN

    QUERY --> DA
    QUERY --> DD
    QUERY --> DCORE
    QUERY --> MEM

    DETAILCTX --> QUERY
    DETAILCTX --> AI
    DETAILCTX --> REPORT

    ACORE --> CONST
    ACORE --> ANALYSISJP
    LOADER --> ACORE
    FILTER --> ACORE

    RUN --> MEM
    RUN --> PARQUET
    DCORE --> PARQUET
    DA --> PARQUET
    DD --> PARQUET

    EXCELAPI --> REPORT
    REPORT --> COMMON
    REPORT --> QUERY
```

## Runtime Flow

```mermaid
sequenceDiagram
    participant Browser as Browser
    participant Route as app/routes
    participant Pipeline as analyze_pipeline
    participant Loader as data_loader
    participant Core as analysis_core
    participant Run as run_helpers
    participant Duck as duckdb_*
    participant Report as reports/excel/exports

    Browser->>Route: CSV upload / analyze request
    Route->>Pipeline: parse + prepare + execute
    Pipeline->>Loader: read CSV / prepare_event_log
    Loader-->>Pipeline: prepared_df
    Pipeline->>Core: analyze_prepared_event_log
    Core-->>Pipeline: frequency / transition / pattern
    Pipeline->>Run: save_run_data
    Run->>Duck: persist_prepared_parquet
    Duck-->>Run: prepared.parquet
    Run-->>Route: run_id + cached metadata
    Route-->>Browser: preview payload

    Browser->>Route: detail / flow / variants / bottlenecks
    Route->>Duck: query Parquet with filters
    Duck-->>Route: detail payload
    Route-->>Browser: JSON for detail screen

    Browser->>Route: Excel export
    Route->>Report: build workbook context
    Report->>Duck: load detail/query rows as needed
    Report-->>Route: xlsx bytes
    Route-->>Browser: Excel file
```

## Responsibility Boundaries

- `app/main.py`
  FastAPI の配線担当。実装本体ではなく依存注入ハブ。
- `app/routes/`
  HTTP 入出力担当。基本的に薄い。
- `app/services/`
  画面/API 用の組み立て担当。run 管理、キャッシュ、AI、export context を持つ。
- `core/analysis_*`
  pandas ベースの初回分析担当。
- `core/duckdb_*`
  Parquet を前提にした再集計・詳細クエリ担当。
- `reports/excel/exports/`
  画面表示結果を workbook へ再構成する担当。
- `static/*`
  各画面の状態管理と API 消費担当。

## Important Design Decisions

- 初回分析と再表示クエリを分離している。
- run ごとに Parquet を保存し、詳細画面はそこを読む。
- filter は event-level と case-level を混在運用している。
- AI は補助機能であり、失敗時は rule-based fallback を返す。
- Excel 出力は単表出力ではなく、分析文脈を再構成して複数シートを生成する。
