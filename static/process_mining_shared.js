/**
 * ProcessMiningShared — Frontend共有ユーティリティ
 * =====================================================================
 * `window.ProcessMiningShared` として公開される共有API一覧。
 * `static/app.js` / `static/detail.js` / `static/pattern_detail.js` から利用する。
 *
 * 公開API一覧:
 * ---------------------------------------------------------------------
 *  ステータス表示
 *   - setStatus(panelElement, message, type?)      ステータスパネルにメッセージを表示
 *   - hideStatus(panelElement)                     ステータスパネルを非表示にする
 *
 *  HTML/数値フォーマット
 *   - escapeHtml(value)                            HTML特殊文字をエスケープ
 *   - formatNumber(value, maximumFractionDigits?)  ja-JPロケールで数値をフォーマット
 *   - formatDateTime(value)                        ISO文字列を ja-JP の日時表示に変換
 *   - formatDurationSeconds(value)                 秒数を ja-JP ロケールでフォーマット
 *
 *  セッションストレージ / URL
 *   - loadLatestResult(storageKey?)                sessionStorage から直近の分析結果を復元
 *   - getRunId(latestResult)                       URLパラメータまたは復元結果から run_id を取得
 *
 *  トランジション/ツールチップ
 *   - buildTransitionKey(fromActivity, toActivity) 遷移IDを組み立てる
 *   - showTooltip(event, html)                     共有ツールチップを表示
 *   - hideTooltip()                                共有ツールチップを非表示にする
 *
 *  fetch / DOM構築
 *   - fetchJson(url, fallbackMessage, timeoutMs?)  JSON取得の共通ラッパ (タイムアウト付き)
 *   - buildTable(rows, options?)                   汎用テーブルHTMLを生成
 *
 *  ダウンロード / セレクタ
 *   - getDownloadFileName(response, fallbackName)  Content-Disposition からファイル名を取得
 *   - readMultiSelectValues(selectElement)         複数選択セレクタから選択値を配列で取得
 * ---------------------------------------------------------------------
 */
(() => {
    const DEFAULT_STORAGE_KEY = "processMiningLastResult";
    const DEFAULT_EMPTY_MESSAGE = "表示できるデータがありません。";
    const DEFAULT_WIDE_HEADERS = new Set([
        "処理順パターン",
        "アクティビティ名",
        "アクティビティ",
        "前処理アクティビティ名",
        "後処理アクティビティ名",
    ]);

    /**
     * ステータスパネルにメッセージを表示する。
     * @param {HTMLElement} panelElement - ステータスパネルDOM要素
     * @param {string} message           - 表示するメッセージ
     * @param {string} [type="info"]     - CSSクラス名サフィックス ("info"|"success"|"error" 等)
     */
    function setStatus(panelElement, message, type = "info") {
        panelElement.textContent = message;
        panelElement.className = `status-panel ${type}`;
    }

    /**
     * ステータスパネルを非表示にする。
     * @param {HTMLElement} panelElement - ステータスパネルDOM要素
     */
    function hideStatus(panelElement) {
        panelElement.className = "status-panel hidden";
        panelElement.textContent = "";
    }

    /**
     * HTML特殊文字をエスケープしてXSSを防止する。
     * @param {*} value - 任意の値 (文字列化してからエスケープ)
     * @returns {string} エスケープ済み文字列
     */
    function escapeHtml(value) {
        return String(value)
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#039;");
    }

    /**
     * sessionStorage から直近の分析結果を復元する。JSONパース失敗時は削除してnullを返す。
     * @param {string} [storageKey="processMiningLastResult"] - ストレージキー
     * @returns {object|null} 分析結果オブジェクト。未保存またはパース失敗時はnull。
     */
    function loadLatestResult(storageKey = DEFAULT_STORAGE_KEY) {
        const storedValue = sessionStorage.getItem(storageKey);

        if (!storedValue) {
            return null;
        }

        try {
            return JSON.parse(storedValue);
        } catch {
            sessionStorage.removeItem(storageKey);
            return null;
        }
    }

    /**
     * URLパラメータまたは復元結果から run_id を取得する。
     * @param {object|null} latestResult - loadLatestResult() の戻り値
     * @returns {string} run_id。取得できない場合は空文字。
     */
    function getRunId(latestResult) {
        const params = new URLSearchParams(window.location.search);
        return params.get("run_id") || latestResult?.run_id || "";
    }

    /**
     * ja-JPロケールで数値をフォーマットする (最大小数桁数指定可)。
     * @param {*} value                            - 数値または文字列
     * @param {number} [maximumFractionDigits=2]   - 最大小数点以下桁数
     * @returns {string} フォーマット済み文字列 (例: "1,234.5")
     */
    function formatNumber(value, maximumFractionDigits = 2) {
        return Number(value || 0).toLocaleString("ja-JP", {
            maximumFractionDigits,
        });
    }

    /**
     * ISO文字列等を ja-JP ロケールの日時表示に変換する。
     * @param {*} value - 日時文字列または Date オブジェクト
     * @returns {string} ja-JP 日時表記。パース失敗時は元の値を文字列化して返す。
     */
    function formatDateTime(value) {
        if (!value) {
            return "";
        }

        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
            return String(value);
        }

        return date.toLocaleString("ja-JP");
    }

    /**
     * 秒数を ja-JP ロケールでフォーマットする (最大小数点2桁)。
     * @param {*} value - 秒数 (数値または文字列)
     * @returns {string} フォーマット済み文字列
     */
    function formatDurationSeconds(value) {
        return Number(value || 0).toLocaleString("ja-JP", {
            maximumFractionDigits: 2,
        });
    }

    /**
     * 遷移IDを組み立てる (前処理__TO__後処理 形式)。
     * @param {string} fromActivity - 前処理アクティビティ名
     * @param {string} toActivity   - 後処理アクティビティ名
     * @returns {string} 遷移ID
     */
    function buildTransitionKey(fromActivity, toActivity) {
        return `${fromActivity}__TO__${toActivity}`;
    }

    /**
     * 共有ツールチップ (#pl-tooltip) を表示する。無ければ生成する。
     * @param {MouseEvent} event - ホバーイベント (event.targetのbounding rectを基準に配置)
     * @param {string} html      - ツールチップのHTML内容
     */
    function showTooltip(event, html) {
        let tip = document.getElementById("pl-tooltip");
        if (!tip) {
            tip = document.createElement("div");
            tip.id = "pl-tooltip";
            tip.className = "tooltip";
            tip.style.display = "none";
            document.body.appendChild(tip);
        }

        tip.innerHTML = html;
        tip.style.display = "block";
        const rect = event.target.getBoundingClientRect();
        tip.style.left = `${rect.left + rect.width / 2 - tip.offsetWidth / 2}px`;
        tip.style.top = `${rect.top - tip.offsetHeight - 8 + window.scrollY}px`;
    }

    /**
     * 共有ツールチップを非表示にする。
     */
    function hideTooltip() {
        const tip = document.getElementById("pl-tooltip");
        if (tip) {
            tip.style.display = "none";
        }
    }

    /**
     * JSON取得の共通ラッパ (タイムアウト+エラーハンドリング付き)。
     * @param {string} url                      - リクエストURL
     * @param {string} fallbackMessage          - payloadにエラー情報がない場合のメッセージ
     * @param {number} [timeoutMs=30000]        - タイムアウト (ミリ秒)
     * @returns {Promise<object>} パース済みJSON
     * @throws {Error} ステータス異常 / タイムアウト / ネットワークエラー
     */
    async function fetchJson(url, fallbackMessage, timeoutMs = 30000) {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

        try {
            const response = await fetch(url, { signal: controller.signal });
            clearTimeout(timeoutId);
            const payload = await response.json();

            if (!response.ok) {
                throw new Error(payload.detail || payload.error || fallbackMessage);
            }

            return payload;
        } catch (error) {
            clearTimeout(timeoutId);
            if (error.name === "AbortError") {
                throw new Error("サーバーからの応答がタイムアウトしました。データ量を絞って再試行してください。");
            }
            throw error;
        }
    }

    /**
     * Response の Content-Disposition ヘッダからダウンロードファイル名を取得する。
     * UTF-8 ("filename*=UTF-8''...") と ASCII ("filename=...") の両形式に対応。
     * @param {Response} response        - fetch の Response オブジェクト
     * @param {string} fallbackFileName  - ヘッダから取得できない場合のデフォルト名
     * @returns {string} ファイル名
     */
    function getDownloadFileName(response, fallbackFileName) {
        const disposition = response.headers.get("Content-Disposition") || "";
        const utf8Match = disposition.match(/filename\*=UTF-8''([^;]+)/i);
        if (utf8Match?.[1]) {
            try {
                return decodeURIComponent(utf8Match[1]);
            } catch {
                return utf8Match[1];
            }
        }

        const asciiMatch = disposition.match(/filename="?([^";]+)"?/i);
        return asciiMatch?.[1] || fallbackFileName;
    }

    /**
     * 複数選択セレクタから選択値を配列で取得する (空値を除外)。
     * @param {HTMLSelectElement|null} selectElement - <select multiple> 要素
     * @returns {string[]} 選択値の配列
     */
    function readMultiSelectValues(selectElement) {
        return Array.from(selectElement?.selectedOptions || [])
            .map((optionElement) => String(optionElement.value || "").trim())
            .filter(Boolean);
    }

    /**
     * 汎用テーブルHTMLを生成する。パターン分析の場合はパターン詳細へのリンクセルに切り替える。
     * @param {object[]} rows                              - 行オブジェクトの配列
     * @param {object}   [options]                         - オプション
     * @param {string}   [options.emptyMessage]            - 空配列時のメッセージ
     * @param {string}   [options.analysisKey]             - 分析種別 ("pattern" の場合リンク化)
     * @param {string}   [options.runId]                   - run_id (パターン詳細リンク構築用)
     * @param {string}   [options.patternHeader]           - パターン列のヘッダ名
     * @param {string}   [options.patternRowIndexKey]      - パターン行indexのキー名
     * @param {Function} [options.buildPatternDetailHref]  - (runId, rowIndex) => href
     * @param {boolean}  [options.filterInternalHeaders]   - "__"で始まるヘッダを除外するか
     * @param {Set<string>} [options.wideHeaders]          - 幅広セルにするヘッダ名のセット
     * @returns {string} テーブルHTML文字列
     */
    function buildTable(rows, options = {}) {
        const {
            emptyMessage = DEFAULT_EMPTY_MESSAGE,
            analysisKey = "",
            runId = "",
            patternHeader = "処理順パターン",
            patternRowIndexKey = "__rowIndex",
            buildPatternDetailHref = null,
            filterInternalHeaders = true,
            wideHeaders = DEFAULT_WIDE_HEADERS,
        } = options;

        if (!rows.length) {
            return `<p class="empty-state">${emptyMessage}</p>`;
        }

        const headers = Object.keys(rows[0]).filter((header) => !filterInternalHeaders || !header.startsWith("__"));
        const headHtml = headers
            .map((header) => `<th>${escapeHtml(header)}</th>`)
            .join("");

        const bodyHtml = rows
            .map((row) => {
                const cells = headers
                    .map((header) => {
                        const cellValue = escapeHtml(row[header]);
                        const isWideHeader = wideHeaders.has(header);
                        const isPatternLink = (
                            analysisKey === "pattern"
                            && header === patternHeader
                            && runId
                            && Number.isInteger(row[patternRowIndexKey])
                            && typeof buildPatternDetailHref === "function"
                        );

                        if (isPatternLink) {
                            return `
                                <td class="table-cell--wide">
                                    <div class="cell-scroll-wrapper">
                                        <a href="${buildPatternDetailHref(runId, row[patternRowIndexKey])}" class="table-link">
                                            ${cellValue}
                                        </a>
                                    </div>
                                </td>
                            `;
                        }

                        if (isWideHeader) {
                            return `
                                <td class="table-cell--wide">
                                    <div class="cell-scroll-wrapper">${cellValue}</div>
                                </td>
                            `;
                        }

                        return `<td>${cellValue}</td>`;
                    })
                    .join("");

                return `<tr>${cells}</tr>`;
            })
            .join("");

        return `
            <div class="table-wrap">
                <table class="data-table">
                    <thead><tr>${headHtml}</tr></thead>
                    <tbody>${bodyHtml}</tbody>
                </table>
            </div>
        `;
    }

    window.ProcessMiningShared = {
        setStatus,
        hideStatus,
        escapeHtml,
        loadLatestResult,
        getRunId,
        formatNumber,
        formatDateTime,
        formatDurationSeconds,
        buildTransitionKey,
        showTooltip,
        hideTooltip,
        fetchJson,
        buildTable,
        getDownloadFileName,
        readMultiSelectValues,
    };
})();
