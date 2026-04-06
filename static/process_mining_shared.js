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

    function setStatus(panelElement, message, type = "info") {
        panelElement.textContent = message;
        panelElement.className = `status-panel ${type}`;
    }

    function hideStatus(panelElement) {
        panelElement.className = "status-panel hidden";
        panelElement.textContent = "";
    }

    function escapeHtml(value) {
        return String(value)
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#039;");
    }

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

    function getRunId(latestResult) {
        const params = new URLSearchParams(window.location.search);
        return params.get("run_id") || latestResult?.run_id || "";
    }

    function formatNumber(value, maximumFractionDigits = 2) {
        return Number(value || 0).toLocaleString("ja-JP", {
            maximumFractionDigits,
        });
    }

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

    function formatDurationSeconds(value) {
        return Number(value || 0).toLocaleString("ja-JP", {
            maximumFractionDigits: 2,
        });
    }

    function buildTransitionKey(fromActivity, toActivity) {
        return `${fromActivity}__TO__${toActivity}`;
    }

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
                throw new Error("サーバーからの応答がタイムアウトしました。データ量を絞らして再試行してください。");
            }
            throw error;
        }
    }

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
                <table>
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
        fetchJson,
        buildTable,
    };
})();
