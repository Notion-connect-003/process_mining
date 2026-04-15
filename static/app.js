const PREVIEW_ROW_COUNT = 10;
const STORAGE_KEY = "processMiningLastResult";
const TOP_PAGE_STATE_KEY = "processMiningTopPageState";
const DASHBOARD_SUPPLEMENT_KEY = "processMiningDashboardSupplement";
const FLOW_SELECTION_STORAGE_KEY = "processMiningFlowSelection";
const TRANSITION_DRILLDOWN_STORAGE_KEY = "processMiningTransitionDrilldown";
const FILTER_SLOT_KEYS = ["filter_value_1", "filter_value_2", "filter_value_3"];
const DEFAULT_FILTER_LABELS = {
    filter_value_1: "グループ/カテゴリ フィルター①",
    filter_value_2: "グループ/カテゴリ フィルター②",
    filter_value_3: "グループ/カテゴリ フィルター③",
};

const form = document.getElementById("analyze-form");
const submitButton = document.getElementById("submit-button");
const statusPanel = document.getElementById("status-panel");
const summaryPanel = document.getElementById("summary-panel");
const resultPanels = document.getElementById("result-panels");
const csvFileInput = document.getElementById("csv-file-input");
const columnSourceNote = document.getElementById("column-source-note");
const diagnosticsPanel = document.getElementById("log-diagnostics-panel");
const diagnosticsSection = document.getElementById("log-diagnostics-section");
const diagnosticsButton = document.getElementById("run-diagnostics-button");
const diagnosticsExcelButton = document.getElementById("download-diagnostics-excel-button");
const diagnosticsSampleLimitInput = document.getElementById("diagnostics-sample-limit-input");
const resetFilterButton = document.getElementById("reset-filter-button");
const filterSelectionNote = document.getElementById("filter-selection-note");
const initialProfilePayloadElement = document.getElementById("initial-profile-payload");
const insightPanel = document.getElementById("insight-panel");
const setupSection = document.getElementById("setup-section");
const setupToggleButton = document.getElementById("setup-toggle-button");
const setupSourceTag = document.getElementById("setup-source-tag");
const setupMappingTag = document.getElementById("setup-mapping-tag");
const setupVolumeTag = document.getElementById("setup-volume-tag");
const setupSummaryOpenButton = document.getElementById("setup-summary-open-button");
const topOpenCsvButton = document.getElementById("top-open-csv-button");
const filterChipBar = document.getElementById("filter-chip-bar");

const caseIdColumnSelect = document.getElementById("case-id-column-select");
const activityColumnSelect = document.getElementById("activity-column-select");
const timestampColumnSelect = document.getElementById("timestamp-column-select");
const analysisDateFromInput = document.getElementById("analysis-date-from");
const analysisDateToInput = document.getElementById("analysis-date-to");
const startActivityValuesSelect = document.getElementById("start-activity-values-select");
const endActivityValuesSelect = document.getElementById("end-activity-values-select");

const filterColumnRefs = [
    {
        slot: "filter_value_1",
        titleElement: document.getElementById("filter-group-title-1"),
        columnSelect: document.getElementById("filter-column-1-select"),
        valueSelect: document.getElementById("filter-value-1-select"),
        valueLabel: document.getElementById("filter-value-1-label"),
    },
    {
        slot: "filter_value_2",
        titleElement: document.getElementById("filter-group-title-2"),
        columnSelect: document.getElementById("filter-column-2-select"),
        valueSelect: document.getElementById("filter-value-2-select"),
        valueLabel: document.getElementById("filter-value-2-label"),
    },
    {
        slot: "filter_value_3",
        titleElement: document.getElementById("filter-group-title-3"),
        columnSelect: document.getElementById("filter-column-3-select"),
        valueSelect: document.getElementById("filter-value-3-select"),
        valueLabel: document.getElementById("filter-value-3-label"),
    },
];

const defaultSourceFileName = String(columnSourceNote?.dataset?.sourceFileName || "").trim();
const DEFAULT_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT = 3000;

let isLoadingProfile = false;
let isAnalyzing = false;
let isRunningDiagnostics = false;
let isDownloadingDiagnosticsExcel = false;
let profileRequestVersion = 0;
let diagnosticRequestVersion = 0;
let dashboardSupplementRequestVersion = 0;
let restoredTopPageState = loadTopPageState();
let requiresFileReselection = false;
let currentProfilePayload = mergeProfilePayload(
    loadInitialProfilePayload(),
    restoredTopPageState?.profilePayload,
);
let currentDashboardSupplement = loadDashboardSupplement();

function setStatus(message, type = "info") {
    statusPanel.textContent = message;
    statusPanel.className = `status-panel ${type}`;
}

function hideStatus() {
    statusPanel.className = "status-panel hidden";
    statusPanel.textContent = "";
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function formatNumber(value) {
    const numericValue = Number(value);
    if (!Number.isFinite(numericValue)) {
        return String(value ?? "-");
    }
    return numericValue.toLocaleString("ja-JP");
}

function formatPercent(value, digits = 1) {
    const numericValue = Number(value);
    if (!Number.isFinite(numericValue)) {
        return "-";
    }
    return `${numericValue.toFixed(digits)}%`;
}

function isNumericCellValue(value) {
    if (typeof value === "number") {
        return Number.isFinite(value);
    }

    if (typeof value !== "string") {
        return false;
    }

    return /^-?\d+(?:\.\d+)?%?$/.test(value.replaceAll(",", "").trim());
}

function loadDashboardSupplement() {
    const storedValue = sessionStorage.getItem(DASHBOARD_SUPPLEMENT_KEY);
    if (!storedValue) {
        return null;
    }

    try {
        return JSON.parse(storedValue);
    } catch {
        sessionStorage.removeItem(DASHBOARD_SUPPLEMENT_KEY);
        return null;
    }
}

function saveDashboardSupplement(payload) {
    currentDashboardSupplement = payload || null;
    try {
        if (!payload) {
            sessionStorage.removeItem(DASHBOARD_SUPPLEMENT_KEY);
            return;
        }
        sessionStorage.setItem(DASHBOARD_SUPPLEMENT_KEY, JSON.stringify(payload));
    } catch {
        // Ignore storage failures and keep the current render only.
    }
}

function clearDashboardSupplement() {
    currentDashboardSupplement = null;
    try {
        sessionStorage.removeItem(DASHBOARD_SUPPLEMENT_KEY);
    } catch {
        // Ignore storage failures and keep the current render only.
    }
}

function setSetupSectionOpen(isOpen) {
    if (!setupSection || !setupToggleButton) {
        return;
    }

    setupSection.classList.toggle("is-open", Boolean(isOpen));
    setupToggleButton.setAttribute("aria-expanded", String(Boolean(isOpen)));
}

function getResolvedMappingLabel(selectElement, fallbackValue = "") {
    const directValue = String(selectElement?.value || "").trim();
    if (directValue) {
        return directValue;
    }
    return String(fallbackValue || "").trim();
}

function getCurrentSelectedAnalysisKeys() {
    if (!form) {
        return [];
    }
    return Array.from(form.querySelectorAll('input[name="analysis_keys"]:checked'))
        .map((input) => String(input.value || "").trim())
        .filter(Boolean);
}

function syncAnalysisSelectionChips() {
    if (!form) {
        return;
    }

    form.querySelectorAll(".target-check").forEach((labelElement) => {
        const checkbox = labelElement.querySelector('input[name="analysis_keys"]');
        labelElement.classList.toggle("checked", Boolean(checkbox?.checked));
    });
}

function getDashboardAnalysisKey(analyses = {}) {
    if (analyses?.pattern) {
        return "pattern";
    }
    if (analyses?.transition) {
        return "transition";
    }
    if (analyses?.frequency) {
        return "frequency";
    }
    return Object.keys(analyses || {})[0] || "";
}

function updateSetupSummary(resultData = loadLatestResult(), supplement = currentDashboardSupplement) {
    if (setupSourceTag) {
        const sourceFileName = String(
            currentProfilePayload?.source_file_name
            || resultData?.source_file_name
            || defaultSourceFileName
            || ""
        ).trim();
        setupSourceTag.textContent = sourceFileName
            ? `ファイル: ${sourceFileName}`
            : "ファイル: 未選択";
    }

    if (setupMappingTag) {
        const defaultSelection = currentProfilePayload?.default_selection || {};
        const mappingLabels = [
            getResolvedMappingLabel(caseIdColumnSelect, defaultSelection.case_id_column),
            getResolvedMappingLabel(activityColumnSelect, defaultSelection.activity_column),
            getResolvedMappingLabel(timestampColumnSelect, defaultSelection.timestamp_column),
        ].filter(Boolean);
        setupMappingTag.textContent = mappingLabels.length === 3
            ? `列: ${mappingLabels.join(" / ")}`
            : "列: ケースID / アクティビティ / タイムスタンプ";
    }

    if (!setupVolumeTag) {
        return;
    }

    const diagnostics = currentProfilePayload?.diagnostics;
    const dashboard = supplement?.dashboard;
    if (dashboard?.has_data) {
        setupVolumeTag.textContent = `件数: ${formatNumber(dashboard.total_cases)} ケース / ${formatNumber(dashboard.total_records)} イベント / ${formatNumber(dashboard.activity_type_count)} アクティビティ`;
        return;
    }

    if (diagnostics) {
        setupVolumeTag.textContent = `件数: ${formatNumber(diagnostics.case_count)} ケース / ${formatNumber(diagnostics.record_count)} レコード / ${formatNumber(diagnostics.activity_type_count)} アクティビティ`;
        return;
    }

    if (resultData?.case_count || resultData?.event_count) {
        setupVolumeTag.textContent = `件数: ${formatNumber(resultData.case_count)} ケース / ${formatNumber(resultData.event_count)} イベント`;
        return;
    }

    setupVolumeTag.textContent = "件数: ログ診断未実行";
}
function buildPreviewMessage(analysis, previewRows) {
    const totalRowCount = Number(analysis?.row_count ?? analysis?.rows?.length ?? 0);
    return totalRowCount > previewRows.length
        ? `Top ${previewRows.length} 件を表示 / 全 ${formatNumber(totalRowCount)} 件`
        : `全 ${formatNumber(totalRowCount)} 件を表示`;
}

function getDashboardPreviewHeaders(analysisKey) {
    switch (analysisKey) {
    case "frequency":
        return ["アクティビティ", "イベント件数", "ケース数", "平均所要時間(分)"];
    case "transition":
        return ["遷移", "ケース数", "平均所要時間(分)", "中央値所要時間(分)", "比率(%)"];
    case "pattern":
        return ["パターン", "ケース数", "比率(%)", "平均ケース処理時間(分)"];
    default:
        return [];
    }
}

function buildDashboardPreviewRows(analysisKey, rows = []) {
    const previewHeaders = getDashboardPreviewHeaders(analysisKey);

    return rows.slice(0, 5).map((row, rowIndex) => {
        if (analysisKey === "transition") {
            const transitionName = row["遷移"]
                || [row["遷移元アクティビティ"], row["遷移先アクティビティ"]]
                    .filter(Boolean)
                    .join(" → ");
            return {
                "遷移": transitionName || "-",
                "ケース数": row["ケース数"] ?? row["遷移件数"] ?? "-",
                "平均所要時間(分)": row["平均所要時間(分)"] ?? "-",
                "中央値所要時間(分)": row["中央値所要時間(分)"] ?? "-",
                "比率(%)": row["比率(%)"] ?? row["遷移比率(%)"] ?? "-",
                __rowIndex: rowIndex,
            };
        }

        if (analysisKey === "pattern") {
            const patternText = String(
                row["パターン"]
                || row["処理順パターン"]
                || row.pattern
                || ""
            ).trim();
            const patternSteps = patternText
                .split(/\s*(?:→|->)\s*/u)
                .map((step) => step.trim())
                .filter(Boolean);
            const compactPattern = patternSteps.length > 3
                ? `${patternSteps.slice(0, 3).join(" → ")} → ... (${patternSteps.length}ステップ)`
                : patternText;
            return {
                "パターン": compactPattern || "-",
                "ケース数": row["ケース数"] ?? "-",
                "比率(%)": row["比率(%)"] ?? row["ケース比率(%)"] ?? "-",
                "平均ケース処理時間(分)": row["平均ケース処理時間(分)"] ?? row["平均所要時間(分)"] ?? "-",
                __rowIndex: rowIndex,
            };
        }

        const normalizedRow = {};
        previewHeaders.forEach((header) => {
            normalizedRow[header] = row[header] ?? "-";
        });
        normalizedRow.__rowIndex = rowIndex;
        return normalizedRow;
    });
}

function renderInsightPanel(supplement, state = "ready") {
    if (!insightPanel) {
        return;
    }

    if (state === "hidden") {
        insightPanel.className = "dashboard-insight hidden";
        insightPanel.innerHTML = "";
        return;
    }

    const insightTexts = Array.isArray(supplement?.insights?.items)
        ? supplement.insights.items.map((item) => String(item?.text || "").trim()).filter(Boolean).slice(0, 5)
        : [];
    let chipLabel = "AI 要約";
    let bodyText = "";

    if (state === "loading") {
        chipLabel = "読込中";
        bodyText = "ダッシュボードの補足情報を取得しています。";
    } else if (state === "error") {
        chipLabel = "取得失敗";
        bodyText = "ダッシュボード補足の取得に失敗しました。";
    } else if (!insightTexts.length) {
        chipLabel = "準備中";
        bodyText = "分析を実行すると、この画面向けの要点を表示します。";
    }

    insightPanel.className = "dashboard-insight";
    insightPanel.dataset.state = state;
    insightPanel.innerHTML = `
        <div class="dashboard-insight-head">
            <div class="dashboard-insight-title">AI インサイト</div>
            <span class="dashboard-insight-chip">${escapeHtml(chipLabel)}</span>
        </div>
        ${
            state === "ready" && insightTexts.length
                ? `
                    <ul class="dashboard-insight-list">
                        ${insightTexts.map((text) => `
                            <li class="dashboard-insight-item">
                                <span class="dashboard-insight-item-icon">•</span>
                                <span>${escapeHtml(text)}</span>
                            </li>
                        `).join("")}
                    </ul>
                `
                : `<p class="dashboard-insight-text">${escapeHtml(bodyText)}</p>`
        }
    `;
}
function buildDefaultProfilePayload() {
    return {
        headers: [],
        default_selection: {},
        column_settings: { filters: [] },
        filter_options: { filters: [] },
        diagnostics: null,
        source_file_name: "",
    };
}

function loadInitialProfilePayload() {
    if (!initialProfilePayloadElement) {
        return buildDefaultProfilePayload();
    }

    try {
        return mergeProfilePayload(
            buildDefaultProfilePayload(),
            JSON.parse(initialProfilePayloadElement.textContent || "{}"),
        );
    } catch {
        return buildDefaultProfilePayload();
    }
}

function mergeProfilePayload(basePayload, nextPayload) {
    const base = basePayload || buildDefaultProfilePayload();
    if (!nextPayload) {
        return base;
    }

    return {
        ...base,
        ...nextPayload,
        headers: Array.isArray(nextPayload.headers) ? nextPayload.headers : base.headers,
        default_selection: nextPayload.default_selection || base.default_selection,
        column_settings: nextPayload.column_settings || base.column_settings,
        filter_options: nextPayload.filter_options || base.filter_options,
        diagnostics: Object.prototype.hasOwnProperty.call(nextPayload, "diagnostics")
            ? nextPayload.diagnostics
            : base.diagnostics,
    };
}

function syncSubmitState() {
    const isBusy = (
        isLoadingProfile
        || isAnalyzing
        || isRunningDiagnostics
        || isDownloadingDiagnosticsExcel
    );
    submitButton.disabled = isBusy;
    if (diagnosticsButton) {
        diagnosticsButton.disabled = isBusy;
    }
    if (diagnosticsExcelButton) {
        diagnosticsExcelButton.disabled = isBusy;
    }
    if (topOpenCsvButton) {
        topOpenCsvButton.disabled = isBusy;
    }
}

function buildPatternDetailHref(runId, patternIndex) {
    return `/analysis/patterns/${encodeURIComponent(String(patternIndex))}?run_id=${encodeURIComponent(runId)}`;
}

function buildAnalysisDetailHref(analysisKey, runId) {
    const path = `/analysis/${encodeURIComponent(analysisKey)}`;
    return runId ? `${path}?run_id=${encodeURIComponent(runId)}` : path;
}

function buildTable(rows, options = {}) {
    if (!rows.length) {
        return '<p class="empty-state">表示できるデータがありません。</p>';
    }

    const { analysisKey = "", runId = "" } = options;
    const headers = Object.keys(rows[0]).filter((header) => !header.startsWith("__"));
    const headHtml = headers.map((header) => `<th>${escapeHtml(header)}</th>`).join("");
    const bodyHtml = rows.map((row) => {
        const cells = headers.map((header) => {
            const cellValue = escapeHtml(row[header]);
            const numericClass = isNumericCellValue(row[header]) ? " num" : "";
            const isWideHeader = (
                header.includes("\u30d1\u30bf\u30fc\u30f3")
                || header.includes("\u30a2\u30af\u30c6\u30a3\u30d3\u30c6\u30a3")
            );
            const isPatternLink = (
                analysisKey === "pattern"
                && header.includes("\u30d1\u30bf\u30fc\u30f3")
                && runId
                && Number.isInteger(row.__rowIndex)
            );

            if (isPatternLink) {
                return `
                    <td class="table-cell--wide${numericClass}">
                        <div class="cell-scroll-wrapper">
                            <a href="${buildPatternDetailHref(runId, row.__rowIndex)}" class="table-link">${cellValue}</a>
                        </div>
                    </td>
                `;
            }

            if (isWideHeader) {
                return `
                    <td class="table-cell--wide${numericClass}">
                        <div class="cell-scroll-wrapper">${cellValue}</div>
                    </td>
                `;
            }

            return `<td class="${numericClass.trim()}">${cellValue}</td>`;
        }).join("");

        return `<tr>${cells}</tr>`;
    }).join("");

    return `
        <div class="table-wrap">
            <table class="data-table">
                <thead><tr>${headHtml}</tr></thead>
                <tbody>${bodyHtml}</tbody>
            </table>
        </div>
    `;
}

function buildGroupedTable(rows, groupColumns = [], options = {}) {
    if (!rows.length) {
        return '<p class="empty-state">表示できるデータがありません。</p>';
    }

    const headers = Object.keys(rows[0]).filter((h) => !h.startsWith("__"));
    const validGroupCols = groupColumns.filter((col) => headers.includes(col));

    if (!validGroupCols.length) {
        return buildTable(rows, options);
    }

    const { analysisKey = "", runId = "" } = options;
    const nonGroupHeaders = headers.filter((h) => !validGroupCols.includes(h));
    const allHeaders = [...validGroupCols, ...nonGroupHeaders];
    const headHtml = allHeaders.map((header) => `<th>${escapeHtml(header)}</th>`).join("");

    // rowspan險育ｮ・ 蜷・｡後・蜷・げ繝ｫ繝ｼ繝怜・縺ｫ縺､縺・※縲悟酔縺倅ｸ贋ｽ阪げ繝ｫ繝ｼ繝怜､縺檎ｶ壹￥騾｣邯壽焚縲阪ｒ豎ゅａ繧・
    function calcRowspan(rowIndex, colIndex) {
        const row = rows[rowIndex];
        let span = 1;
        while (rowIndex + span < rows.length) {
            const same = validGroupCols
                .slice(0, colIndex + 1)
                .every((col) => String(rows[rowIndex + span][col] ?? "") === String(row[col] ?? ""));
            if (!same) break;
            span++;
        }
        return span;
    }

    function isMergedCell(rowIndex, colIndex) {
        if (rowIndex === 0) return false;
        return validGroupCols
            .slice(0, colIndex + 1)
            .every((col) => String(rows[rowIndex - 1][col] ?? "") === String(rows[rowIndex][col] ?? ""));
    }

    const bodyHtml = rows.map((row, rowIndex) => {
        const groupCellsHtml = validGroupCols.map((groupCol, colIndex) => {
            if (isMergedCell(rowIndex, colIndex)) return "";
            const span = calcRowspan(rowIndex, colIndex);
            const cellStr = escapeHtml(String(row[groupCol] ?? ""));
            const rowspanAttr = span > 1 ? ` rowspan="${span}"` : "";
            return `<td class="group-cell"${rowspanAttr}>${cellStr}</td>`;
        }).join("");

        const dataCellsHtml = nonGroupHeaders.map((header) => {
            const cellValue = row[header];
            const cellStr = escapeHtml(String(cellValue ?? ""));
            const numericClass = isNumericCellValue(cellValue) ? " num" : "";
            const isWideHeader = header.includes("パターン") || header.includes("アクティビティ");
            const isPatternLink = (
                analysisKey === "pattern"
                && header.includes("パターン")
                && runId
                && Number.isInteger(row.__rowIndex)
            );

            if (isPatternLink) {
                return `<td class="table-cell--wide${numericClass}"><div class="cell-scroll-wrapper"><a href="${buildPatternDetailHref(runId, row.__rowIndex)}" class="table-link">${cellStr}</a></div></td>`;
            }
            if (isWideHeader) {
                return `<td class="table-cell--wide${numericClass}"><div class="cell-scroll-wrapper">${cellStr}</div></td>`;
            }
            return `<td class="${numericClass.trim()}">${cellStr}</td>`;
        }).join("");

        return `<tr>${groupCellsHtml}${dataCellsHtml}</tr>`;
    }).join("");

    return `
        <div class="table-wrap">
            <table class="data-table data-table--grouped">
                <thead><tr>${headHtml}</tr></thead>
                <tbody>${bodyHtml}</tbody>
            </table>
        </div>
    `;
}

function splitPatternSteps(patternText) {
    return String(patternText || "")
        .split(/\s*(?:→|->)\s*/u)
        .map((step) => step.trim())
        .filter(Boolean);
}

function hasRepeatedPatternStep(patternText) {
    const steps = splitPatternSteps(patternText);
    return new Set(steps).size < steps.length;
}

function countRepeatedPatternSteps(patternText) {
    const steps = splitPatternSteps(patternText);
    return Math.max(0, steps.length - new Set(steps).size);
}

function calculateRepeatRatePct(patternText) {
    const steps = splitPatternSteps(patternText);
    if (!steps.length) {
        return 0;
    }
    return Math.round((countRepeatedPatternSteps(patternText) / steps.length) * 10000) / 100;
}

function buildPatternReviewFlag(repeatRatePct) {
    return Number(repeatRatePct || 0) >= 20 ? "要確認" : "";
}

function buildPatternSimpleComment(repeatCount, repeatRatePct) {
    if (Number(repeatRatePct || 0) >= 20) {
        return `繰り返し率 ${Number(repeatRatePct || 0).toFixed(2)}% で、手戻りが多い可能性があります。`;
    }
    if (Number(repeatCount || 0) > 0) {
        return `同一アクティビティの繰り返しが ${Number(repeatCount || 0)} 回あります。`;
    }
    return "繰り返しは確認されません。";
}

function normalizePatternAnalysisRow(row) {
    if (!row || typeof row !== "object" || Array.isArray(row)) {
        return row;
    }

    const patternText = (
        row["処理順パターン"]
        || row["パターン"]
        || row.pattern
        || ""
    );
    const repeatFlag = hasRepeatedPatternStep(patternText) ? "○" : "";
    const repeatCount = countRepeatedPatternSteps(patternText);
    const repeatRatePct = calculateRepeatRatePct(patternText);
    const reviewFlag = buildPatternReviewFlag(repeatRatePct);
    const simpleComment = buildPatternSimpleComment(repeatCount, repeatRatePct);

    return {
        ...row,
        "繰り返し": row["繰り返し"] ?? repeatFlag,
        "繰り返し回数": row["繰り返し回数"] ?? repeatCount,
        "繰り返し率(%)": row["繰り返し率(%)"] ?? repeatRatePct,
        "確認区分": row["確認区分"] ?? reviewFlag,
        "簡易コメント": row["簡易コメント"] ?? simpleComment,
    };
}

function normalizeLatestResult(data) {
    if (!data || typeof data !== "object" || Array.isArray(data)) {
        return data;
    }

    const analyses = data.analyses;
    if (!analyses || typeof analyses !== "object" || Array.isArray(analyses)) {
        return data;
    }

    const patternAnalysis = analyses.pattern;
    if (!patternAnalysis || !Array.isArray(patternAnalysis.rows)) {
        return data;
    }

    return {
        ...data,
        analyses: {
            ...analyses,
            pattern: {
                ...patternAnalysis,
                rows: patternAnalysis.rows.map((row) => normalizePatternAnalysisRow(row)),
            },
        },
    };
}

function saveLatestResult(data) {
    const normalizedData = normalizeLatestResult(data);
    clearDashboardSupplement();
    try {
        sessionStorage.setItem(STORAGE_KEY, JSON.stringify(normalizedData));
        sessionStorage.removeItem(FLOW_SELECTION_STORAGE_KEY);
        sessionStorage.removeItem(TRANSITION_DRILLDOWN_STORAGE_KEY);
    } catch {
        try {
            const fallbackData = {
                run_id: normalizedData.run_id,
                source_file_name: normalizedData.source_file_name,
                selected_analysis_keys: normalizedData.selected_analysis_keys,
                case_count: normalizedData.case_count,
                event_count: normalizedData.event_count,
                applied_filters: normalizedData.applied_filters,
                column_settings: normalizedData.column_settings,
                analyses: {},
            };
            sessionStorage.setItem(STORAGE_KEY, JSON.stringify(fallbackData));
            sessionStorage.removeItem(FLOW_SELECTION_STORAGE_KEY);
            sessionStorage.removeItem(TRANSITION_DRILLDOWN_STORAGE_KEY);
        } catch {
            // Ignore storage failures and keep the current render only.
        }
    }
}

function loadLatestResult() {
    const storedValue = sessionStorage.getItem(STORAGE_KEY);
    if (!storedValue) {
        return null;
    }

    try {
        return normalizeLatestResult(JSON.parse(storedValue));
    } catch {
        sessionStorage.removeItem(STORAGE_KEY);
        return null;
    }
}

function loadTopPageState() {
    const storedValue = sessionStorage.getItem(TOP_PAGE_STATE_KEY);
    if (!storedValue) {
        return null;
    }

    try {
        return JSON.parse(storedValue);
    } catch {
        sessionStorage.removeItem(TOP_PAGE_STATE_KEY);
        return null;
    }
}

function isUploadedSourceName(sourceFileName) {
    return Boolean(sourceFileName && defaultSourceFileName && sourceFileName !== defaultSourceFileName);
}

function buildTopPageState() {
    const activityEndpointFilterState = getCurrentActivityEndpointFilterState();
    return {
        source_file_name: currentProfilePayload?.source_file_name || "",
        profilePayload: currentProfilePayload,
        mapping_state: getCurrentMappingState(),
        filter_value_state: getCurrentFilterValueState(),
        start_activity_values: activityEndpointFilterState.start_activity_values,
        end_activity_values: activityEndpointFilterState.end_activity_values,
        date_from: String(analysisDateFromInput?.value || "").trim(),
        date_to: String(analysisDateToInput?.value || "").trim(),
        diagnostics_sample_limit: String(
            diagnosticsSampleLimitInput?.value || DEFAULT_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT,
        ).trim(),
    };
}

function saveTopPageState() {
    try {
        sessionStorage.setItem(TOP_PAGE_STATE_KEY, JSON.stringify(buildTopPageState()));
    } catch {
        // Ignore storage failures and keep the current render only.
    }
}

function restoreTopPageState(state) {
    if (!state) {
        return;
    }

    const mappingState = state.mapping_state || {};
    const filterValueState = state.filter_value_state || {};

    if (caseIdColumnSelect) {
        caseIdColumnSelect.value = mappingState.case_id_column || caseIdColumnSelect.value;
    }
    if (activityColumnSelect) {
        activityColumnSelect.value = mappingState.activity_column || activityColumnSelect.value;
    }
    if (timestampColumnSelect) {
        timestampColumnSelect.value = mappingState.timestamp_column || timestampColumnSelect.value;
    }
    if (analysisDateFromInput) {
        analysisDateFromInput.value = state.date_from || "";
    }
    if (analysisDateToInput) {
        analysisDateToInput.value = state.date_to || "";
    }
    if (diagnosticsSampleLimitInput) {
        diagnosticsSampleLimitInput.value = String(
            state.diagnostics_sample_limit || DEFAULT_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT,
        ).trim();
    }

    filterColumnRefs.forEach((filterRef, index) => {
        const selectedColumn = mappingState[`filter_column_${index + 1}`] || "";
        if (filterRef.columnSelect) {
            filterRef.columnSelect.value = selectedColumn;
        }

        const availableOptions = Array.from(filterRef.valueSelect?.options || []).map((option) => option.value);
        const selectedValue = getPreferredSelection(
            availableOptions,
            filterValueState[filterRef.slot],
        );
        if (filterRef.valueSelect) {
            filterRef.valueSelect.value = selectedValue;
        }
    });

    replaceMultiSelectOptions(
        startActivityValuesSelect,
        Array.from(startActivityValuesSelect?.options || []).map((option) => option.value),
        Array.isArray(state.start_activity_values) ? state.start_activity_values : [],
    );
    replaceMultiSelectOptions(
        endActivityValuesSelect,
        Array.from(endActivityValuesSelect?.options || []).map((option) => option.value),
        Array.isArray(state.end_activity_values) ? state.end_activity_values : [],
    );

    const restoredSourceFileName = String(
        state.profilePayload?.source_file_name || state.source_file_name || "",
    ).trim();
    requiresFileReselection = isUploadedSourceName(restoredSourceFileName) && !csvFileInput?.files?.[0];
    updateColumnSourceNote(currentProfilePayload?.source_file_name || restoredSourceFileName);
}

function getCurrentMappingState() {
    return {
        case_id_column: String(caseIdColumnSelect?.value || "").trim(),
        activity_column: String(activityColumnSelect?.value || "").trim(),
        timestamp_column: String(timestampColumnSelect?.value || "").trim(),
        filter_column_1: String(filterColumnRefs[0]?.columnSelect?.value || "").trim(),
        filter_column_2: String(filterColumnRefs[1]?.columnSelect?.value || "").trim(),
        filter_column_3: String(filterColumnRefs[2]?.columnSelect?.value || "").trim(),
    };
}

function getCurrentFilterValueState() {
    return Object.fromEntries(
        filterColumnRefs.map((filterRef) => [
            filterRef.slot,
            String(filterRef.valueSelect?.value || "").trim(),
        ])
    );
}

function getProfileFilterDefinitions(profilePayload) {
    const rawDefinitions = Array.isArray(profilePayload?.column_settings?.filters)
        ? profilePayload.column_settings.filters
        : [];
    const definitionMap = new Map(
        rawDefinitions.map((definition) => [definition.slot, definition])
    );

    return FILTER_SLOT_KEYS.map((slot) => {
        const fallbackLabel = DEFAULT_FILTER_LABELS[slot];
        const definition = definitionMap.get(slot) || {};
        return {
            slot,
            label: definition.label || fallbackLabel,
            column_name: definition.column_name || "",
        };
    });
}

function getFilterValueDefinitions(profilePayload) {
    const rawDefinitions = Array.isArray(profilePayload?.filter_options?.filters)
        ? profilePayload.filter_options.filters
        : (
            Array.isArray(profilePayload?.diagnostics?.filters)
                ? profilePayload.diagnostics.filters
                : []
        );
    const definitionMap = new Map(rawDefinitions.map((definition) => [definition.slot, definition]));

    return getProfileFilterDefinitions(profilePayload).map((definition) => ({
        ...definition,
        options: Array.isArray(definitionMap.get(definition.slot)?.options)
            ? definitionMap.get(definition.slot).options
            : [],
    }));
}

function getPreferredSelection(options, ...candidates) {
    for (const candidate of candidates) {
        if (candidate && options.includes(candidate)) {
            return candidate;
        }
    }
    return "";
}

function replaceSelectOptions(selectElement, options, selectedValue, placeholder = "驕ｸ謚槭＠縺ｦ縺上□縺輔＞") {
    if (!selectElement) {
        return;
    }

    selectElement.replaceChildren();
    selectElement.append(new Option(placeholder, ""));
    options.forEach((optionValue) => {
        selectElement.append(new Option(optionValue, optionValue, false, optionValue === selectedValue));
    });
    selectElement.value = selectedValue || "";
}

function replaceMultiSelectOptions(selectElement, options, selectedValues = []) {
    if (!selectElement) {
        return;
    }

    const normalizedSelectedValues = [...new Set(
        (Array.isArray(selectedValues) ? selectedValues : [])
            .map((value) => String(value || "").trim())
            .filter(Boolean)
    )];
    const normalizedOptions = [...new Set(
        [
            ...(Array.isArray(options) ? options : []),
            ...normalizedSelectedValues,
        ]
            .map((value) => String(value || "").trim())
            .filter(Boolean)
    )].sort((leftValue, rightValue) => leftValue.localeCompare(rightValue, "ja"));

    selectElement.innerHTML = normalizedOptions.map((value) => `
        <option value="${escapeHtml(value)}">${escapeHtml(value)}</option>
    `).join("");

    Array.from(selectElement.options).forEach((optionElement) => {
        optionElement.selected = normalizedSelectedValues.includes(optionElement.value);
    });
}

function readMultiSelectValues(selectElement) {
    return Array.from(selectElement?.selectedOptions || [])
        .map((optionElement) => String(optionElement.value || "").trim())
        .filter(Boolean);
}

function getCurrentActivityEndpointFilterState() {
    return {
        start_activity_values: readMultiSelectValues(startActivityValuesSelect),
        end_activity_values: readMultiSelectValues(endActivityValuesSelect),
    };
}

function renderColumnSelectors(profilePayload) {
    const headers = Array.isArray(profilePayload?.headers) ? profilePayload.headers : [];
    const defaultSelection = profilePayload?.default_selection || {};
    const currentMappingState = getCurrentMappingState();
    const filterDefinitions = getProfileFilterDefinitions(profilePayload);

    replaceSelectOptions(
        caseIdColumnSelect,
        headers,
        getPreferredSelection(
            headers,
            currentMappingState.case_id_column,
            profilePayload?.column_settings?.case_id_column,
            defaultSelection.case_id_column,
        )
    );
    replaceSelectOptions(
        activityColumnSelect,
        headers,
        getPreferredSelection(
            headers,
            currentMappingState.activity_column,
            profilePayload?.column_settings?.activity_column,
            defaultSelection.activity_column,
        )
    );
    replaceSelectOptions(
        timestampColumnSelect,
        headers,
        getPreferredSelection(
            headers,
            currentMappingState.timestamp_column,
            profilePayload?.column_settings?.timestamp_column,
            defaultSelection.timestamp_column,
        )
    );

    filterColumnRefs.forEach((filterRef, index) => {
        const definition = filterDefinitions[index];
        const selectedColumn = getPreferredSelection(
            headers,
            currentMappingState[`filter_column_${index + 1}`],
            definition.column_name,
        );
        if (filterRef.titleElement) {
            filterRef.titleElement.textContent = definition.label;
        }
        replaceSelectOptions(filterRef.columnSelect, headers, selectedColumn, "未設定");
    });
}

function renderActivityEndpointSelectors(profilePayload) {
    const currentFilterState = getCurrentActivityEndpointFilterState();
    const allActivityNames = Array.isArray(profilePayload?.filter_options?.all_activity_names)
        ? profilePayload.filter_options.all_activity_names
        : [];

    replaceMultiSelectOptions(
        startActivityValuesSelect,
        allActivityNames,
        currentFilterState.start_activity_values,
    );
    replaceMultiSelectOptions(
        endActivityValuesSelect,
        allActivityNames,
        currentFilterState.end_activity_values,
    );

    const isDisabled = allActivityNames.length === 0;
    if (startActivityValuesSelect) {
        startActivityValuesSelect.disabled = isDisabled;
    }
    if (endActivityValuesSelect) {
        endActivityValuesSelect.disabled = isDisabled;
    }
}

function renderFilterValueSelectors(profilePayload) {
    const currentFilterValues = getCurrentFilterValueState();
    const filterDefinitions = getFilterValueDefinitions(profilePayload);
    const selectedFilters = filterDefinitions
        .filter((definition) => definition.column_name)
        .map((definition) => `${definition.label}: ${definition.column_name}`);

    filterColumnRefs.forEach((filterRef, index) => {
        const definition = filterDefinitions[index];
        const options = Array.isArray(definition.options) ? definition.options : [];
        const selectedValue = getPreferredSelection(
            options,
            currentFilterValues[definition.slot],
        );

        if (filterRef.valueLabel) {
            filterRef.valueLabel.textContent = "値";
        }

        const valuePlaceholder = definition.column_name ? "すべて（グループ設定）" : "すべて";
        replaceSelectOptions(filterRef.valueSelect, options, selectedValue, valuePlaceholder);
        filterRef.valueSelect.disabled = !definition.column_name;
    });

    if (!filterSelectionNote) {
        return;
    }

    filterSelectionNote.textContent = selectedFilters.length
        ? `現在のフィルター候補: ${selectedFilters.join(" / ")}`
        : "分析対象にしたい列を選ぶと、絞り込み候補を選択できます。";
}

function buildMissingCountText(diagnostics) {
    const missingCounts = diagnostics?.missing_counts || {};
    const items = [
        `ケースID ${missingCounts.case_id ?? "-"}`,
        `アクティビティ ${missingCounts.activity ?? "-"}`,
        `タイムスタンプ ${missingCounts.timestamp ?? "-"}`,
    ];
    return items.join(" / ");
}

function buildLogPeriodText(diagnostics) {
    if (!diagnostics?.time_range?.min || !diagnostics?.time_range?.max) {
        return "ケースID / アクティビティ / タイムスタンプ列を選択すると表示します。";
    }
    return `${diagnostics.time_range.min} ～ ${diagnostics.time_range.max}`;
}

function buildDuplicateRateText(diagnostics) {
    const duplicateRate = Number(diagnostics?.duplicate_rate || 0);
    return `${(duplicateRate * 100).toFixed(1)}%`;
}

function renderDiagnostics(profilePayload) {
    if (!diagnosticsPanel) {
        return;
    }

    const diagnostics = profilePayload?.diagnostics;
    if (!diagnostics) {
        diagnosticsPanel.innerHTML = '<p class="empty-state">ログ診断を実行すると、件数・期間・欠損数・サンプル値を表示します。</p>';
        return;
    }

    const columns = Array.isArray(diagnostics.columns) ? diagnostics.columns : [];
    const columnRowsHtml = columns.map((column) => `
        <tr>
            <td>${escapeHtml(column.name)}</td>
            <td>${escapeHtml((column.sample_values || []).join(", ") || "-")}</td>
            <td>${escapeHtml(column.unique_count ?? "-")}</td>
            <td>${escapeHtml(column.missing_count ?? "-")}</td>
        </tr>
    `).join("");

    diagnosticsPanel.innerHTML = `
        <div class="diagnostics-summary-grid">
            <article class="diagnostic-card"><span class="summary-label">ログレコード数</span><strong>${escapeHtml(diagnostics.record_count ?? "-")}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">ケース数</span><strong>${escapeHtml(diagnostics.case_count ?? "-")}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">アクティビティ種類数</span><strong>${escapeHtml(diagnostics.activity_type_count ?? "-")}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">ログ期間</span><strong>${escapeHtml(buildLogPeriodText(diagnostics))}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">欠損数</span><strong>${escapeHtml(buildMissingCountText(diagnostics))}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">重複行数</span><strong>${escapeHtml(diagnostics.duplicate_row_count ?? 0)}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">重複あり/なし</span><strong>${escapeHtml(diagnostics.duplicate_status || "なし")}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">重複除外後レコード数</span><strong>${escapeHtml(diagnostics.deduplicated_record_count ?? "-")}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">重複率</span><strong>${escapeHtml(buildDuplicateRateText(diagnostics))}</strong></article>
        </div>
        <p class="panel-note">ヘッダー一覧: ${escapeHtml((diagnostics.headers || []).join(", "))}</p>
        <div class="table-wrap diagnostics-table-wrap">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>列名</th>
                        <th>サンプル値</th>
                        <th>ユニーク件数</th>
                        <th>欠損件数</th>
                    </tr>
                </thead>
                <tbody>${columnRowsHtml}</tbody>
            </table>
        </div>
    `;
}

function updateColumnSourceNote(sourceFileName) {
    if (!columnSourceNote) {
        return;
    }

    if (requiresFileReselection && !csvFileInput?.files?.[0]) {
        columnSourceNote.innerHTML = `前回は <code>${escapeHtml(sourceFileName || "CSV ファイル")}</code> を使っていました。再実行するには CSV を選び直してください。`;
        return;
    }

    if (!sourceFileName) {
        columnSourceNote.textContent = "CSV を読み込むと、列候補とログ診断を表示します。";
        return;
    }

    columnSourceNote.innerHTML = `現在は <code>${escapeHtml(sourceFileName)}</code> の列候補を表示しています。`;
}

function renderProfilePayload(profilePayload) {
    currentProfilePayload = profilePayload || currentProfilePayload;
    renderColumnSelectors(currentProfilePayload);
    renderActivityEndpointSelectors(currentProfilePayload);
    renderFilterValueSelectors(currentProfilePayload);
    renderDiagnostics(currentProfilePayload);
    updateColumnSourceNote(currentProfilePayload?.source_file_name || "");
    updateSetupSummary(loadLatestResult(), currentDashboardSupplement);
    syncAnalysisSelectionChips();
}

function appendMappingSettings(formData) {
    const mappingState = getCurrentMappingState();
    Object.entries(mappingState).forEach(([fieldName, fieldValue]) => {
        if (fieldValue) {
            formData.set(fieldName, fieldValue);
        }
    });
}

function getDiagnosticsSampleLimit() {
    const rawValue = String(
        diagnosticsSampleLimitInput?.value || DEFAULT_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT,
    ).trim();
    const numericValue = Number.parseInt(rawValue, 10);
    if (!Number.isFinite(numericValue) || numericValue <= 0) {
        return DEFAULT_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT;
    }

    return numericValue;
}

function appendDiagnosticsSettings(formData) {
    appendMappingSettings(formData);
    formData.set("sample_row_limit", String(getDiagnosticsSampleLimit()));
}

function appendActivityEndpointFilterSettings(formData) {
    const activityEndpointFilterState = getCurrentActivityEndpointFilterState();
    ["start_activity_values", "end_activity_values"].forEach((fieldName) => {
        formData.delete(fieldName);
        const selectedValues = Array.isArray(activityEndpointFilterState[fieldName])
            ? activityEndpointFilterState[fieldName]
            : [];
        if (selectedValues.length) {
            formData.set(fieldName, selectedValues.join(","));
        }
    });
}

function triggerFileDownload(blob, fileName) {
    const downloadUrl = URL.createObjectURL(blob);
    const downloadLink = document.createElement("a");
    downloadLink.href = downloadUrl;
    downloadLink.download = fileName;
    document.body.appendChild(downloadLink);
    downloadLink.click();
    downloadLink.remove();
    URL.revokeObjectURL(downloadUrl);
}

async function fetchLogProfile(file) {
    const formData = new FormData();
    if (file) {
        formData.append("csv_file", file);
    }
    appendMappingSettings(formData);

    const response = await fetch("/api/csv-headers", {
        method: "POST",
        body: formData,
    });
    const payload = await response.json();

    if (!response.ok) {
        throw new Error(payload.error || "ヘッダー一覧の取得に失敗しました。");
    }

    return payload;
}

async function fetchLogDiagnostics(file) {
    const formData = new FormData();
    if (file) {
        formData.append("csv_file", file);
    }
    appendDiagnosticsSettings(formData);

    const response = await fetch("/api/log-diagnostics", {
        method: "POST",
        body: formData,
    });
    const payload = await response.json();

    if (!response.ok) {
        throw new Error(payload.error || "ログ診断の取得に失敗しました。");
    }

    return payload;
}

async function downloadLogDiagnosticsExcel() {
    const validationError = (
        validateFileSelectionState()
        || validateColumnSelections()
        || validateFilterColumnSelections()
    );
    if (validationError) {
        setStatus(validationError, "error");
        return;
    }

    const formData = new FormData();
    const selectedFile = csvFileInput?.files?.[0] || null;
    if (selectedFile) {
        formData.append("csv_file", selectedFile);
    }
    appendDiagnosticsSettings(formData);

    isDownloadingDiagnosticsExcel = true;
    syncSubmitState();
    setStatus("ログ診断Excelを出力しています...", "info");

    try {
        const response = await fetch("/api/log-diagnostics-excel", {
            method: "POST",
            body: formData,
        });

        if (!response.ok) {
            let errorMessage = "ログ診断Excelの出力に失敗しました。";
            try {
                const payload = await response.json();
                errorMessage = payload.error || errorMessage;
            } catch {
                // ignore
            }
            throw new Error(errorMessage);
        }

        const blob = await response.blob();
        const downloadFileName = getDownloadFileName(response, "log_diagnostics.xlsx");
        triggerFileDownload(blob, downloadFileName);
        setStatus("ログ診断Excelを出力しました。", "success");
        saveTopPageState();
    } catch (error) {
        setStatus(error.message, "error");
    } finally {
        isDownloadingDiagnosticsExcel = false;
        syncSubmitState();
    }
}

async function refreshLogProfile() {
    const selectedFile = csvFileInput?.files?.[0] || null;
    const fileSelectionError = validateFileSelectionState();
    if (fileSelectionError && !selectedFile) {
        setStatus(fileSelectionError, "error");
        return;
    }

    const currentVersion = profileRequestVersion + 1;
    profileRequestVersion = currentVersion;
    diagnosticRequestVersion += 1;
    isLoadingProfile = true;
    syncSubmitState();
    setStatus("ヘッダー候補を読み込んでいます...", "info");

    try {
        const payload = await fetchLogProfile(selectedFile);
        if (currentVersion !== profileRequestVersion) {
            return;
        }

        requiresFileReselection = false;
        renderProfilePayload(payload);
        saveTopPageState();
        setStatus("候補を更新しました。", "success");
    } catch (error) {
        if (currentVersion !== profileRequestVersion) {
            return;
        }
        setStatus(error.message, "error");
    } finally {
        if (currentVersion === profileRequestVersion) {
            isLoadingProfile = false;
            syncSubmitState();
        }
    }
}

function validateFileSelectionState() {
    if (requiresFileReselection && !csvFileInput?.files?.[0]) {
        return "前回アップロードしたファイル設定を再利用するには、CSV ファイルを選び直してください。";
    }

    return "";
}

async function runLogDiagnostics() {
    const validationError = (
        validateFileSelectionState()
        || validateColumnSelections()
        || validateFilterColumnSelections()
    );
    if (validationError) {
        setStatus(validationError, "error");
        return;
    }

    const selectedFile = csvFileInput?.files?.[0] || null;
    const currentVersion = diagnosticRequestVersion + 1;
    diagnosticRequestVersion = currentVersion;
    isRunningDiagnostics = true;
    if (diagnosticsSection) {
        diagnosticsSection.open = true;
    }
    syncSubmitState();
    diagnosticsPanel.innerHTML = '<p class="panel-note">読み込み中...</p>';
    setStatus("ログ診断を実行しています...", "info");

    try {
        const payload = await fetchLogDiagnostics(selectedFile);
        if (currentVersion !== diagnosticRequestVersion) {
            return;
        }

        requiresFileReselection = false;
        renderProfilePayload(payload);
        saveTopPageState();
        setStatus("ログ診断を表示しました。", "success");
    } catch (error) {
        if (currentVersion !== diagnosticRequestVersion) {
            return;
        }
        renderDiagnostics({ diagnostics: null });
        setStatus(error.message, "error");
    } finally {
        if (currentVersion === diagnosticRequestVersion) {
            isRunningDiagnostics = false;
            syncSubmitState();
        }
    }
}

function validateColumnSelections() {
    const caseIdColumn = String(caseIdColumnSelect?.value || "").trim();
    const activityColumn = String(activityColumnSelect?.value || "").trim();
    const timestampColumn = String(timestampColumnSelect?.value || "").trim();

    if (!caseIdColumn || !activityColumn || !timestampColumn) {
        return "ケースID列 / アクティビティ列 / タイムスタンプ列を選択してください。";
    }

    if (new Set([caseIdColumn, activityColumn, timestampColumn]).size !== 3) {
        return "ケースID列 / アクティビティ列 / タイムスタンプ列にはそれぞれ異なる列を選択してください。";
    }

    return "";
}

function validateFilterColumnSelections() {
    const selectedColumns = filterColumnRefs
        .map((filterRef) => String(filterRef.columnSelect?.value || "").trim())
        .filter(Boolean);

    if (selectedColumns.length !== new Set(selectedColumns).size) {
        return "グループ/カテゴリー フィルター①〜③ にはそれぞれ異なる列を選択してください。";
    }

    return "";
}

function validateAnalyzeForm() {
    if (!Array.isArray(currentProfilePayload?.headers) || !currentProfilePayload.headers.length) {
        return "ログ情報を読み込んでから、分析対象の CSV を選択してください。";
    }

    const fileSelectionError = validateFileSelectionState();
    if (fileSelectionError) {
        return fileSelectionError;
    }

    const columnError = validateColumnSelections();
    if (columnError) {
        return columnError;
    }

    const filterColumnError = validateFilterColumnSelections();
    if (filterColumnError) {
        return filterColumnError;
    }

    const selectedAnalysisCount = form.querySelectorAll('input[name="analysis_keys"]:checked').length;
    if (selectedAnalysisCount === 0) {
        return "少なくとも 1 つの分析を選択してください。";
    }

    return "";
}

function getDownloadFileName(response, fallbackFileName) {
    const disposition = response.headers.get("Content-Disposition") || "";
    const utf8FileNameMatch = disposition.match(/filename\*=UTF-8''([^;]+)/i);
    if (utf8FileNameMatch) {
        return decodeURIComponent(utf8FileNameMatch[1]);
    }

    const fileNameMatch = disposition.match(/filename="([^"]+)"/i);
    if (fileNameMatch) {
        return fileNameMatch[1];
    }

    return fallbackFileName;
}

function buildAppliedFilterSummary(appliedFilters = {}, columnSettings = {}) {
    const filterDefinitions = Array.isArray(columnSettings?.filters) ? columnSettings.filters : [];
    const labelMap = new Map(filterDefinitions.map((definition) => [definition.slot, definition.label]));
    const appliedItems = [];

    if (appliedFilters?.date_from) {
        appliedItems.push(`開始日: ${appliedFilters.date_from}`);
    }
    if (appliedFilters?.date_to) {
        appliedItems.push(`終了日: ${appliedFilters.date_to}`);
    }

    appliedItems.push(...FILTER_SLOT_KEYS
        .filter((slot) => Boolean(appliedFilters?.[slot]))
        .map((slot) => `${labelMap.get(slot) || DEFAULT_FILTER_LABELS[slot]}: ${appliedFilters[slot]}`));

    if (appliedFilters?.start_activity_values) {
        appliedItems.push(`開始アクティビティ: ${appliedFilters.start_activity_values}`);
    }
    if (appliedFilters?.end_activity_values) {
        appliedItems.push(`終了アクティビティ: ${appliedFilters.end_activity_values}`);
    }

    return appliedItems.length ? appliedItems.join(" / ") : "フィルター未適用";
}
function buildFilterChipItems(appliedFilters = {}, columnSettings = {}) {
    const filterDefinitions = Array.isArray(columnSettings?.filters) ? columnSettings.filters : [];
    const labelMap = new Map(filterDefinitions.map((definition) => [definition.slot, definition.label]));
    const columnNameMap = new Map(filterDefinitions.map((definition) => [definition.slot, definition.column_name]));
    const chips = [];

    if (appliedFilters?.date_from || appliedFilters?.date_to) {
        chips.push({
            icon: "DATE",
            text: appliedFilters?.date_from && appliedFilters?.date_to
                ? `${appliedFilters.date_from} - ${appliedFilters.date_to}`
                : appliedFilters?.date_from
                    ? `開始日 ${appliedFilters.date_from}`
                    : `終了日 ${appliedFilters.date_to}`,
        });
    } else {
        chips.push({ icon: "DATE", text: "全期間" });
    }

    FILTER_SLOT_KEYS.forEach((slot, index) => {
        const colName = columnNameMap.get(slot);
        if (appliedFilters?.[slot]) {
            chips.push({
                icon: ["F1", "F2", "F3"][index] || "FLT",
                text: `${labelMap.get(slot) || DEFAULT_FILTER_LABELS[slot]}: ${appliedFilters[slot]}`,
            });
        } else if (colName) {
            chips.push({
                icon: "GRP",
                text: `${labelMap.get(slot) || DEFAULT_FILTER_LABELS[slot]} グループ設定`,
            });
        }
    });

    if (appliedFilters?.start_activity_values) {
        chips.push({
            icon: "START",
            text: `開始: ${appliedFilters.start_activity_values}`,
        });
    }

    if (appliedFilters?.end_activity_values) {
        chips.push({
            icon: "END",
            text: `終了: ${appliedFilters.end_activity_values}`,
        });
    }

    return chips;
}
function renderFilterChipBar(appliedFilters = {}, columnSettings = {}) {
    if (!filterChipBar) {
        return;
    }

    const chips = buildFilterChipItems(appliedFilters, columnSettings);
    filterChipBar.innerHTML = `
        <div class="dashboard-filter-chipbar-items">
            ${chips.map((chip) => `
                <span class="chip">
                    <span>${escapeHtml(chip.icon)}</span>
                    <span>${escapeHtml(chip.text)}</span>
                    <span class="chip-close" aria-hidden="true">×</span>
                </span>
            `).join("")}
            <button type="button" class="chip chip-secondary dashboard-filter-add">フィルター設定</button>
        </div>
        <div class="dashboard-filter-chipbar-actions">
            <span class="dashboard-filter-chipbar-note">${escapeHtml(buildAppliedFilterSummary(appliedFilters, columnSettings))}</span>
        </div>
    `;

    filterChipBar.querySelector(".dashboard-filter-add")?.addEventListener("click", () => {
        setSetupSectionOpen(true);
        filterColumnRefs[1]?.columnSelect?.focus();
    });
}

async function fetchDashboardSupplementPayload(runId, analysisKey) {
    const params = new URLSearchParams({
        row_limit: "0",
        include_dashboard: "true",
        include_impact: "false",
        include_root_cause: "false",
        include_insights: "true",
    });
    const response = await fetch(
        `/api/runs/${encodeURIComponent(runId)}/analyses/${encodeURIComponent(analysisKey)}?${params.toString()}`,
    );
    const payload = await response.json();

    if (!response.ok) {
        throw new Error(payload.error || "ダッシュボード補足の取得に失敗しました。");
    }

    return payload;
}

async function hydrateDashboardSupplement(runId, analyses = {}) {
    const analysisKey = getDashboardAnalysisKey(analyses);
    if (!runId || !analysisKey) {
        clearDashboardSupplement();
        updateSetupSummary(loadLatestResult(), null);
        renderInsightPanel(null, "hidden");
        return;
    }

    if (
        currentDashboardSupplement?.run_id === runId
        && currentDashboardSupplement?.analysis_key === analysisKey
    ) {
        const latestResult = loadLatestResult();
        if (latestResult?.run_id === runId) {
            renderSummary(latestResult, currentDashboardSupplement);
            renderInsightPanel(currentDashboardSupplement, "ready");
            updateSetupSummary(latestResult, currentDashboardSupplement);
        }
        return;
    }

    const requestVersion = dashboardSupplementRequestVersion + 1;
    dashboardSupplementRequestVersion = requestVersion;

    try {
        const payload = await fetchDashboardSupplementPayload(runId, analysisKey);
        if (requestVersion !== dashboardSupplementRequestVersion) {
            return;
        }

        const supplement = {
            run_id: payload.run_id,
            analysis_key: analysisKey,
            dashboard: payload.dashboard || {},
            impact: payload.impact || {},
            insights: payload.insights || {},
        };
        saveDashboardSupplement(supplement);

        const latestResult = loadLatestResult();
        if (latestResult?.run_id === runId) {
            renderSummary(latestResult, supplement);
            renderInsightPanel(supplement, "ready");
            updateSetupSummary(latestResult, supplement);
        }
    } catch {
        if (requestVersion !== dashboardSupplementRequestVersion) {
            return;
        }
        clearDashboardSupplement();

        const latestResult = loadLatestResult();
        if (latestResult?.run_id === runId) {
            renderSummary(latestResult, null);
            renderInsightPanel(null, "error");
            updateSetupSummary(latestResult, null);
        }
    }
}

function renderSummary(data, supplement = currentDashboardSupplement) {
    if (!data) {
        summaryPanel.className = "summary-panel hidden";
        summaryPanel.innerHTML = "";
        renderFilterChipBar();
        return;
    }

    const matchedSupplement = supplement?.run_id === data.run_id ? supplement : null;
    const dashboard = matchedSupplement?.dashboard || {};
    const diagnostics = currentProfilePayload?.diagnostics || {};
    const activityTypeCount = dashboard?.activity_type_count || diagnostics?.activity_type_count || 0;
    const avgDurationText = dashboard?.avg_case_duration_text || "集計中";
    const avgDurationSubtext = dashboard?.median_case_duration_text
        ? `中央値 ${dashboard.median_case_duration_text}`
        : "中央値を集計中";
    const top10CoverageText = Number.isFinite(Number(dashboard?.top10_variant_coverage_pct))
        ? formatPercent(dashboard.top10_variant_coverage_pct, 1)
        : "集計中";
    const top10CoverageSubtext = dashboard?.top_bottleneck_avg_wait_text
        ? `最大待ち時間 ${dashboard.top_bottleneck_avg_wait_text}`
        : "上位10パターンを集計中";
    const bottleneckHeadline = dashboard?.top_bottleneck_transition_label || "集計中";
    const bottleneckSubtext = dashboard?.top_bottleneck_avg_wait_text
        ? `平均待ち時間 ${dashboard.top_bottleneck_avg_wait_text}`
        : "平均待ち時間を集計中";

    renderFilterChipBar(data.applied_filters, data.column_settings);

    summaryPanel.className = "summary-panel dashboard-summary-panel dashboard-summary-panel--kpi";
    summaryPanel.innerHTML = `
        <div class="dashboard-kpi-grid">
            <article class="kpi-card dashboard-kpi-card">
                <div class="kpi-icon dashboard-kpi-icon dashboard-kpi-icon--blue">CS</div>
                <div>
                    <div class="kpi-label">ケース数</div>
                    <div class="kpi-value">${escapeHtml(formatNumber(data.case_count))}</div>
                    <div class="kpi-sub">${escapeHtml(formatNumber(data.event_count))} イベント</div>
                </div>
            </article>
            <article class="kpi-card dashboard-kpi-card">
                <div class="kpi-icon dashboard-kpi-icon dashboard-kpi-icon--indigo">ACT</div>
                <div>
                    <div class="kpi-label">アクティビティ種類数</div>
                    <div class="kpi-value">${escapeHtml(formatNumber(activityTypeCount))}</div>
                    <div class="kpi-sub">${escapeHtml(formatNumber(Object.keys(data.analyses || {}).length))} 分析を表示</div>
                </div>
            </article>
            <article class="kpi-card dashboard-kpi-card">
                <div class="kpi-icon dashboard-kpi-icon dashboard-kpi-icon--emerald">AVG</div>
                <div>
                    <div class="kpi-label">平均所要時間</div>
                    <div class="kpi-value">${escapeHtml(avgDurationText)}</div>
                    <div class="kpi-sub">${escapeHtml(avgDurationSubtext)}</div>
                </div>
            </article>
            <article class="kpi-card dashboard-kpi-card">
                <div class="kpi-icon dashboard-kpi-icon dashboard-kpi-icon--amber">TOP</div>
                <div>
                    <div class="kpi-label">上位10パターンカバー率</div>
                    <div class="kpi-value">${escapeHtml(top10CoverageText)}</div>
                    <div class="kpi-sub">${escapeHtml(top10CoverageSubtext)}</div>
                </div>
            </article>
            <article class="kpi-card dashboard-kpi-card dashboard-kpi-card--warning">
                <div class="kpi-icon dashboard-kpi-icon dashboard-kpi-icon--rose">BOT</div>
                <div>
                    <div class="kpi-label">最大ボトルネック</div>
                    <div class="kpi-value dashboard-kpi-value--compact">${escapeHtml(bottleneckHeadline)}</div>
                    <div class="kpi-sub">${escapeHtml(bottleneckSubtext)}</div>
                </div>
            </article>
        </div>
        ${data.group_mode && data.group_summary ? buildGroupKpiCards(data.group_summary, data.group_columns || []) : ""}
    `;
}

function buildGroupKpiCards(groupSummary, groupColumns) {
    if (!groupColumns.length || !groupSummary) return "";
    const firstCol = groupColumns[0];
    const colData = groupSummary[firstCol];
    if (!colData) return "";

    const entries = Object.entries(colData).sort((a, b) => b[1].case_count - a[1].case_count);
    if (!entries.length) return "";

    const maxCaseCount = Math.max(...entries.map(([, v]) => v.case_count || 0)) || 1;
    const maxEventCount = Math.max(...entries.map(([, v]) => v.event_count || 0)) || 1;

    const caseRows = entries.map(([label, vals]) => {
        const pct = Math.round((vals.case_count / maxCaseCount) * 100);
        return `
            <div class="group-kpi-row">
                <span class="group-kpi-label" title="${escapeHtml(label)}">${escapeHtml(label)}</span>
                <div class="group-kpi-bar-track"><div class="group-kpi-bar-fill" style="width:${pct}%"></div></div>
                <span class="group-kpi-value">${escapeHtml(formatNumber(vals.case_count))}</span>
            </div>
        `;
    }).join("");

    const eventRows = entries.map(([label, vals]) => {
        const pct = Math.round((vals.event_count / maxEventCount) * 100);
        return `
            <div class="group-kpi-row">
                <span class="group-kpi-label" title="${escapeHtml(label)}">${escapeHtml(label)}</span>
                <div class="group-kpi-bar-track"><div class="group-kpi-bar-fill group-kpi-bar-fill--event" style="width:${pct}%"></div></div>
                <span class="group-kpi-value">${escapeHtml(formatNumber(vals.event_count))}</span>
            </div>
        `;
    }).join("");

    const hasDuration = entries.some(([, v]) => v.avg_duration_min != null);
    const durationCard = hasDuration ? (() => {
        const maxDur = Math.max(...entries.map(([, v]) => v.avg_duration_min || 0)) || 1;
        const durRows = entries.map(([label, vals]) => {
            const dur = vals.avg_duration_min ?? 0;
            const pct = Math.round((dur / maxDur) * 100);
            return `
                <div class="group-kpi-row">
                    <span class="group-kpi-label" title="${escapeHtml(label)}">${escapeHtml(label)}</span>
                    <div class="group-kpi-bar-track"><div class="group-kpi-bar-fill group-kpi-bar-fill--duration" style="width:${pct}%"></div></div>
                    <span class="group-kpi-value">${escapeHtml(String(dur.toFixed ? dur.toFixed(1) : dur))} 分</span>
                </div>
            `;
        }).join("");
        return `
            <div class="group-kpi-card">
                <div class="group-kpi-title">${escapeHtml(firstCol)} 別 平均所要時間(分)</div>
                <div class="group-kpi-card-body">${durRows}</div>
            </div>
        `;
    })() : "";

    return `
        <div class="group-kpi-panel">
            <div class="group-kpi-card">
                <div class="group-kpi-title">${escapeHtml(firstCol)} 別 ケース数</div>
                <div class="group-kpi-card-body">${caseRows}</div>
            </div>
            <div class="group-kpi-card">
                <div class="group-kpi-title">${escapeHtml(firstCol)} 別 イベント数</div>
                <div class="group-kpi-card-body">${eventRows}</div>
            </div>
            ${durationCard}
        </div>
    `;
}

function setActiveAnalysisTab(activeKey) {
    resultPanels.querySelectorAll("[data-analysis-tab]").forEach((buttonElement) => {
        buttonElement.classList.toggle(
            "active",
            String(buttonElement.dataset.analysisTab || "") === String(activeKey || ""),
        );
    });

    resultPanels.querySelectorAll("[data-analysis-panel]").forEach((panelElement) => {
        panelElement.classList.toggle(
            "active",
            String(panelElement.dataset.analysisPanel || "") === String(activeKey || ""),
        );
    });
}

function findFilterSlotByColumn(columnName) {
    return filterColumnRefs.findIndex(
        (ref) => String(ref.columnSelect?.value || "").trim() === String(columnName || "").trim(),
    );
}

function buildGroupTabBar(rows, groupColumns, activeValue, onSelect) {
    if (!groupColumns.length || !rows.length) return "";
    const firstGroupCol = groupColumns[0];
    const uniqueValues = [...new Set(rows.map((r) => String(r[firstGroupCol] ?? "")).filter(Boolean))].sort();
    if (!uniqueValues.length) return "";

    const tabsHtml = [
        `<button type="button" class="tab group-tab ${!activeValue ? "active" : ""}" data-group-value="">全体</button>`,
        ...uniqueValues.map((val) =>
            `<button type="button" class="tab group-tab ${activeValue === val ? "active" : ""}" data-group-value="${escapeHtml(val)}">${escapeHtml(val)}</button>`
        ),
    ].join("");

    return `<div class="tab-bar group-tab-bar" data-group-col="${escapeHtml(firstGroupCol)}">${tabsHtml}</div>`;
}

function buildBreadcrumbNav(drillPath) {
    if (!drillPath.length) return "";
    const crumbs = [
        `<span class="breadcrumb-item breadcrumb-item--link" data-drill-index="-1">全体</span>`,
        ...drillPath.map((entry, index) =>
            index < drillPath.length - 1
                ? `<span class="breadcrumb-sep">›</span><span class="breadcrumb-item breadcrumb-item--link" data-drill-index="${index}">${escapeHtml(entry.value)}</span>`
                : `<span class="breadcrumb-sep">›</span><span class="breadcrumb-item">${escapeHtml(entry.value)}</span>`
        ),
    ].join("");
    return `<nav class="breadcrumb-nav">${crumbs}</nav>`;
}

function renderAnalysisPanels(analyses, runId, groupColumns = []) {
    const analysisEntries = Object.entries(analyses || {});
    if (!analysisEntries.length) {
        resultPanels.className = "result-stack hidden";
        resultPanels.innerHTML = "";
        return;
    }

    const firstAnalysisKey = analysisEntries[0][0];
    const isGroupMode = groupColumns.length > 0;

    resultPanels.className = "analysis-section dashboard-analysis-shell";
    resultPanels.innerHTML = `
        <div class="tab-bar dashboard-tab-bar">
            ${analysisEntries.map(([analysisKey, analysis], index) => `
                <button
                    type="button"
                    class="tab dashboard-tab ${index === 0 ? "active" : ""}"
                    data-analysis-tab="${escapeHtml(analysisKey)}"
                >
                    ${escapeHtml(analysis.analysis_name || analysisKey)}
                </button>
            `).join("")}
        </div>
        <div class="tab-content dashboard-tab-content">
            ${analysisEntries.map(([analysisKey, analysis], index) => {
                const allRows = analysis.rows || [];

                const renderPanelContent = (filteredRows, drillPath = []) => {
                    const previewRows = buildDashboardPreviewRows(analysisKey, filteredRows);
                    const activeGroupValue = drillPath.length ? drillPath[drillPath.length - 1].value : "";
                    const groupTabBarHtml = isGroupMode
                        ? buildGroupTabBar(allRows, groupColumns, activeGroupValue, null)
                        : "";
                    const breadcrumbHtml = drillPath.length ? buildBreadcrumbNav(drillPath) : "";
                    const tableHtml = isGroupMode
                        ? buildGroupedTable(previewRows, groupColumns, { analysisKey, runId })
                        : buildTable(previewRows, { analysisKey, runId });

                    return `
                        ${breadcrumbHtml}
                        ${groupTabBarHtml}
                        ${tableHtml}
                    `;
                };

                const initialPreview = buildDashboardPreviewRows(analysisKey, allRows);
                const initialGroupTabBar = isGroupMode ? buildGroupTabBar(allRows, groupColumns, "", null) : "";
                const initialTable = isGroupMode
                    ? buildGroupedTable(initialPreview, groupColumns, { analysisKey, runId })
                    : buildTable(initialPreview, { analysisKey, runId });

                return `
                    <section
                        class="tab-panel dashboard-tab-panel ${index === 0 ? "active" : ""}"
                        data-analysis-panel="${escapeHtml(analysisKey)}"
                        data-analysis-key="${escapeHtml(analysisKey)}"
                    >
                        <div class="dashboard-preview-shell">
                            <div class="dashboard-preview-head">
                                <div>
                                    <h2>${escapeHtml(analysis.analysis_name || analysisKey)}</h2>
                                    <p class="dashboard-preview-meta">${escapeHtml(buildPreviewMessage(analysis, initialPreview))}</p>
                                </div>
                                <a href="${buildAnalysisDetailHref(analysisKey, runId || "")}" class="detail-link">詳細ページ</a>
                            </div>
                            <div class="group-content-area">
                                ${initialGroupTabBar}
                                ${initialTable}
                            </div>
                            <div class="dashboard-preview-foot">
                                <a href="${buildAnalysisDetailHref(analysisKey, runId || "")}" class="detail-link">詳細ページで続きを見る</a>
                            </div>
                        </div>
                    </section>
                `;
            }).join("")}
        </div>
    `;

    resultPanels.querySelectorAll("[data-analysis-tab]").forEach((buttonElement) => {
        buttonElement.addEventListener("click", () => {
            setActiveAnalysisTab(buttonElement.dataset.analysisTab || firstAnalysisKey);
        });
    });

    // 繧ｰ繝ｫ繝ｼ繝励ち繝悶・繧､繝吶Φ繝医Μ繧ｹ繝翫・險ｭ螳夲ｼ医し繝ｼ繝舌・蜀榊・譫撰ｼ・
    if (isGroupMode) {
        resultPanels.querySelectorAll(".group-tab-bar").forEach((tabBar) => {
            const groupCol = tabBar.dataset.groupCol;

            tabBar.querySelectorAll(".group-tab").forEach((tabBtn) => {
                tabBtn.addEventListener("click", () => {
                    const selectedValue = tabBtn.dataset.groupValue;
                    const slotIndex = findFilterSlotByColumn(groupCol);

                    if (selectedValue) {
                        // 繧ｰ繝ｫ繝ｼ繝怜､繧帝∈謚・竊・縺昴・繧ｹ繝ｭ繝・ヨ繧偵ヵ繧｣繝ｫ繧ｿ繝ｪ繝ｳ繧ｰ繝｢繝ｼ繝峨↓蛻・ｊ譖ｿ縺医※蜀榊・譫・
                        if (slotIndex >= 0) {
                            // 荳倶ｽ阪せ繝ｭ繝・ヨ縺ｮ繝輔ぅ繝ｫ繧ｿ繝ｼ蛟､繧偵け繝ｪ繧｢・域ｬ｡縺ｮ髫主ｱ､縺ｯ繧ｰ繝ｫ繝ｼ繝励Δ繝ｼ繝峨・縺ｾ縺ｾ・・
                            for (let i = slotIndex + 1; i < filterColumnRefs.length; i++) {
                                if (filterColumnRefs[i].valueSelect) {
                                    filterColumnRefs[i].valueSelect.value = "";
                                }
                            }
                            filterColumnRefs[slotIndex].valueSelect.value = selectedValue;
                        }
                    } else {
                        // 縲悟・菴薙阪ち繝・竊・蜈ｨ繧ｹ繝ｭ繝・ヨ縺ｮ繝輔ぅ繝ｫ繧ｿ繝ｼ蛟､繧偵け繝ｪ繧｢縺励※蜀榊・譫・
                        filterColumnRefs.forEach((ref) => {
                            if (ref.valueSelect) ref.valueSelect.value = "";
                        });
                    }

                    form.requestSubmit();
                });
            });
        });

        // 繝代Φ縺上★縺ｮ縲梧綾繧九阪け繝ｪ繝・け 竊・荳贋ｽ埼嚴螻､縺ｫ謌ｻ縺｣縺ｦ蜀榊・譫・
        resultPanels.querySelectorAll(".breadcrumb-item--link").forEach((crumb) => {
            crumb.addEventListener("click", () => {
                const drillIndex = Number.parseInt(crumb.dataset.drillIndex ?? "-1", 10);
                // drillIndex=-1 竊・蜈ｨ菴難ｼ亥・繧ｹ繝ｭ繝・ヨ繧ｯ繝ｪ繧｢・・
                // drillIndex=n 竊・n+1髫主ｱ､繧医ｊ荳九ｒ繧ｯ繝ｪ繧｢
                const clearFrom = drillIndex + 1;
                for (let i = clearFrom; i < filterColumnRefs.length; i++) {
                    if (filterColumnRefs[i].valueSelect) {
                        filterColumnRefs[i].valueSelect.value = "";
                    }
                }
                form.requestSubmit();
            });
        });
    }
}

function renderDashboard(data) {
    const normalizedData = normalizeLatestResult(data);
    const matchedSupplement = currentDashboardSupplement?.run_id === normalizedData.run_id
        ? currentDashboardSupplement
        : null;

    renderSummary(normalizedData, matchedSupplement);
    renderInsightPanel(
        matchedSupplement,
        normalizedData.run_id && !matchedSupplement ? "loading" : matchedSupplement ? "ready" : "hidden",
    );
    renderAnalysisPanels(normalizedData.analyses, normalizedData.run_id || "", normalizedData.group_columns || []);
    updateSetupSummary(normalizedData, matchedSupplement);

    if (normalizedData.run_id) {
        void hydrateDashboardSupplement(normalizedData.run_id, normalizedData.analyses || {});
    }
}

form.addEventListener("submit", async (event) => {
    event.preventDefault();

    const validationError = validateAnalyzeForm();
    if (validationError) {
        setStatus(validationError, "error");
        return;
    }

    isAnalyzing = true;
    syncSubmitState();
    setStatus("分析を実行しています...", "info");
    summaryPanel.className = "summary-panel hidden";
    renderInsightPanel(null, "hidden");
    resultPanels.className = "result-stack";
    resultPanels.innerHTML = "";

    try {
        const formData = new FormData(form);
        appendActivityEndpointFilterSettings(formData);
        const response = await fetch("/api/analyze", {
            method: "POST",
            body: formData,
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || "分析に失敗しました。");
        }

        saveLatestResult(data);
        renderDashboard(data);
        saveTopPageState();
        setSetupSectionOpen(false);
        setStatus("分析が完了しました。", "success");
    } catch (error) {
        setStatus(error.message, "error");
    } finally {
        isAnalyzing = false;
        syncSubmitState();
    }
});

setupToggleButton?.addEventListener("click", () => {
    setSetupSectionOpen(!setupSection?.classList.contains("is-open"));
});

setupSummaryOpenButton?.addEventListener("click", () => {
    setSetupSectionOpen(true);
    caseIdColumnSelect?.focus();
});

topOpenCsvButton?.addEventListener("click", () => {
    setSetupSectionOpen(true);
    csvFileInput?.click();
});

csvFileInput?.addEventListener("change", () => {
    requiresFileReselection = false;
    setSetupSectionOpen(true);
    void refreshLogProfile();
});

[caseIdColumnSelect, activityColumnSelect, timestampColumnSelect, ...filterColumnRefs.map((filterRef) => filterRef.columnSelect)]
    .forEach((element) => {
        element?.addEventListener("change", () => {
            hideStatus();
            updateSetupSummary(loadLatestResult(), currentDashboardSupplement);
            renderFilterChipBar(loadLatestResult()?.applied_filters, loadLatestResult()?.column_settings);
            saveTopPageState();
            void refreshLogProfile();
        });
    });

filterColumnRefs.forEach((filterRef) => {
        filterRef.valueSelect?.addEventListener("change", () => {
            hideStatus();
            renderFilterChipBar(loadLatestResult()?.applied_filters, loadLatestResult()?.column_settings);
            saveTopPageState();
        });
    });

[analysisDateFromInput, analysisDateToInput].forEach((element) => {
    element?.addEventListener("change", () => {
        hideStatus();
        renderFilterChipBar(loadLatestResult()?.applied_filters, loadLatestResult()?.column_settings);
        saveTopPageState();
    });
});

[startActivityValuesSelect, endActivityValuesSelect].forEach((element) => {
    element?.addEventListener("change", () => {
        hideStatus();
        renderFilterChipBar(loadLatestResult()?.applied_filters, loadLatestResult()?.column_settings);
        saveTopPageState();
    });
});
form.querySelectorAll('input[name="analysis_keys"]').forEach((element) => {
    element.addEventListener("change", () => {
        hideStatus();
        syncAnalysisSelectionChips();
        saveTopPageState();
    });
});

diagnosticsSampleLimitInput?.addEventListener("change", () => {
    hideStatus();
    saveTopPageState();
});

diagnosticsButton?.addEventListener("click", () => {
    void runLogDiagnostics();
});

diagnosticsExcelButton?.addEventListener("click", () => {
    void downloadLogDiagnosticsExcel();
});

resetFilterButton?.addEventListener("click", () => {
    if (analysisDateFromInput) analysisDateFromInput.value = "";
    if (analysisDateToInput) analysisDateToInput.value = "";
    filterColumnRefs.forEach((filterRef) => {
        if (filterRef.columnSelect) filterRef.columnSelect.value = "";
        replaceSelectOptions(filterRef.valueSelect, [], "", "すべて");
        if (filterRef.valueSelect) filterRef.valueSelect.disabled = true;
    });
    replaceMultiSelectOptions(startActivityValuesSelect, [], []);
    replaceMultiSelectOptions(endActivityValuesSelect, [], []);
    if (startActivityValuesSelect) startActivityValuesSelect.disabled = true;
    if (endActivityValuesSelect) endActivityValuesSelect.disabled = true;
    if (filterSelectionNote) {
        filterSelectionNote.textContent = "分析対象にしたい列を選ぶと、絞り込み候補を選択できます。";
    }
    hideStatus();
    renderFilterChipBar();
    saveTopPageState();
});
const latestResult = loadLatestResult();
renderFilterChipBar(latestResult?.applied_filters, latestResult?.column_settings);
if (latestResult) {
    renderDashboard(latestResult);
}

renderProfilePayload(currentProfilePayload);
restoreTopPageState(restoredTopPageState);
syncAnalysisSelectionChips();
updateSetupSummary(latestResult, currentDashboardSupplement);
setSetupSectionOpen(Boolean(requiresFileReselection));
saveTopPageState();
syncSubmitState();
hideStatus();











