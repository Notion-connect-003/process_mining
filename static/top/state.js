/**
 * Top page module.
 * Depends on: window.ProcessMiningShared and earlier top scripts.
 * Exposes: script-scoped functions used by static/app.js entrypoint.
 */
window.ProcessMiningTop = window.ProcessMiningTop || {};

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


const sharedUi = window.ProcessMiningShared;
const { buildTable, escapeHtml, formatNumber, loadLatestResult } = sharedUi;

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

    // rowspan は先頭のグループ列にだけ付けて、同じ値が続く間だけ縦結合する。
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
