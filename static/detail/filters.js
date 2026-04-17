/**
 * Detail page module.
 * Depends on: window.ProcessMiningShared and earlier detail scripts.
 * Exposes: script-scoped functions used by static/detail.js entrypoint.
 */
window.ProcessMiningDetail = window.ProcessMiningDetail || {};

const FLOW_SELECTION_STORAGE_KEY = "processMiningFlowSelection";
const DETAIL_ROW_LIMIT = 500;
const RENDERING_LIMIT = 1200; // Stricter limit for total elements
const EDGE_LIMIT = 800;      // Stricter limit for paths specifically
const AGGRESSIVE_LIMIT = 3000; // Threshold for extreme reduction

// -----------------------------------------------------------------------------
// Generic utilities
// -----------------------------------------------------------------------------
function debounce(func, wait) {
    let timeout;
    return (...args) => {
        clearTimeout(timeout);
        timeout = setTimeout(() => func(...args), wait);
    };
}

const analysisKey = document.body.dataset.analysisKey;
const statusPanel = document.getElementById("detail-status-panel");
const summaryPanel = document.getElementById("detail-summary-panel");
const chartPanel = document.getElementById("detail-chart-panel");
const chartTitle = document.getElementById("detail-chart-title");
const chartNote = document.getElementById("detail-chart-note");
const chartContainer = document.getElementById("detail-chart");
const resultPanel = document.getElementById("detail-result-panel");
const detailExportExcelButton = document.getElementById("detail-export-excel-button");
const detailExportTitle = document.getElementById("detail-export-title");
const detailExportMeta = document.getElementById("detail-export-meta");
const detailExportNote = document.getElementById("detail-export-note");
const detailExportScope = document.getElementById("detail-export-scope");
const detailPageTitle = document.getElementById("detail-page-title");
const detailPageCopy = document.getElementById("detail-page-copy");
const aiInsightsTitle = document.getElementById("ai-insights-title");
const aiInsightsMeta = document.getElementById("ai-insights-meta");
const aiInsightsNote = document.getElementById("ai-insights-note");
const aiInsightsState = document.getElementById("ai-insights-state");
const aiInsightsButton = document.getElementById("ai-insights-btn");
const aiInsightsOutput = document.getElementById("ai-insights-output");
const FILTER_SLOT_KEYS = ["filter_value_1", "filter_value_2", "filter_value_3"];
const DEFAULT_FILTER_LABELS = {
    filter_value_1: "グループ/カテゴリー フィルター①",
    filter_value_2: "グループ/カテゴリー フィルター②",
    filter_value_3: "グループ/カテゴリー フィルター③",
};
const DEFAULT_DETAIL_FILTERS = Object.freeze({
    date_from: "",
    date_to: "",
    filter_value_1: "",
    filter_value_2: "",
    filter_value_3: "",
    activity_mode: "include",
    activity_values: Object.freeze([]),
});
const VARIANT_PAGE_SIZE = 10;
let activeDetailFilters = { ...DEFAULT_DETAIL_FILTERS };
let detailPageAnalysisLoader = null;
let currentDetailColumnSettings = {};
let currentAiInsightsPayload = null;
let aiInsightsRequestVersion = 0;
let previousProcessMapMouseMoveHandler = null;
let previousProcessMapMouseUpHandler = null;
let pendingDetailSupplementSections = new Set();
let detailSupplementErrorMessage = "";
let currentDetailSummaryData = null;
let currentRenderedAnalysis = null;
let detailSupplementRequestVersion = 0;
let frequencyChartLimit = 10;
let transitionChartLimit = 15;

const sharedUi = window.ProcessMiningShared;
const { buildTransitionKey, escapeHtml, fetchJson, formatDateTime, formatDurationSeconds, getRunId, loadLatestResult } = sharedUi;
const DETAIL_HEAVY_FETCH_TIMEOUT_MS = 120000;
const DETAIL_LIGHTWEIGHT_LOAD_OPTIONS = Object.freeze({
    includeDashboard: false,
    includeImpact: false,
    includeRootCause: false,
    includeInsights: false,
});
const setStatus = (message, type = "info") => sharedUi.setStatus(statusPanel, message, type);
const hideStatus = () => sharedUi.hideStatus(statusPanel);


function cloneDetailFilters(filters = {}) {
    const rawActivityValues = Array.isArray(filters.activity_values)
        ? filters.activity_values
        : String(filters.activity_values || "").split(",");
    const activityValues = [...new Set(
        rawActivityValues
            .map((value) => String(value || "").trim())
            .filter(Boolean)
    )];

    return {
        date_from: String(filters.date_from || "").trim(),
        date_to: String(filters.date_to || "").trim(),
        filter_value_1: String(filters.filter_value_1 || "").trim(),
        filter_value_2: String(filters.filter_value_2 || "").trim(),
        filter_value_3: String(filters.filter_value_3 || "").trim(),
        activity_mode: String(filters.activity_mode || "include").trim() === "exclude" ? "exclude" : "include",
        activity_values: activityValues,
    };
}

function buildDefaultFilterDefinitions(columnSettings = {}) {
    const rawDefinitions = Array.isArray(columnSettings?.filters) ? columnSettings.filters : [];
    const rawDefinitionMap = new Map(rawDefinitions.map((definition) => [definition.slot, definition]));

    return FILTER_SLOT_KEYS.map((slot) => {
        const rawDefinition = rawDefinitionMap.get(slot) || {};
        return {
            slot,
            label: rawDefinition.label || DEFAULT_FILTER_LABELS[slot],
            column_name: rawDefinition.column_name || "",
            options: [],
        };
    });
}

function normalizeFilterDefinitions(filters = [], columnSettings = {}) {
    const definitionMap = new Map(
        buildDefaultFilterDefinitions(columnSettings).map((definition) => [definition.slot, definition])
    );

    (Array.isArray(filters) ? filters : []).forEach((definition) => {
        if (!definitionMap.has(definition.slot)) {
            return;
        }

        definitionMap.set(definition.slot, {
            ...definitionMap.get(definition.slot),
            label: definition.label || definitionMap.get(definition.slot).label,
            column_name: definition.column_name || definitionMap.get(definition.slot).column_name,
            options: Array.isArray(definition.options) ? definition.options : [],
        });
    });

    return FILTER_SLOT_KEYS.map((slot) => definitionMap.get(slot));
}

function buildFilterQueryParams(filters = {}) {
    const normalizedFilters = cloneDetailFilters(filters);
    const params = new URLSearchParams();

    Object.entries(normalizedFilters).forEach(([filterName, filterValue]) => {
        if (filterName === "activity_values") {
            if (Array.isArray(filterValue) && filterValue.length) {
                params.set(filterName, filterValue.join(","));
            }
            return;
        }

        if (filterName === "activity_mode") {
            if (Array.isArray(normalizedFilters.activity_values) && normalizedFilters.activity_values.length) {
                params.set(filterName, filterValue);
            }
            return;
        }

        if (filterValue) {
            params.set(filterName, filterValue);
        }
    });

    return params;
}

function buildFilterOptionsApiUrl(runId) {
    return `/api/runs/${encodeURIComponent(runId)}/filter-options`;
}

function loadDetailFilterOptions(runId) {
    return fetchJson(
        buildFilterOptionsApiUrl(runId),
        "フィルタ候補を読み込めませんでした。"
    );
}

function buildPatternDetailHref(runId, patternIndex) {
    return `/analysis/patterns/${encodeURIComponent(String(patternIndex))}?run_id=${encodeURIComponent(runId)}`;
}

// -----------------------------------------------------------------------------
// Analysis detail API helpers
// -----------------------------------------------------------------------------

function buildAnalysisDetailApiUrl(runId, rowOffset = 0, filters = activeDetailFilters, options = {}) {
    const resolvedOptions = {
        rowLimit: DETAIL_ROW_LIMIT,
        includeDashboard: true,
        includeImpact: true,
        includeRootCause: true,
        includeInsights: true,
        ...options,
    };
    const params = new URLSearchParams({
        row_limit: String(Math.max(0, Number(resolvedOptions.rowLimit) || 0)),
        row_offset: String(Math.max(0, Number(rowOffset) || 0)),
    });
    params.set("include_dashboard", resolvedOptions.includeDashboard ? "true" : "false");
    params.set("include_impact", resolvedOptions.includeImpact ? "true" : "false");
    params.set("include_root_cause", resolvedOptions.includeRootCause ? "true" : "false");
    params.set("include_insights", resolvedOptions.includeInsights ? "true" : "false");
    buildFilterQueryParams(filters).forEach((value, key) => {
        params.set(key, value);
    });

    return `/api/runs/${encodeURIComponent(runId)}/analyses/${encodeURIComponent(analysisKey)}?${params.toString()}`;
}

function buildTable(rows, options = {}) {
    return sharedUi.buildTable(rows, {
        ...options,
        buildPatternDetailHref,
    });
}

function loadFlowSelection(runId) {
    try {
        sessionStorage.removeItem(FLOW_SELECTION_STORAGE_KEY);
    } catch {
        // Ignore storage failures.
    }

    return null;
}

function saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey) {
    try {
        sessionStorage.removeItem(FLOW_SELECTION_STORAGE_KEY);
    } catch {
        // Ignore storage failures.
    }
}

function loadAnalysisPage(runId, rowOffset = 0, filters = activeDetailFilters, options = {}) {
    return fetchJson(
        buildAnalysisDetailApiUrl(runId, rowOffset, filters, options),
        "分析詳細の読み込みに失敗しました。",
        DETAIL_HEAVY_FETCH_TIMEOUT_MS
    );
}

function getInitialDetailLoadOptions() {
    if (analysisKey === "pattern") {
        return {
            ...DETAIL_LIGHTWEIGHT_LOAD_OPTIONS,
            includeImpact: true,
        };
    }
    return DETAIL_LIGHTWEIGHT_LOAD_OPTIONS;
}

function getDeferredDetailLoadOptions() {
    return {
        rowLimit: 0,
        includeDashboard: true,
        includeImpact: false,
        includeRootCause: true,
        includeInsights: true,
    };
}

function mergeDetailSummaryData(payload) {
    currentDetailSummaryData = {
        ...(currentDetailSummaryData || {}),
        source_file_name: payload?.source_file_name ?? currentDetailSummaryData?.source_file_name,
        case_count: payload?.case_count ?? currentDetailSummaryData?.case_count,
        event_count: payload?.event_count ?? currentDetailSummaryData?.event_count,
        dashboard: payload?.dashboard ?? currentDetailSummaryData?.dashboard,
        impact: payload?.impact ?? currentDetailSummaryData?.impact,
        insights: payload?.insights ?? currentDetailSummaryData?.insights,
        root_cause: payload?.root_cause ?? currentDetailSummaryData?.root_cause,
        applied_filters: payload?.applied_filters ?? currentDetailSummaryData?.applied_filters,
        column_settings: payload?.column_settings ?? currentDetailSummaryData?.column_settings,
    };
    return currentDetailSummaryData;
}

async function refreshDeferredDetailSections(runId) {
    if (!currentRenderedAnalysis) {
        return null;
    }

    const requestVersion = detailSupplementRequestVersion + 1;
    detailSupplementRequestVersion = requestVersion;
    detailSupplementErrorMessage = "";
    pendingDetailSupplementSections = new Set(["dashboard", "root_cause", "insights"]);

    if (currentDetailSummaryData) {
        renderSummary(currentDetailSummaryData, currentRenderedAnalysis);
    }

    try {
        const supplementData = await loadAnalysisPage(
            runId,
            0,
            activeDetailFilters,
            getDeferredDetailLoadOptions(),
        );

        if (requestVersion !== detailSupplementRequestVersion) {
            return null;
        }

        mergeDetailSummaryData(supplementData);
        pendingDetailSupplementSections = new Set(supplementData.deferred_sections || []);
        detailSupplementErrorMessage = "";
        renderSummary(currentDetailSummaryData, currentRenderedAnalysis);
        return supplementData;
    } catch (error) {
        if (requestVersion !== detailSupplementRequestVersion) {
            return null;
        }

        detailSupplementErrorMessage = error.message;
        renderSummary(currentDetailSummaryData, currentRenderedAnalysis);
        return null;
    }
}

async function syncAiInsightsPanel(runId, analysisName = "", { forceRefresh = false } = {}) {
    if (!aiInsightsButton || !aiInsightsTitle || !aiInsightsMeta || !aiInsightsNote || !aiInsightsOutput) {
        return null;
    }

    const requestVersion = aiInsightsRequestVersion + 1;
    aiInsightsRequestVersion = requestVersion;

    if (forceRefresh) {
        renderAiInsightsLoading(analysisName);
    }

    try {
        const payload = forceRefresh
            ? await generateAiInsights(runId, activeDetailFilters, Boolean(currentAiInsightsPayload?.generated))
            : await loadAiInsightsState(runId, activeDetailFilters);

        if (requestVersion !== aiInsightsRequestVersion) {
            return null;
        }

        renderAiInsightsPayload(payload, analysisName);
        return payload;
    } catch (error) {
        if (requestVersion !== aiInsightsRequestVersion) {
            return null;
        }

            aiInsightsMeta.textContent = "分析コメントの取得に失敗しました。";
        aiInsightsNote.textContent = error.message;
        aiInsightsOutput.textContent = "";
        aiInsightsOutput.classList.add("hidden");
        aiInsightsButton.disabled = false;
            aiInsightsButton.textContent = "分析コメントを生成";
        setAiInsightsChip("取得失敗", "error");
        return null;
    }
}

function buildVariantListApiUrl(runId, limit = 10, filters = activeDetailFilters) {
    const params = new URLSearchParams({
        limit: String(Math.max(0, Number(limit) || 0)),
    });
    buildFilterQueryParams(filters).forEach((value, key) => {
        params.set(key, value);
    });

    return `/api/runs/${encodeURIComponent(runId)}/variants?${params.toString()}`;
}

function loadVariantList(runId, limit = 10, filters = activeDetailFilters) {
    return fetchJson(
        buildVariantListApiUrl(runId, limit, filters),
        "バリアント一覧の読み込みに失敗しました。",
        DETAIL_HEAVY_FETCH_TIMEOUT_MS
    );
}

function buildBottleneckApiUrl(runId, limit = 5, variantId = null, filters = activeDetailFilters) {
    const params = new URLSearchParams({
        limit: String(Math.max(0, Number(limit) || 0)),
    });

    if (variantId !== null && variantId !== undefined) {
        params.set("variant_id", String(variantId));
    }
    buildFilterQueryParams(filters).forEach((value, key) => {
        params.set(key, value);
    });

    return `/api/runs/${encodeURIComponent(runId)}/bottlenecks?${params.toString()}`;
}

function loadBottleneckSummary(runId, limit = 5, variantId = null, filters = activeDetailFilters) {
    return fetchJson(
        buildBottleneckApiUrl(runId, limit, variantId, filters),
        "ボトルネック概要の読み込みに失敗しました。",
        DETAIL_HEAVY_FETCH_TIMEOUT_MS
    );
}

function buildActivityCasesApiUrl(runId, activity, limit = 20, variantId = null, filters = activeDetailFilters) {
    const params = new URLSearchParams({
        activity: String(activity || ""),
        limit: String(Math.max(0, Number(limit) || 0)),
    });

    if (variantId !== null && variantId !== undefined) {
        params.set("variant_id", String(variantId));
    }
    buildFilterQueryParams(filters).forEach((value, key) => {
        params.set(key, value);
    });

    return `/api/runs/${encodeURIComponent(runId)}/activity-cases?${params.toString()}`;
}

function loadActivityCases(runId, activity, limit = 20, variantId = null, filters = activeDetailFilters) {
    return fetchJson(
        buildActivityCasesApiUrl(runId, activity, limit, variantId, filters),
        "アクティビティのケース読み込みに失敗しました。"
    );
}

function buildTransitionCasesApiUrl(runId, fromActivity, toActivity, limit = 20, variantId = null, filters = activeDetailFilters) {
    const params = new URLSearchParams({
        from_activity: String(fromActivity || ""),
        to_activity: String(toActivity || ""),
        limit: String(Math.max(0, Number(limit) || 0)),
    });

    if (variantId !== null && variantId !== undefined) {
        params.set("variant_id", String(variantId));
    }
    buildFilterQueryParams(filters).forEach((value, key) => {
        params.set(key, value);
    });

    return `/api/runs/${encodeURIComponent(runId)}/transition-cases?${params.toString()}`;
}

function loadTransitionCases(runId, fromActivity, toActivity, limit = 20, variantId = null, filters = activeDetailFilters) {
    return fetchJson(
        buildTransitionCasesApiUrl(runId, fromActivity, toActivity, limit, variantId, filters),
        "遷移のケース読み込みに失敗しました。"
    );
}

function buildCaseTraceApiUrl(runId, caseId) {
    return `/api/runs/${encodeURIComponent(runId)}/cases/${encodeURIComponent(String(caseId || "").trim())}`;
}

function loadCaseTrace(runId, caseId) {
    return fetchJson(
        buildCaseTraceApiUrl(runId, caseId),
        "ケース追跡の読み込みに失敗しました。"
    );
}

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

function buildDetailExcelExportUrl(runId, options = {}) {
    const {
        analysisKeyName = analysisKey,
        filters = activeDetailFilters,
        patternDisplayLimit = "",
        variantId = null,
        selectedActivity = "",
        selectedTransitionKey = "",
        caseId = "",
        drilldownLimit = 20,
    } = options;
    const params = new URLSearchParams({
        analysis_key: String(analysisKeyName || ""),
        drilldown_limit: String(Math.max(0, Number(drilldownLimit) || 0)),
    });

    if (patternDisplayLimit) {
        params.set("pattern_display_limit", String(patternDisplayLimit));
    }
    if (variantId !== null && variantId !== undefined) {
        params.set("variant_id", String(variantId));
    }
    if (selectedActivity) {
        params.set("selected_activity", String(selectedActivity));
    }
    if (selectedTransitionKey) {
        params.set("selected_transition_key", String(selectedTransitionKey));
    }
    if (caseId) {
        params.set("case_id", String(caseId));
    }
    buildFilterQueryParams(filters).forEach((value, key) => {
        params.set(key, value);
    });

    return `/api/runs/${encodeURIComponent(runId)}/report-excel?${params.toString()}`;
}

// -----------------------------------------------------------------------------
// Formatting helpers
// -----------------------------------------------------------------------------


function buildFilterSelectionSummary(filters = {}, filterDefinitions = []) {
    const normalizedFilters = cloneDetailFilters(filters);
    const filterLabelMap = new Map(filterDefinitions.map((definition) => [definition.slot, definition.label]));
    const appliedItems = [];

    if (normalizedFilters.date_from) {
        appliedItems.push(`開始日: ${normalizedFilters.date_from}`);
    }
    if (normalizedFilters.date_to) {
        appliedItems.push(`終了日: ${normalizedFilters.date_to}`);
    }

    FILTER_SLOT_KEYS.forEach((slot) => {
        if (normalizedFilters[slot]) {
            appliedItems.push(`${filterLabelMap.get(slot) || DEFAULT_FILTER_LABELS[slot]}: ${normalizedFilters[slot]}`);
        }
    });

    if (normalizedFilters.activity_values.length) {
        appliedItems.push(
            `アクティビティ ${normalizedFilters.activity_mode === "exclude" ? "除外" : "含む"}: ${normalizedFilters.activity_values.join(", ")}`
        );
    }

    return appliedItems.length
        ? appliedItems.join(" / ")
        : "分析対象条件未適用";
}

function replaceSingleSelectOptions(selectElement, options, selectedValue = "", emptyLabel = "全て") {
    if (!selectElement) {
        return;
    }

    const normalizedSelectedValue = String(selectedValue || "").trim();
    const normalizedOptions = [...new Set(
        (Array.isArray(options) ? options : [])
            .map((value) => String(value || "").trim())
            .filter(Boolean)
    )];
    if (normalizedSelectedValue && !normalizedOptions.includes(normalizedSelectedValue)) {
        normalizedOptions.unshift(normalizedSelectedValue);
    }

    selectElement.innerHTML = `
        <option value="">${escapeHtml(emptyLabel)}</option>
        ${normalizedOptions.map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join("")}
    `;
    selectElement.value = normalizedSelectedValue;
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
    )];

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

function buildActivityFilterOptions(variantItems = [], selectedValues = []) {
    const optionSet = new Set(
        (Array.isArray(selectedValues) ? selectedValues : [])
            .map((value) => String(value || "").trim())
            .filter(Boolean)
    );

    (Array.isArray(variantItems) ? variantItems : []).forEach((variant) => {
        (Array.isArray(variant.activities) ? variant.activities : []).forEach((activityName) => {
            const normalizedActivityName = String(activityName || "").trim();
            if (normalizedActivityName) {
                optionSet.add(normalizedActivityName);
            }
        });
    });

    return [...optionSet].sort((leftValue, rightValue) => leftValue.localeCompare(rightValue, "ja"));
}
