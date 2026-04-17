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
const { buildTransitionKey, escapeHtml, fetchJson, formatDateTime, getDownloadFileName, getRunId, loadLatestResult, readMultiSelectValues } = sharedUi;
const DETAIL_HEAVY_FETCH_TIMEOUT_MS = 120000;
const DETAIL_LIGHTWEIGHT_LOAD_OPTIONS = Object.freeze({
    includeDashboard: false,
    includeImpact: false,
    includeRootCause: false,
    includeInsights: false,
});
const setStatus = (message, type = "info") => sharedUi.setStatus(statusPanel, message, type);
const hideStatus = () => sharedUi.hideStatus(statusPanel);

function buildAiInsightsApiUrl(runId, filters = activeDetailFilters, forceRefresh = false) {
    const params = new URLSearchParams();
    buildFilterQueryParams(filters).forEach((value, key) => {
        params.set(key, value);
    });
    if (forceRefresh) {
        params.set("force_refresh", "true");
    }
    const query = params.toString();
    return `/api/runs/${encodeURIComponent(runId)}/ai-insights/${encodeURIComponent(analysisKey)}${query ? `?${query}` : ""}`;
}

async function loadAiInsightsState(runId, filters = activeDetailFilters) {
    return fetchJson(
        buildAiInsightsApiUrl(runId, filters),
        "分析コメントの状態を読み込めませんでした。",
        10000
    );
}

async function generateAiInsights(runId, filters = activeDetailFilters, forceRefresh = false) {
    const response = await fetch(buildAiInsightsApiUrl(runId, filters, forceRefresh), {
        method: "POST",
    });
    const payload = await response.json();
    if (!response.ok) {
        throw new Error(payload.detail || payload.error || "分析コメントを生成できませんでした。");
    }
    return payload;
}

function setAiInsightsChip(text, modifier = "idle") {
    if (!aiInsightsState) return;
    aiInsightsState.textContent = text;
    aiInsightsState.className = `ai-insights-chip${modifier ? ` is-${modifier}` : ""}`;
}

function renderAiInsightsPayload(payload, analysisName = "") {
    if (!aiInsightsTitle || !aiInsightsMeta || !aiInsightsNote || !aiInsightsOutput || !aiInsightsButton) {
        return;
    }

    const resolvedAnalysisName = payload?.analysis_name || analysisName || detailPageTitle?.textContent?.trim() || "";
    aiInsightsTitle.textContent = resolvedAnalysisName ? `${resolvedAnalysisName} 分析コメント` : "分析コメント";

    if (!payload?.generated) {
        currentAiInsightsPayload = payload || null;
        aiInsightsMeta.textContent = "分析ごとに生成し、画面を切り替えても同じ条件なら再表示されます。";
        aiInsightsNote.textContent = payload?.note || "まだ生成していません。";
        aiInsightsOutput.textContent = "";
        aiInsightsOutput.classList.add("hidden");
        aiInsightsButton.disabled = false;
        aiInsightsButton.textContent = "分析コメントを生成";
        setAiInsightsChip("未生成", "idle");
        return;
    }

    currentAiInsightsPayload = payload;
    const generatedAtLabel = payload.generated_at ? formatDateTime(payload.generated_at) : "";
    aiInsightsMeta.textContent = generatedAtLabel || "現在の分析条件に対応する解説です。";
    aiInsightsNote.textContent = payload.note || "現在の分析条件に対応する解説です。";
    aiInsightsOutput.textContent = payload.text || "";
    aiInsightsOutput.classList.toggle("hidden", !payload.text);
    aiInsightsButton.disabled = false;
    aiInsightsButton.textContent = payload.cached ? "分析コメントを再生成" : "分析コメントを更新";

    if (payload.mode === "rule_based") {
        setAiInsightsChip(payload.cached ? "要約保存済み" : "要約生成済み", "fallback");
    } else {
        setAiInsightsChip(payload.cached ? "保存済み" : "生成済み", "ready");
    }
}

function renderAiInsightsLoading(analysisName = "") {
    if (!aiInsightsTitle || !aiInsightsMeta || !aiInsightsNote || !aiInsightsOutput || !aiInsightsButton) {
        return;
    }

    const resolvedAnalysisName = analysisName || detailPageTitle?.textContent?.trim() || "";
    aiInsightsTitle.textContent = resolvedAnalysisName ? `${resolvedAnalysisName} 分析コメント` : "分析コメント";
    aiInsightsMeta.textContent = "現在の分析条件に対する解説を生成しています。";
    aiInsightsNote.textContent = "生成中です。完了すると画面切替後も保持されます。";
    aiInsightsOutput.textContent = "解説を生成しています...";
    aiInsightsOutput.classList.remove("hidden");
    aiInsightsButton.disabled = true;
    aiInsightsButton.textContent = "生成中...";
    setAiInsightsChip("生成中", "loading");
}

function syncDetailExportPanel(analysisName = "", options = {}) {
    if (!detailExportExcelButton || !detailExportTitle || !detailExportMeta || !detailExportNote || !detailExportScope) {
        return;
    }

    const {
        filters = activeDetailFilters,
        variantId = null,
        selectedActivity = "",
        selectedTransitionKey = "",
        caseId = "",
        filterDefs = buildDefaultFilterDefinitions(currentDetailColumnSettings),
    } = options;
    const resolvedAnalysisName = analysisName || detailPageTitle?.textContent?.trim() || "この分析";
    const normalizedTransitionLabel = String(selectedTransitionKey || "").replace("__TO__", " → ");
    const filterSummary = buildFilterSelectionSummary(filters, Array.isArray(filterDefs) ? filterDefs : []);
    const chips = [
        `${resolvedAnalysisName}レポート`,
        filterSummary,
    ];

    if (variantId !== null && variantId !== undefined) {
        chips.push(`Variant #${variantId}`);
    }
    if (selectedActivity) {
        chips.push(`アクティビティ: ${selectedActivity}`);
    }
    if (normalizedTransitionLabel) {
        chips.push(`遷移: ${normalizedTransitionLabel}`);
    }
    if (caseId) {
        chips.push(`ケース: ${caseId}`);
    }

    let metaText = "この分析画面に対応する内容だけを出力します。";
    if (analysisKey === "frequency") {
        metaText = "頻度分析に関連するサマリーと集計表だけを Excel にまとめます。";
    } else if (analysisKey === "transition") {
        metaText = "前後処理分析、ボトルネック、改善インパクトをまとめて出力します。";
    } else if (analysisKey === "pattern") {
        metaText = "処理順パターンの統合一覧、パターンサマリー、表示件数に応じた上位パターン詳細シートを出力します。";
    }

    const selectionItems = [];
    if (variantId !== null && variantId !== undefined) selectionItems.push(`Variant #${variantId}`);
    if (selectedActivity) selectionItems.push(`アクティビティ「${selectedActivity}」`);
    if (normalizedTransitionLabel) selectionItems.push(`遷移「${normalizedTransitionLabel}」`);
    if (caseId) selectionItems.push(`ケース「${caseId}」`);

    detailExportTitle.textContent = `${resolvedAnalysisName}のExcelレポート`;
    detailExportMeta.textContent = metaText;
    detailExportNote.textContent = selectionItems.length
        ? `現在の絞り込みと ${selectionItems.join(" / ")} の選択状態も反映して出力します。`
        : "現在の絞り込み条件を反映して出力します。";
    detailExportScope.innerHTML = chips
        .filter(Boolean)
        .map((item) => `<span class="detail-export-chip">${escapeHtml(item)}</span>`)
        .join("");
    detailExportExcelButton.textContent = `${resolvedAnalysisName}をExcel出力`;
}

// -----------------------------------------------------------------------------
// Filter state helpers
// -----------------------------------------------------------------------------

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

function formatVariantRatio(ratio) {
    return (Number(ratio || 0) * 100).toLocaleString("ja-JP", {
        maximumFractionDigits: 2,
    });
}

function formatDurationHours(hours) {
    return `${Number(hours || 0).toLocaleString("ja-JP", {
        minimumFractionDigits: 1,
        maximumFractionDigits: 2,
    })}h`;
}

function formatDurationSeconds(seconds) {
    return `${Number(seconds || 0).toLocaleString("ja-JP", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
    })} sec`;
}

function formatDashboardMetricNumber(value) {
    return Number(value || 0).toLocaleString("ja-JP");
}

function buildDeferredSectionMessage(defaultMessage = "補助集計を読み込んでいます...") {
    if (detailSupplementErrorMessage) {
        return `<p class="empty-state">${escapeHtml(detailSupplementErrorMessage)}</p>`;
    }
    return `<p class="panel-note">${escapeHtml(defaultMessage)}</p>`;
}

function formatDashboardCoverage(coveragePct) {
    return `${Number(coveragePct || 0).toLocaleString("ja-JP", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
    })}%`;
}

function buildDashboardCardsHtml(dashboard) {
    if (!dashboard && pendingDetailSupplementSections.has("dashboard")) {
        return `
            <section class="summary-panel-dashboard">
                <div class="dashboard-header">
                    <h2 class="dashboard-title">基本ダッシュボード</h2>
                    <p class="dashboard-copy">分析対象条件適用後の分析要約です。バリアント選択中も全体値を表示します。</p>
                </div>
                <div class="dashboard-empty">
                    ${buildDeferredSectionMessage("基本ダッシュボードを読み込んでいます...")}
                </div>
            </section>
        `;
    }

    if (!dashboard?.has_data) {
        return `
            <section class="summary-panel-dashboard">
                <div class="dashboard-header">
                    <h2 class="dashboard-title">基本ダッシュボード</h2>
                    <p class="dashboard-copy">分析対象条件適用後の分析要約です。バリアント選択中も全体値を表示します。</p>
                </div>
                <div class="dashboard-empty">
                    <p class="empty-state">条件に一致するデータがありません。</p>
                </div>
            </section>
        `;
    }

    const dashboardCards = [
        {
            label: "アクティビティ種類数",
            value: formatDashboardMetricNumber(dashboard.activity_type_count),
        },
        {
            label: "平均処理時間",
            value: dashboard.avg_case_duration_text || "0s",
        },
        {
            label: "中央処理時間",
            value: dashboard.median_case_duration_text || "0s",
        },
        {
            label: "最大処理時間",
            value: dashboard.max_case_duration_text || "0s",
        },
        {
            label: "上位10バリアントカバー率",
            value: formatDashboardCoverage(dashboard.top10_variant_coverage_pct),
        },
        {
            label: "最大ボトルネック",
            value: dashboard.top_bottleneck_transition_label || "-",
        },
        {
            label: "最大ボトルネック平均所要時間",
            value: dashboard.top_bottleneck_transition_label
                ? `Avg ${formatDurationHours(dashboard.top_bottleneck_avg_wait_hours)}`
                : "-",
            note: dashboard.top_bottleneck_transition_label || "対象遷移なし",
        },
    ];

    return `
        <section class="summary-panel-dashboard">
            <div class="dashboard-header">
                <h2 class="dashboard-title">基本ダッシュボード</h2>
                <p class="dashboard-copy">分析対象条件適用後の分析要約です。バリアント選択中も全体値を表示します。</p>
            </div>
            <div class="grid-auto">
                ${dashboardCards.map((card) => `
                    <article class="kpi-card">
                        <div>
                            <div class="kpi-label">${escapeHtml(card.label)}</div>
                            <div class="kpi-value">${escapeHtml(card.value)}</div>
                            ${card.note ? `<div class="kpi-sub">${escapeHtml(card.note)}</div>` : ""}
                        </div>
                    </article>
                `).join("")}
            </div>
        </section>
    `;
}

function formatRootCauseRatio(ratio) {
    return `${Number(ratio || 0).toLocaleString("ja-JP", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
    })}%`;
}

function formatImpactScore(score) {
    return Number(score || 0).toLocaleString("ja-JP", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
    });
}


function buildInsightsSectionHtml(insights) {
    if (!insights && pendingDetailSupplementSections.has("insights")) {
        return `
            <section class="summary-panel-insights">
                <div class="dashboard-header">
                    <h2 class="dashboard-title">自動インサイト</h2>
                    <p class="dashboard-copy">既存集計から重要ポイントを自動で要約しています。</p>
                </div>
                ${buildDeferredSectionMessage("自動インサイトを読み込んでいます...")}
            </section>
        `;
    }

    const items = Array.isArray(insights?.items) ? insights.items : [];
    const description = insights?.description || "既存集計から重要ポイントを自動で要約しています。";

    if (!items.length) {
        return `
            <section class="summary-panel-insights">
                <div class="dashboard-header">
                    <h2 class="dashboard-title">自動インサイト</h2>
                    <p class="dashboard-copy">${escapeHtml(description)}</p>
                </div>
                <p class="empty-state">条件に一致するデータがないため、インサイトを表示できません。</p>
            </section>
        `;
    }

    return `
        <section class="summary-panel-insights">
            <div class="dashboard-header">
                <h2 class="dashboard-title">自動インサイト</h2>
                <p class="dashboard-copy">${escapeHtml(description)}</p>
            </div>
            <ul class="insight-list">
                ${items.slice(0, 5).map((item, index) => `<li><span class="insight-icon">${["📊", "📈", "⚠️", "🔍", "✅"][index] || "•"}</span><span>${escapeHtml(item.text || "")}</span></li>`).join("")}
            </ul>
        </section>
    `;
}


function buildImpactSortLabel(sortKey) {
    switch (sortKey) {
        case "impact_share_pct":
            return "改善インパクト比率";
        case "avg_duration_sec":
            return "平均所要時間";
        case "case_count":
            return "ケース数";
        case "max_duration_sec":
            return "最大所要時間";
        case "impact_score":
        default:
            return "改善インパクト";
    }
}

function getDefaultImpactViewState() {
    return {
        sortKey: "impact_score",
        minCaseCount: "",
        minAvgDurationHours: "",
        displayLimit: 10,
    };
}

function getFilteredImpactRows(impact, viewState = getDefaultImpactViewState()) {
    const rows = Array.isArray(impact?.rows) ? impact.rows : [];
    const minimumCaseCount = Math.max(0, Number(viewState.minCaseCount) || 0);
    const minimumAvgDurationSec = Math.max(0, (Number(viewState.minAvgDurationHours) || 0) * 3600);
    const safeSortKey = viewState.sortKey || "impact_score";
    const safeDisplayLimit = Math.max(1, Number(viewState.displayLimit) || 10);

    const filteredRows = rows.filter((row) => (
        Number(row.case_count || 0) >= minimumCaseCount
        && Number(row.avg_duration_sec || 0) >= minimumAvgDurationSec
    ));

    filteredRows.sort((leftRow, rightRow) => {
        const primaryDiff = Number(rightRow[safeSortKey] || 0) - Number(leftRow[safeSortKey] || 0);
        if (primaryDiff !== 0) {
            return primaryDiff;
        }

        const impactDiff = Number(rightRow.impact_score || 0) - Number(leftRow.impact_score || 0);
        if (impactDiff !== 0) {
            return impactDiff;
        }

        return String(leftRow.transition_label || "").localeCompare(String(rightRow.transition_label || ""), "ja");
    });

    return filteredRows.slice(0, safeDisplayLimit).map((row, index) => ({
        ...row,
        rank: index + 1,
    }));
}

function buildImpactTableHtml(rows) {
    if (!rows.length) {
        return '<p class="empty-state">条件に一致する遷移がありません。</p>';
    }

    const headerLabels = [
        "順位",
        "遷移",
        "ケース数",
        "平均所要時間",
        "最大所要時間",
        "所要時間シェア(%)",
        "改善インパクト",
        "改善インパクト比率(%)",
    ];
    const headHtml = headerLabels.map((headerLabel) => `<th>${escapeHtml(headerLabel)}</th>`).join("");
    const bodyHtml = rows.map((row) => {
        const transitionKey = row.transition_key || buildTransitionKey(row.from_activity, row.to_activity);
        return `
            <tr data-impact-transition-key="${escapeHtml(transitionKey)}" tabindex="0" aria-selected="false">
                <td>${escapeHtml(row.rank)}</td>
                <td class="table-cell--wide"><div class="cell-scroll-wrapper">${escapeHtml(row.transition_label)}</div></td>
                <td>${escapeHtml(formatDashboardMetricNumber(row.case_count))}</td>
                <td>${escapeHtml(row.avg_duration_text)}</td>
                <td>${escapeHtml(row.max_duration_text)}</td>
                <td>${escapeHtml(formatRootCauseRatio(row.wait_share_pct))}</td>
                <td>${escapeHtml(formatImpactScore(row.impact_score))}</td>
                <td>${escapeHtml(formatRootCauseRatio(row.impact_share_pct))}</td>
            </tr>
        `;
    }).join("");

    return `
        <div class="table-wrap">
            <table>
                <thead><tr>${headHtml}</tr></thead>
                <tbody>${bodyHtml}</tbody>
            </table>
        </div>
    `;
}

function buildRootCauseGroupHtml(group) {
    const rows = Array.isArray(group?.rows) ? group.rows : [];
    const metaParts = [`対象列: ${group.column_name || "-"}`];
    const singleValueNotice = rows.length === 1
        ? "現在は1値のみのため、比較対象がありません。分析対象条件を広げると、値ごとの差を比較できます。"
        : "";

    if ((group.total_value_count || 0) > (group.returned_value_count || 0)) {
        metaParts.push(`平均処理時間が長い順に上位 ${group.returned_value_count} 件を表示`);
    } else {
        metaParts.push("平均処理時間が長い順に表示");
    }

    if (!rows.length) {
        return `
            <section class="root-cause-group">
                <div class="root-cause-group-head">
                    <h3>${escapeHtml(group.label || "原因分析")}</h3>
                    <p class="panel-note">${escapeHtml(metaParts.join(" / "))}</p>
                </div>
                <p class="empty-state">表示できる値がありません。</p>
            </section>
        `;
    }

    const tableRows = rows.map((row) => ({
        "値": row.value,
        "ケース数": formatDashboardMetricNumber(row.case_count),
        "ケース比率": formatRootCauseRatio(row.case_ratio_pct),
        "平均処理時間": row.avg_case_duration_text,
        "中央処理時間": row.median_case_duration_text,
        "最大処理時間": row.max_case_duration_text,
    }));

        return `
            <section class="root-cause-group">
                <div class="root-cause-group-head">
                    <h3>${escapeHtml(group.label || "原因分析")}</h3>
                    <p class="panel-note">${escapeHtml(metaParts.join(" / "))}</p>
                </div>
                ${buildTable(tableRows)}
                ${singleValueNotice ? `<p class="panel-note">${escapeHtml(singleValueNotice)}</p>` : ""}
            </section>
        `;
}

function buildRootCauseSectionHtml(rootCause) {
    if (!rootCause && pendingDetailSupplementSections.has("root_cause")) {
        return `
            <section class="root-cause-panel">
                <div class="result-header">
                    <div>
                        <h2>原因分析</h2>
                        <p class="result-meta">グループ/カテゴリー列ごとに、値別のケース処理時間を比較します。</p>
                    </div>
                </div>
                ${buildDeferredSectionMessage("原因分析を読み込んでいます...")}
            </section>
        `;
    }

    if (!rootCause?.has_data) {
        return `
            <section class="root-cause-panel">
                <div class="result-header">
                    <div>
                        <h2>原因分析</h2>
                        <p class="result-meta">グループ/カテゴリー列ごとに、値別のケース処理時間を比較します。</p>
                    </div>
                </div>
                <p class="empty-state">条件に一致するデータがありません。</p>
            </section>
        `;
    }

    if (!Array.isArray(rootCause.groups) || !rootCause.groups.length) {
        return `
            <section class="root-cause-panel">
                <div class="result-header">
                    <div>
                        <h2>原因分析</h2>
                        <p class="result-meta">グループ/カテゴリー列ごとに、値別のケース処理時間を比較します。</p>
                    </div>
                </div>
                <p class="empty-state">原因分析に使うグループ/カテゴリー列が未設定です。</p>
            </section>
        `;
    }

    return `
        <section class="root-cause-panel">
            <div class="result-header">
                <div>
                    <h2>原因分析</h2>
                    <p class="result-meta">TOP画面で設定したグループ/カテゴリー列ごとに、値別のケース処理時間を比較しています。</p>
                </div>
            </div>
            <div class="root-cause-group-list">
                ${rootCause.groups.map((group) => buildRootCauseGroupHtml(group)).join("")}
            </div>
        </section>
    `;
}

function buildImpactSectionHtml(impact, viewState = getDefaultImpactViewState()) {
    if (!impact && pendingDetailSupplementSections.has("impact")) {
        return `
            <section class="impact-panel">
                <div class="result-header">
                    <div>
                        <h2>改善インパクト分析</h2>
                        <p class="result-meta">平均所要時間 × 件数をもとに、改善効果の大きい遷移を表示しています。</p>
                    </div>
                </div>
                ${buildDeferredSectionMessage("改善インパクト分析を読み込んでいます...")}
            </section>
        `;
    }

    if (!impact?.has_data) {
        return `
            <section class="impact-panel">
                <div class="result-header">
                    <div>
                        <h2>改善インパクト分析</h2>
                        <p class="result-meta">平均所要時間 × 件数をもとに、改善効果の大きい遷移を表示しています。</p>
                    </div>
                </div>
                <p class="empty-state">条件に一致するデータがありません。</p>
            </section>
        `;
    }

    const rows = Array.isArray(impact.rows) ? impact.rows : [];
    if (!rows.length) {
        return `
            <section class="impact-panel">
                <div class="result-header">
                    <div>
                        <h2>改善インパクト分析</h2>
                        <p class="result-meta">平均所要時間 × 件数をもとに、改善効果の大きい遷移を表示しています。</p>
                    </div>
                </div>
                <p class="empty-state">表示できる遷移がありません。</p>
            </section>
        `;
    }

    const safeSortKey = viewState.sortKey || "impact_score";
    const safeMinCaseCount = String(viewState.minCaseCount || "");
    const safeMinAvgDurationHours = String(viewState.minAvgDurationHours || "");
    const safeDisplayLimit = String(viewState.displayLimit || 10);
    const filteredRows = getFilteredImpactRows(impact, viewState);
    const metaText = filteredRows.length
        ? `${buildImpactSortLabel(safeSortKey)}の高い順に ${filteredRows.length} 件を表示しています。`
        : "条件に一致する遷移がありません。";

    return `
        <section class="impact-panel">
            <div class="result-header">
                <div>
                    <h2>改善インパクト分析</h2>
                    <p class="result-meta">平均所要時間 × 件数をもとに、改善効果の大きい遷移を表示しています。</p>
                </div>
            </div>
            <div class="impact-controls" role="group" aria-label="改善インパクト分析 controls">
                <label class="field">
                    <span>並び順</span>
                    <select id="impact-sort-select">
                        <option value="impact_score"${safeSortKey === "impact_score" ? " selected" : ""}>改善インパクト順</option>
                        <option value="impact_share_pct"${safeSortKey === "impact_share_pct" ? " selected" : ""}>改善インパクト比率順</option>
                        <option value="avg_duration_sec"${safeSortKey === "avg_duration_sec" ? " selected" : ""}>平均所要時間順</option>
                        <option value="case_count"${safeSortKey === "case_count" ? " selected" : ""}>ケース数順</option>
                        <option value="max_duration_sec"${safeSortKey === "max_duration_sec" ? " selected" : ""}>最大所要時間順</option>
                    </select>
                </label>
                <label class="field">
                    <span>最低ケース数</span>
                    <input id="impact-min-case-count-input" type="number" min="0" step="1" value="${escapeHtml(safeMinCaseCount)}" placeholder="0">
                </label>
                <label class="field">
                    <span>最低平均所要時間(時間)</span>
                    <input id="impact-min-avg-hours-input" type="number" min="0" step="0.1" value="${escapeHtml(safeMinAvgDurationHours)}" placeholder="0">
                </label>
                <label class="field">
                    <span>表示件数</span>
                    <select id="impact-display-limit-select">
                        <option value="10"${safeDisplayLimit === "10" ? " selected" : ""}>Top 10</option>
                        <option value="20"${safeDisplayLimit === "20" ? " selected" : ""}>Top 20</option>
                        <option value="50"${safeDisplayLimit === "50" ? " selected" : ""}>Top 50</option>
                    </select>
                </label>
            </div>
            <p class="panel-note">${escapeHtml(metaText)}</p>
            <p class="panel-note">対象遷移 ${escapeHtml(formatDashboardMetricNumber(rows.length))} 件 / 表示遷移 ${escapeHtml(formatDashboardMetricNumber(filteredRows.length))} 件</p>
            ${buildImpactTableHtml(filteredRows)}
        </section>
    `;
}

// -----------------------------------------------------------------------------
// Variant, bottleneck, and case trace render helpers
// -----------------------------------------------------------------------------

function getVariantSequenceText(variant) {
    return Array.isArray(variant?.activities)
        ? variant.activities.join(" → ")
        : "";
}

function buildVariantCoverageHtml(coverage) {
    if (!coverage) {
        return '<p class="panel-note">カバー率を計算できませんでした。</p>';
    }

    const totalCount = Math.max(0, Number(coverage.total_case_count || 0));
    const coveredCount = Math.min(totalCount, Math.max(0, Number(coverage.covered_case_count || 0)));
    const otherCount = Math.max(0, totalCount - coveredCount);
    const coverageRatio = totalCount > 0 ? coveredCount / totalCount : 0;
    const coveragePct = Math.round(coverageRatio * 100);

    const donutSegments = [
        {
            label: coverage.display_label || `上位${coverage.displayed_variant_count}件`,
            ratio: coverageRatio,
            color: "#3b82f6",
            tooltip: `対象: ${coveredCount.toLocaleString("ja-JP")} 件 (${coveragePct}%)`,
        },
        ...(otherCount > 0 ? [{
            label: "その他",
            ratio: otherCount / Math.max(1, totalCount),
            color: "#e2e8f0",
            tooltip: `その他: ${otherCount.toLocaleString("ja-JP")} 件`,
        }] : []),
    ];

    return buildDonutChartMarkup(
        donutSegments,
        totalCount,
        coverage.display_label || `上位${coverage.displayed_variant_count}件カバー率`,
        `${coveragePct}%`,
    );
}

function getDefaultVariantViewState() {
    return {
        searchTerm: "",
        sortKey: "count",
        displayLimit: "10",
        page: 1,
    };
}

function buildVariantSearchText(variant) {
    return [
        `Variant #${variant?.variant_id || ""}`,
        variant?.pattern || "",
        getVariantSequenceText(variant),
    ]
        .join(" ")
        .toLocaleLowerCase("ja-JP");
}

function buildVariantCoveragePayload(variantItems, totalCaseCount, displayLimit = 10, options = {}) {
    const isAllDisplay = Boolean(options.isAllDisplay);
    const safeTotalCaseCount = Math.max(0, Number(totalCaseCount || 0));
    const safeLimit = isAllDisplay
        ? variantItems.length
        : Math.max(0, Number(displayLimit) || 0);
    const coveredItems = safeLimit > 0
        ? variantItems.slice(0, safeLimit)
        : variantItems.slice();
    const displayLabel = isAllDisplay
        ? "全件カバー率"
        : `上位${Number(coveredItems.length || 0).toLocaleString("ja-JP")}件カバー率`;
    const coveredCaseCountRaw = coveredItems.reduce(
        (sum, variant) => sum + Number(variant.count || 0),
        0,
    );
    const coveredCaseCount = Math.min(safeTotalCaseCount, Math.max(0, coveredCaseCountRaw));

    return {
        display_label: displayLabel,
        displayed_variant_count: coveredItems.length,
        covered_case_count: coveredCaseCount,
        total_case_count: safeTotalCaseCount,
        ratio: safeTotalCaseCount
            ? coveredCaseCount / safeTotalCaseCount
            : 0,
    };
}

function getFilteredSortedVariants(variants, viewState = getDefaultVariantViewState()) {
    const normalizedSearchTerm = String(viewState.searchTerm || "").trim().toLocaleLowerCase("ja-JP");
    const safeSortKey = viewState.sortKey || "count";
    const filteredVariants = variants.filter((variant) => (
        !normalizedSearchTerm || buildVariantSearchText(variant).includes(normalizedSearchTerm)
    ));

    filteredVariants.sort((leftVariant, rightVariant) => {
        const numericDiff = Number(rightVariant[safeSortKey] || 0) - Number(leftVariant[safeSortKey] || 0);
        if (numericDiff !== 0) {
            return numericDiff;
        }

        const countDiff = Number(rightVariant.count || 0) - Number(leftVariant.count || 0);
        if (countDiff !== 0) {
            return countDiff;
        }

        return String(leftVariant.pattern || "").localeCompare(String(rightVariant.pattern || ""), "ja");
    });

    return filteredVariants;
}

function getVariantPageState(variants, viewState = getDefaultVariantViewState()) {
    const filteredVariants = getFilteredSortedVariants(variants, viewState);
    const safeDisplayLimit = String(viewState.displayLimit || "10").trim().toLowerCase();
    const isAllDisplay = safeDisplayLimit === "all";
    const maxVisibleCount = isAllDisplay
        ? filteredVariants.length
        : Math.max(1, Number(safeDisplayLimit) || 10);
    const limitedVariants = filteredVariants.slice(0, maxVisibleCount);
    const pageSize = VARIANT_PAGE_SIZE;
    const totalPages = Math.max(1, Math.ceil(limitedVariants.length / pageSize) || 1);
    const currentPage = Math.min(Math.max(1, Number(viewState.page) || 1), totalPages);
    const startIndex = limitedVariants.length ? (currentPage - 1) * pageSize : 0;
    const endIndex = Math.min(limitedVariants.length, startIndex + pageSize);
    const visibleVariants = limitedVariants.slice(startIndex, endIndex);

    return {
        filteredVariants,
        limitedVariants,
        visibleVariants,
        maxVisibleCount,
        pageSize,
        currentPage,
        totalPages,
        isAllDisplay,
        startRowNumber: limitedVariants.length ? startIndex + 1 : 0,
        endRowNumber: limitedVariants.length ? endIndex : 0,
    };
}

function buildVariantSortLabel(sortKey) {
    switch (sortKey) {
    case "ratio":
        return "比率順";
    case "avg_case_duration_sec":
        return "平均処理時間順";
    case "activity_count":
        return "アクティビティ数順";
    case "count":
    default:
        return "件数順";
    }
}

function buildVariantPaginationPages(currentPage, totalPages) {
    if (totalPages <= 1) {
        return [];
    }

    const pages = [];
    const startPage = Math.max(1, currentPage - 2);
    const endPage = Math.min(totalPages, startPage + 4);
    const normalizedStartPage = Math.max(1, endPage - 4);

    if (normalizedStartPage > 1) {
        pages.push(1);
        if (normalizedStartPage > 2) {
            pages.push("ellipsis-start");
        }
    }

    for (let pageNumber = normalizedStartPage; pageNumber <= endPage; pageNumber += 1) {
        pages.push(pageNumber);
    }

    if (endPage < totalPages) {
        if (endPage < totalPages - 1) {
            pages.push("ellipsis-end");
        }
        pages.push(totalPages);
    }

    return pages;
}

function buildVariantPaginationHtml(pageState) {
    if (!pageState || pageState.totalPages <= 1) {
        return "";
    }

    const pageItems = buildVariantPaginationPages(pageState.currentPage, pageState.totalPages);

    return `
        <p class="result-pagination-meta">
            ${escapeHtml(pageState.currentPage)} / ${escapeHtml(pageState.totalPages)} ページ
        </p>
        <div class="result-pagination-actions variant-pagination-actions">
            <button
                type="button"
                class="ghost-link result-pagination-button variant-pagination-button"
                data-variant-page="${escapeHtml(Math.max(1, pageState.currentPage - 1))}"
                ${pageState.currentPage > 1 ? "" : "disabled"}
            >
                前へ
            </button>
            ${pageItems.map((pageItem) => {
        if (typeof pageItem !== "number") {
            return '<span class="variant-pagination-ellipsis" aria-hidden="true">…</span>';
        }

        return `
                    <button
                        type="button"
                        class="ghost-link result-pagination-button variant-pagination-button${pageItem === pageState.currentPage ? " is-active" : ""}"
                        data-variant-page="${escapeHtml(pageItem)}"
                        aria-pressed="${pageItem === pageState.currentPage ? "true" : "false"}"
                    >
                        ${escapeHtml(pageItem)}
                    </button>
                `;
    }).join("")}
            <button
                type="button"
                class="ghost-link result-pagination-button variant-pagination-button"
                data-variant-page="${escapeHtml(Math.min(pageState.totalPages, pageState.currentPage + 1))}"
                ${pageState.currentPage < pageState.totalPages ? "" : "disabled"}
            >
                次へ
            </button>
        </div>
    `;
}

function buildVariantTransitionKeys(activities = []) {
    const transitionKeys = [];

    for (let index = 0; index < activities.length - 1; index += 1) {
        const fromActivity = String(activities[index] || "").trim();
        const toActivity = String(activities[index + 1] || "").trim();
        if (!fromActivity || !toActivity) {
            continue;
        }
        transitionKeys.push(buildTransitionKey(fromActivity, toActivity));
    }

    return transitionKeys;
}

function buildLcsMatchPairs(baseActivities = [], targetActivities = []) {
    const baseLength = baseActivities.length;
    const targetLength = targetActivities.length;
    const dp = Array.from({ length: baseLength + 1 }, () => Array(targetLength + 1).fill(0));

    for (let baseIndex = baseLength - 1; baseIndex >= 0; baseIndex -= 1) {
        for (let targetIndex = targetLength - 1; targetIndex >= 0; targetIndex -= 1) {
            if (baseActivities[baseIndex] === targetActivities[targetIndex]) {
                dp[baseIndex][targetIndex] = dp[baseIndex + 1][targetIndex + 1] + 1;
            } else {
                dp[baseIndex][targetIndex] = Math.max(
                    dp[baseIndex + 1][targetIndex],
                    dp[baseIndex][targetIndex + 1],
                );
            }
        }
    }

    const matches = [];
    let baseIndex = 0;
    let targetIndex = 0;
    while (baseIndex < baseLength && targetIndex < targetLength) {
        if (baseActivities[baseIndex] === targetActivities[targetIndex]) {
            matches.push({
                baseIndex,
                targetIndex,
                activity: baseActivities[baseIndex],
            });
            baseIndex += 1;
            targetIndex += 1;
        } else if (dp[baseIndex + 1][targetIndex] >= dp[baseIndex][targetIndex + 1]) {
            baseIndex += 1;
        } else {
            targetIndex += 1;
        }
    }

    return matches;
}

function buildVariantDiffState(variants, selectedVariantId) {
    const emptyState = {
        enabled: false,
        selectedIsBaseline: false,
        baselineVariantId: null,
        addedActivities: [],
        skippedActivities: [],
        branchPoints: [],
        addedActivityNames: new Set(),
        addedTransitionKeys: new Set(),
        commonTransitionKeys: new Set(),
    };

    if (selectedVariantId === null || !Array.isArray(variants) || !variants.length) {
        return emptyState;
    }

    const baselineVariant = variants[0];
    const selectedVariant = variants.find(
        (variant) => Number(variant.variant_id) === Number(selectedVariantId),
    );

    if (!baselineVariant || !selectedVariant) {
        return emptyState;
    }

    if (Number(baselineVariant.variant_id) === Number(selectedVariant.variant_id)) {
        return {
            ...emptyState,
            selectedIsBaseline: true,
            baselineVariantId: baselineVariant.variant_id,
        };
    }

    const baselineActivities = Array.isArray(baselineVariant.activities) ? baselineVariant.activities : [];
    const selectedActivities = Array.isArray(selectedVariant.activities) ? selectedVariant.activities : [];
    const matches = buildLcsMatchPairs(baselineActivities, selectedActivities);
    const matchedBaseIndices = new Set(matches.map((item) => item.baseIndex));
    const matchedTargetIndices = new Set(matches.map((item) => item.targetIndex));
    const addedActivities = selectedActivities.filter((_, index) => !matchedTargetIndices.has(index));
    const skippedActivities = baselineActivities.filter((_, index) => !matchedBaseIndices.has(index));
    const baselineTransitionKeys = new Set(buildVariantTransitionKeys(baselineActivities));
    const selectedTransitionKeys = buildVariantTransitionKeys(selectedActivities);
    const selectedActivityNames = new Set(selectedActivities.map((activity) => String(activity || "").trim()).filter(Boolean));
    const baselineActivityNames = new Set(baselineActivities.map((activity) => String(activity || "").trim()).filter(Boolean));
    const branchPoints = [];
    let previousBaseIndex = -1;
    let previousTargetIndex = -1;

    [...matches, { baseIndex: baselineActivities.length, targetIndex: selectedActivities.length }].forEach((match) => {
        const skippedSlice = baselineActivities.slice(previousBaseIndex + 1, match.baseIndex);
        const addedSlice = selectedActivities.slice(previousTargetIndex + 1, match.targetIndex);

        if (skippedSlice.length || addedSlice.length) {
            const focusActivities = [];
            const previousAnchor = previousBaseIndex >= 0
                ? baselineActivities[previousBaseIndex]
                : "";
            const nextAnchor = match.baseIndex < baselineActivities.length
                ? baselineActivities[match.baseIndex]
                : "";

            if (previousAnchor) {
                focusActivities.push(previousAnchor);
            }
            focusActivities.push(...addedSlice.filter(Boolean));
            if (nextAnchor) {
                focusActivities.push(nextAnchor);
            }
            branchPoints.push({
                focusId: `branch-${branchPoints.length + 1}`,
                anchor: `${previousBaseIndex >= 0 ? baselineActivities[previousBaseIndex] : "開始"} → ${match.baseIndex < baselineActivities.length ? baselineActivities[match.baseIndex] : "終了"}`,
                addedActivities: [...new Set(addedSlice.filter(Boolean))],
                skippedActivities: [...new Set(skippedSlice.filter(Boolean))],
                focusActivities,
                focusTransitions: buildVariantTransitionKeys(focusActivities),
            });
        }

        previousBaseIndex = match.baseIndex;
        previousTargetIndex = match.targetIndex;
    });

    return {
        enabled: true,
        selectedIsBaseline: false,
        baselineVariantId: baselineVariant.variant_id,
        addedActivities: [...new Set(addedActivities.filter(Boolean))],
        skippedActivities: [...new Set(skippedActivities.filter(Boolean))],
        branchPoints,
        addedActivityNames: new Set(
            [...selectedActivityNames].filter((activityName) => !baselineActivityNames.has(activityName)),
        ),
        addedTransitionKeys: new Set(
            selectedTransitionKeys.filter((transitionKey) => !baselineTransitionKeys.has(transitionKey)),
        ),
        commonTransitionKeys: new Set(
            selectedTransitionKeys.filter((transitionKey) => baselineTransitionKeys.has(transitionKey)),
        ),
    };
}

function buildVariantDiffBadgeList(items, toneClass, emptyText) {
    if (!Array.isArray(items) || !items.length) {
        return `<span class="variant-diff-empty">${escapeHtml(emptyText)}</span>`;
    }

    return items.slice(0, 5).map((item) => `
        <span class="variant-diff-badge ${toneClass}">${escapeHtml(item)}</span>
    `).join("") + (items.length > 5
        ? `<span class="variant-diff-badge variant-diff-badge--more">+${escapeHtml(items.length - 5)}</span>`
        : "");
}

function buildVariantBranchBadgeList(branchPoints, activeFocusId = "") {
    if (!Array.isArray(branchPoints) || !branchPoints.length) {
        return '<span class="variant-diff-empty">分岐ポイントはありません。</span>';
    }

    return branchPoints.slice(0, 4).map((branchPoint) => `
        <button
            type="button"
            class="variant-diff-badge variant-diff-badge--branch${branchPoint.focusId === activeFocusId ? " variant-diff-badge--branch-active" : ""}"
            data-branch-focus-id="${escapeHtml(branchPoint.focusId || "")}"
            title="${escapeHtml(branchPoint.anchor || "")}"
        >${escapeHtml(branchPoint.anchor)}</button>
    `).join("") + (branchPoints.length > 4
        ? `<span class="variant-diff-badge variant-diff-badge--more">+${escapeHtml(branchPoints.length - 4)}</span>`
        : "");
}

function buildVariantDiffHtml(diffState, activeFocusId = "") {
    if (!diffState?.selectedIsBaseline && !diffState?.enabled) {
        return "";
    }

    if (diffState.selectedIsBaseline) {
        return `
            <div class="variant-diff-summary">
                <p class="panel-note">このバリアントが比較基準です。全体で最も多いバリアントを基準フローとして扱います。</p>
            </div>
        `;
    }

    return `
        <div class="variant-diff-summary">
            <p class="panel-note">Variant #${escapeHtml(diffState.baselineVariantId)} を基準に差分を比較しています。</p>
            <div class="variant-diff-grid">
                <div class="variant-diff-group">
                    <span class="variant-diff-label">追加アクティビティ</span>
                    <div class="variant-diff-badges">
                        ${buildVariantDiffBadgeList(diffState.addedActivities, "variant-diff-badge--added", "追加アクティビティはありません。")}
                    </div>
                </div>
                <div class="variant-diff-group">
                    <span class="variant-diff-label">スキップしたアクティビティ</span>
                    <div class="variant-diff-badges">
                        ${buildVariantDiffBadgeList(diffState.skippedActivities, "variant-diff-badge--skipped", "スキップしたアクティビティはありません。")}
                    </div>
                </div>
                <div class="variant-diff-group">
                    <span class="variant-diff-label">分岐ポイント</span>
                    <div class="variant-diff-badges">
                        ${buildVariantBranchBadgeList(diffState.branchPoints, activeFocusId)}
                    </div>
                </div>
            </div>
        </div>
    `;
}

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

function buildVariantSelectionState(variants, selectedVariantId) {
    if (selectedVariantId === null) {
        return {
            title: "全体表示中",
            meta: "パターン / バリアントを選択すると、そのルートに属するケースだけでフロー図を再描画します。",
            sequence: "現在は全ケースを使ったフロー図を表示しています。",
            titleAttribute: "全ケースを使ったフロー図を表示しています。",
        };
    }

    const selectedVariant = variants.find(
        (variant) => Number(variant.variant_id) === Number(selectedVariantId)
    );

    if (!selectedVariant) {
        return {
            title: "パターン / バリアント情報なし",
            meta: "選択中のパターン / バリアント情報を取得できませんでした。",
            sequence: "",
            titleAttribute: "",
        };
    }

    const sequenceText = getVariantSequenceText(selectedVariant);
    const patternIndex = Number.isInteger(Number(selectedVariant.pattern_index))
        ? Number(selectedVariant.pattern_index) + 1
        : null;
    const titleParts = [];
    if (patternIndex !== null) {
        titleParts.push(`Pattern #${patternIndex}`);
    }
    titleParts.push(`Variant #${selectedVariant.variant_id}`);
    titleParts.push(`${formatVariantRatio(selectedVariant.ratio)}%`);
    titleParts.push(`${Number(selectedVariant.count || 0).toLocaleString("ja-JP")}件`);
    return {
        title: titleParts.join(" / "),
        meta: "選択中のルートに属するケースだけでフロー図を表示しています。",
        sequence: sequenceText,
        titleAttribute: sequenceText,
    };
}

function buildVariantRowCell(label, value, className = "") {
    return `
        <span class="variant-row-cell ${className}">
            <span class="variant-row-cell-label">${escapeHtml(label)}</span>
            <span class="variant-row-cell-value">${escapeHtml(value)}</span>
        </span>
    `;
}

function buildVariantCardsHtml(variants, selectedVariantId = null, emptyMessage = "表示できるルートがありません。", runId = "", rankOffset = 0) {
    if (!variants.length) {
        return `<p class="empty-state">${escapeHtml(emptyMessage)}</p>`;
    }

    return variants
        .map((variant, index) => {
            const displayRank = rankOffset + index + 1;
            const isSelected = Number(variant.variant_id) === Number(selectedVariantId);
            const sequenceText = getVariantSequenceText(variant);
            const caseCountText = Number(variant.count || 0).toLocaleString("ja-JP");
            const patternIndex = Number.isInteger(Number(variant.pattern_index))
                ? Number(variant.pattern_index)
                : null;
            const patternNumber = patternIndex !== null ? patternIndex + 1 : null;
            const detailHref = (runId && patternIndex !== null)
                ? buildPatternDetailHref(runId, patternIndex)
                : "";
            const routeLabel = patternNumber !== null
                ? `Pattern #${patternNumber} / Variant #${variant.variant_id}`
                : `Variant #${variant.variant_id}`;
            const repeatFlag = String(variant.repeat_flag || "").trim();
            const cardTitle = [
                `#${displayRank}`,
                routeLabel,
                `繰り返し ${repeatFlag || "なし"}`,
                `${formatVariantRatio(variant.ratio)}% / ${caseCountText}件`,
                `平均処理時間 ${variant.avg_case_duration_text || "0s"}`,
                sequenceText,
            ].join("\n");

            const patternCellHtml = detailHref
                ? `<span class="variant-row-cell variant-row-cell--pattern">
                        <span class="variant-row-cell-label">パターン / バリアント</span>
                        <a href="${detailHref}" class="variant-row-cell-value variant-row-pattern-link" title="パターン詳細ページへ">${escapeHtml(routeLabel)}</a>
                    </span>`
                : buildVariantRowCell("パターン / バリアント", routeLabel, "variant-row-cell--pattern");

            const sequenceCellHtml = `<span class="variant-row-cell variant-row-cell--sequence" title="${escapeHtml(sequenceText)}">
                    <span class="variant-row-cell-label">ルート</span>
                    <span class="variant-row-cell-value">${escapeHtml(sequenceText)}</span>
                </span>`;

            return `
                <article class="variant-row${isSelected ? " variant-row--selected" : ""}">
                    <div
                        role="button"
                        tabindex="0"
                        class="variant-row-main"
                        data-variant-id="${escapeHtml(variant.variant_id)}"
                        aria-pressed="${isSelected ? "true" : "false"}"
                        title="${escapeHtml(cardTitle)}"
                    >
                        ${buildVariantRowCell("順位", `#${displayRank}`, "variant-row-cell--rank")}
                        ${patternCellHtml}
                        ${buildVariantRowCell("繰り返し", repeatFlag || "なし", "variant-row-cell--repeat")}
                        ${buildVariantRowCell("件数", `${caseCountText}件`, "variant-row-cell--count")}
                        ${buildVariantRowCell("比率", `${formatVariantRatio(variant.ratio)}%`, "variant-row-cell--ratio")}
                        ${buildVariantRowCell("平均処理時間", variant.avg_case_duration_text || "0s", "variant-row-cell--duration")}
                        ${sequenceCellHtml}
                    </div>
                </article>
            `;
        })
        .join("");
}

function buildBottleneckCardsHtml(items, kind, selectionState = {}) {
    if (!Array.isArray(items) || !items.length) {
        return '<p class="empty-state">表示できるボトルネックがありません。</p>';
    }

    return items
        .map((item, index) => {
            const title = kind === "activity"
                ? item.activity
                : `${item.from_activity} → ${item.to_activity}`;
            const itemActivity = kind === "activity" ? item.activity : "";
            const itemTransitionKey = kind === "transition"
                ? item.transition_key || buildTransitionKey(item.from_activity, item.to_activity)
                : "";
            const isSelected = kind === "activity"
                ? itemActivity === selectionState.selectedActivity
                : itemTransitionKey === selectionState.selectedTransitionKey;
            const itemTitle = [
                title,
                `Avg ${formatDurationHours(item.avg_duration_hours)}`,
                `Median ${formatDurationHours(item.median_duration_hours)}`,
                `Max ${formatDurationHours(item.max_duration_hours)}`,
            ].join("\n");

            return `
                <button
                    type="button"
                    class="bottleneck-card${isSelected ? " bottleneck-card--selected" : ""}"
                    data-bottleneck-kind="${escapeHtml(kind)}"
                    data-activity="${escapeHtml(itemActivity)}"
                    data-transition-key="${escapeHtml(itemTransitionKey)}"
                    data-from-activity="${escapeHtml(item.from_activity || "")}"
                    data-to-activity="${escapeHtml(item.to_activity || "")}"
                    aria-pressed="${isSelected ? "true" : "false"}"
                    title="${escapeHtml(itemTitle)}"
                >
                    <div class="bottleneck-card-head">
                        <span class="bottleneck-card-rank">#${escapeHtml(index + 1)}</span>
                        <strong class="bottleneck-card-title">${escapeHtml(title)}</strong>
                    </div>
                    <p class="bottleneck-card-primary">Avg ${escapeHtml(formatDurationHours(item.avg_duration_hours))}</p>
                    <p class="bottleneck-card-meta">
                        ${escapeHtml(Number(item.count || 0).toLocaleString("ja-JP"))} intervals
                        / ${escapeHtml(Number(item.case_count || 0).toLocaleString("ja-JP"))} cases
                    </p>
                    <p class="bottleneck-card-secondary">
                        Median ${escapeHtml(formatDurationHours(item.median_duration_hours))}
                        / Max ${escapeHtml(formatDurationHours(item.max_duration_hours))}
                    </p>
                </button>
            `;
        })
        .join("");
}

function buildCaseDrilldownTable(rows) {
    if (!Array.isArray(rows) || !rows.length) {
        return '<p class="empty-state">表示できるケースがありません。</p>';
    }

    const tableRows = rows.map((row) => ({
        case_id: row.case_id,
        duration_sec: formatDurationSeconds(row.duration_sec),
        duration_text: row.duration_text,
        from_time: row.from_time,
        to_time: row.to_time,
    }));

    return buildTable(tableRows);
}

function buildCaseTraceSummaryHtml(caseId, summary) {
    if (!summary) {
        return "";
    }

    const summaryCards = [
        { label: "ケースID", value: caseId },
        { label: "イベント数", value: Number(summary.event_count || 0).toLocaleString("ja-JP") },
        { label: "開始時刻", value: formatDateTime(summary.start_time) },
        { label: "終了時刻", value: formatDateTime(summary.end_time) },
        { label: "総処理時間", value: summary.total_duration_text || "-" },
        { label: "総処理時間(sec)", value: formatDurationSeconds(summary.total_duration_sec) },
    ];

    return `
        <div class="case-trace-summary-grid">
            ${summaryCards.map((item) => `
                <article class="case-trace-summary-card">
                    <span class="summary-label">${escapeHtml(item.label)}</span>
                    <strong>${escapeHtml(item.value)}</strong>
                </article>
            `).join("")}
        </div>
    `;
}

function buildCaseTraceEventsTable(events) {
    if (!Array.isArray(events) || !events.length) {
        return '<p class="empty-state">表示できるイベントがありません。</p>';
    }

    const eventRows = events.map((eventRow) => ({
        "順番": eventRow.sequence_no,
        "アクティビティ": eventRow.activity,
        "時刻": formatDateTime(eventRow.timestamp),
        "次アクティビティ": eventRow.next_activity || "完了",
        "次イベントまでの所要時間": eventRow.wait_to_next_text || "-",
    }));

    return buildTable(eventRows);
}

// -----------------------------------------------------------------------------
// Process map selection and heatmap decorators
// -----------------------------------------------------------------------------

function applyHeatClass(targetElement, heatEntry) {
    if (!targetElement) {
        return;
    }

    for (let level = 1; level <= 5; level += 1) {
        targetElement.classList.remove(`heat-${level}`);
    }

    if (heatEntry?.heat_class) {
        targetElement.classList.add(heatEntry.heat_class);
    }
}

function collectSelectedTransitionActivities(svgElement, selectedTransitionKey) {
    const activityNames = new Set();

    if (!selectedTransitionKey) {
        return activityNames;
    }

    svgElement.querySelectorAll(".process-map-edge").forEach((edgePathElement) => {
        if ((edgePathElement.dataset.transitionKey || "") !== selectedTransitionKey) {
            return;
        }

        const sourceActivity = edgePathElement.dataset.source || "";
        const targetActivity = edgePathElement.dataset.target || "";

        if (sourceActivity) {
            activityNames.add(sourceActivity);
        }
        if (targetActivity) {
            activityNames.add(targetActivity);
        }
    });

    return activityNames;
}

function applyProcessMapDecorators(viewportElement, options = {}) {
    const {
        activityHeatmap = {},
        transitionHeatmap = {},
        selectedActivity = "",
        selectedTransitionKey = "",
        caseTraceActivities = new Set(),
        caseTraceTransitions = new Set(),
        variantDiffState = null,
        variantBranchFocusState = null,
    } = options;
    const svgElement = viewportElement.querySelector("svg.process-map-svg");

    if (!svgElement) {
        return;
    }

    const hasCaseTraceSelection = caseTraceActivities.size > 0 || caseTraceTransitions.size > 0;
    const hasBranchFocus = Boolean(
        (variantBranchFocusState?.focusActivities || []).length
        || (variantBranchFocusState?.focusTransitions || []).length
    );
    const branchFocusActivities = new Set(variantBranchFocusState?.focusActivities || []);
    const branchFocusTransitions = new Set(variantBranchFocusState?.focusTransitions || []);
    const hasSelection = Boolean(selectedActivity || selectedTransitionKey || hasCaseTraceSelection || hasBranchFocus);
    const selectedTransitionActivities = collectSelectedTransitionActivities(svgElement, selectedTransitionKey);
    const hasVariantDiff = Boolean(variantDiffState?.enabled);
    const addedActivityNames = variantDiffState?.addedActivityNames || new Set();
    const addedTransitionKeys = variantDiffState?.addedTransitionKeys || new Set();
    const commonTransitionKeys = variantDiffState?.commonTransitionKeys || new Set();

    svgElement.querySelectorAll(".process-map-node-group").forEach((nodeGroupElement) => {
        const nodeRectElement = nodeGroupElement.querySelector(".process-map-node");
        const activityName = nodeRectElement?.dataset.activity || "";
        const isSelected = (Boolean(selectedActivity) && activityName === selectedActivity)
            || selectedTransitionActivities.has(activityName)
            || caseTraceActivities.has(activityName);

        applyHeatClass(nodeRectElement, activityHeatmap[activityName]);
        nodeGroupElement.classList.toggle("variant-diff-added", hasVariantDiff && addedActivityNames.has(activityName));
        nodeGroupElement.classList.toggle("variant-diff-common", hasVariantDiff && !addedActivityNames.has(activityName));
        nodeGroupElement.classList.toggle("branch-focus-selected", hasBranchFocus && branchFocusActivities.has(activityName));
        nodeGroupElement.classList.toggle("branch-focus-dimmed", hasBranchFocus && !branchFocusActivities.has(activityName));
        nodeGroupElement.classList.toggle("is-selected", isSelected);
        nodeGroupElement.classList.toggle("is-dimmed", hasSelection && !isSelected);
    });

    svgElement.querySelectorAll(".process-map-edge").forEach((edgePathElement) => {
        const transitionKey = edgePathElement.dataset.transitionKey || "";
        const isSelected = (Boolean(selectedTransitionKey) && transitionKey === selectedTransitionKey)
            || caseTraceTransitions.has(transitionKey);

        applyHeatClass(edgePathElement, transitionHeatmap[transitionKey]);
        edgePathElement.classList.toggle("variant-diff-added", hasVariantDiff && addedTransitionKeys.has(transitionKey));
        edgePathElement.classList.toggle("variant-diff-common", hasVariantDiff && commonTransitionKeys.has(transitionKey));
        edgePathElement.classList.toggle("branch-focus-selected", hasBranchFocus && branchFocusTransitions.has(transitionKey));
        edgePathElement.classList.toggle("branch-focus-dimmed", hasBranchFocus && !branchFocusTransitions.has(transitionKey));
        edgePathElement.classList.toggle("is-selected", isSelected);
        edgePathElement.classList.toggle("is-dimmed", hasSelection && !isSelected);
    });

    svgElement.querySelectorAll(".process-map-edge-label").forEach((edgeLabelElement) => {
        const transitionKey = edgeLabelElement.dataset.transitionKey || "";
        const isSelected = (Boolean(selectedTransitionKey) && transitionKey === selectedTransitionKey)
            || caseTraceTransitions.has(transitionKey);

        edgeLabelElement.classList.toggle("variant-diff-added", hasVariantDiff && addedTransitionKeys.has(transitionKey));
        edgeLabelElement.classList.toggle("variant-diff-common", hasVariantDiff && commonTransitionKeys.has(transitionKey));
        edgeLabelElement.classList.toggle("branch-focus-selected", hasBranchFocus && branchFocusTransitions.has(transitionKey));
        edgeLabelElement.classList.toggle("branch-focus-dimmed", hasBranchFocus && !branchFocusTransitions.has(transitionKey));
        edgeLabelElement.classList.toggle("is-selected", isSelected);
        edgeLabelElement.classList.toggle("is-dimmed", hasSelection && !isSelected);
    });
}

// -----------------------------------------------------------------------------
// Analysis panel renderers
// -----------------------------------------------------------------------------

function renderSummary(data, analysis) {
    const rowCount = analysis.row_count ?? analysis.rows.length;

    summaryPanel.className = "summary-panel";
    const summaryCardsHtml = [
        {
            label: "入力ファイル",
            value: data.source_file_name,
        },
        {
            label: "対象ケース数 / 対象イベント数",
            value: `${data.case_count} / ${data.event_count}`,
        },
        {
            label: "表示件数",
            value: rowCount,
        },
    ].map((card) => `
        <article class="summary-card">
            <span class="summary-label">${escapeHtml(card.label)}</span>
            <strong>${escapeHtml(card.value)}</strong>
        </article>
    `).join("");

    summaryPanel.innerHTML = `
        ${summaryCardsHtml}
        ${buildInsightsSectionHtml(data.insights)}
        ${buildDashboardCardsHtml(data.dashboard)}
        ${buildRootCauseSectionHtml(data.root_cause)}
    `;
}

function compactPatternLabel(patternText = "") {
    const steps = String(patternText || "")
        .split(/\s*(?:→|->|⇒)\s*/u)
        .map((step) => step.trim())
        .filter(Boolean);

    if (steps.length > 3) {
        return `${steps.slice(0, 3).join(" → ")} → ...（全${steps.length}ステップ）`;
    }

    return String(patternText || "").trim() || "-";
}

function buildDetailTableRows(analysisKeyName, rows, rowOffset = 0) {
    return (rows || []).map((row, index) => {
        if (analysisKeyName === "frequency") {
            return {
                "アクティビティ名": row["アクティビティ"] ?? row["アクティビティ名"] ?? "-",
                "イベント件数": row["イベント件数"] ?? "-",
                "ケース数": row["ケース数"] ?? "-",
                "平均処理時間(分)": row["平均処理時間(分)"] ?? row["平均時間(分)"] ?? "-",
                "75%ile(分)": row["75%点(分)"] ?? "-",
                "イベント占有率(%)": row["イベント占有率(%)"] ?? row["event_ratio_pct"] ?? "-",
                __rowIndex: rowOffset + index,
            };
        }

        if (analysisKeyName === "transition") {
            return {
                "遷移名": row["遷移名"] ?? [row["前処理アクティビティ名"], row["後処理アクティビティ名"]].filter(Boolean).join(" → "),
                "ケース数": row["ケース数"] ?? row["遷移件数"] ?? "-",
                "平均所要時間(分)": row["平均所要時間(分)"] ?? row["平均時間(分)"] ?? row["平均待ち時間(分)"] ?? "-",
                "中央値処理時間(分)": row["中央値所要時間(分)"] ?? row["中央値時間(分)"] ?? "-",
                "最大処理時間(分)": row["最大所要時間(分)"] ?? row["最大時間(分)"] ?? "-",
                "割合(%)": row["割合(%)"] ?? row["遷移比率(%)"] ?? "-",
                __rowIndex: rowOffset + index,
            };
        }

        if (analysisKeyName === "pattern") {
            const patternText = row["パターン"] ?? row["処理順パターン"] ?? row["パターン / バリアント"] ?? "";
            return {
                "パターン": compactPatternLabel(patternText),
                "ケース数": row["ケース数"] ?? "-",
                "比率(%)": row["比率(%)"] ?? row["ケース比率(%)"] ?? "-",
                "平均ケース処理時間(分)": row["平均ケース処理時間(分)"] ?? row["平均処理時間(分)"] ?? "-",
                "繰り返し": row["繰り返し"] ?? "-",
                "改善優先度": row["改善優先度スコア"] ?? "-",
                __rowIndex: rowOffset + index,
            };
        }

        return { ...row, __rowIndex: rowOffset + index };
    });
}

function renderResult(analysis, runId = "", onPageChange = null) {
    const rowOffset = Number(analysis.row_offset || 0);
    const rowCount = analysis.row_count ?? analysis.rows.length;
    const returnedRowCount = analysis.returned_row_count ?? analysis.rows.length;
    const tableRows = buildDetailTableRows(analysisKey, analysis.rows, rowOffset);
    const resultMeta = returnedRowCount < rowCount
        ? `全 ${escapeHtml(rowCount)} 件中、先頭 ${escapeHtml(returnedRowCount)} 件を表示`
        : `全 ${escapeHtml(rowCount)} 件を表示`;
    const pageStart = analysis.page_start_row_number ?? (returnedRowCount ? rowOffset + 1 : 0);
    const pageEnd = analysis.page_end_row_number ?? (rowOffset + returnedRowCount);
    const paginationHtml = rowCount > DETAIL_ROW_LIMIT
        ? `
            <div class="result-pagination">
                <p class="result-pagination-meta">${escapeHtml(pageStart)} - ${escapeHtml(pageEnd)} / ${escapeHtml(rowCount)} 件</p>
                <div class="result-pagination-actions">
                    <button
                        type="button"
                        id="detail-prev-page-button"
                        class="ghost-link result-pagination-button"
                        ${analysis.has_previous_page ? "" : "disabled"}
                    >
                        前のページ
                    </button>
                    <button
                        type="button"
                        id="detail-next-page-button"
                        class="ghost-link result-pagination-button"
                        ${analysis.has_next_page ? "" : "disabled"}
                    >
                        次のページ
                    </button>
                </div>
            </div>
        `
        : "";

    resultPanel.className = "result-panel";
    resultPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>${escapeHtml(analysis.analysis_name)}</h2>
                <p class="result-meta">${resultMeta}</p>
                ${returnedRowCount < rowCount ? '<p class="result-meta">大量データでは画面停止を防ぐため、詳細表は一部のみ取得しています。</p>' : ""}
                ${analysis.excel_file ? `<p class="excel-path">Excel: ${escapeHtml(analysis.excel_file)}</p>` : ""}
            </div>
        </div>
        ${buildTable(tableRows, { analysisKey, runId })}
        ${paginationHtml}
    `;

    if (!onPageChange || rowCount <= DETAIL_ROW_LIMIT) {
        return;
    }

    const previousPageButton = document.getElementById("detail-prev-page-button");
    const nextPageButton = document.getElementById("detail-next-page-button");

    if (previousPageButton && analysis.has_previous_page) {
        previousPageButton.addEventListener("click", () => {
            onPageChange(analysis.previous_row_offset ?? 0);
        });
    }

    if (nextPageButton && analysis.has_next_page) {
        nextPageButton.addEventListener("click", () => {
            onPageChange(analysis.next_row_offset ?? pageEnd);
        });
    }
}

// -----------------------------------------------------------------------------
// Process map graph rendering
// -----------------------------------------------------------------------------

function getInitialPatternFlowSettings(totalPatternCount) {
    if (totalPatternCount >= 50000) {
        return { patterns: 10, activities: 20, connections: 15, labels: 0 };
    }

    if (totalPatternCount >= 10000) {
        return { patterns: 15, activities: 30, connections: 20, labels: 0 };
    }

    if (totalPatternCount >= 2000) {
        return { patterns: 25, activities: 45, connections: 30, labels: 10 };
    }

    if (totalPatternCount >= 500) {
        return { patterns: 40, activities: 60, connections: 40, labels: 30 };
    }

    return { patterns: 100, activities: 100, connections: 100, labels: 100 };
}

function wrapJapaneseLabel(text, maxCharsPerLine = 12, maxLines = 2) {
    const characters = Array.from(String(text));
    const lines = [];

    for (let index = 0; index < characters.length; index += maxCharsPerLine) {
        lines.push(characters.slice(index, index + maxCharsPerLine).join(""));
    }

    if (lines.length > maxLines) {
        const visibleLines = lines.slice(0, maxLines);
        const lastLineCharacters = Array.from(visibleLines[maxLines - 1]).slice(0, Math.max(0, maxCharsPerLine - 1));
        visibleLines[maxLines - 1] = `${lastLineCharacters.join("")}…`;
        return visibleLines;
    }

    return lines;
}

function reindexLayerNodes(layerNodes) {
    layerNodes.forEach((node, index) => {
        node.orderScore = index;
    });
}

function countEdgeCrossings(edges, nodeLookup) {
    let crossingScore = 0;

    for (let leftIndex = 0; leftIndex < edges.length; leftIndex += 1) {
        const leftEdge = edges[leftIndex];
        const leftSource = nodeLookup.get(leftEdge.source);
        const leftTarget = nodeLookup.get(leftEdge.target);

        if (!leftSource || !leftTarget) {
            continue;
        }

        for (let rightIndex = leftIndex + 1; rightIndex < edges.length; rightIndex += 1) {
            const rightEdge = edges[rightIndex];
            const rightSource = nodeLookup.get(rightEdge.source);
            const rightTarget = nodeLookup.get(rightEdge.target);

            if (!rightSource || !rightTarget) {
                continue;
            }

            const sourceDiff = leftSource.orderScore - rightSource.orderScore;
            const targetDiff = leftTarget.orderScore - rightTarget.orderScore;

            if (sourceDiff === 0 || targetDiff === 0) {
                continue;
            }

            if (sourceDiff * targetDiff < 0) {
                crossingScore += Math.min(leftEdge.count, rightEdge.count);
            }
        }
    }

    return crossingScore;
}

function countLayerCrossings(layer, edges, nodeLookup) {
    const outgoingGroups = new Map();
    const incomingGroups = new Map();

    edges.forEach((edge) => {
        const sourceNode = nodeLookup.get(edge.source);
        const targetNode = nodeLookup.get(edge.target);

        if (!sourceNode || !targetNode) {
            return;
        }

        if (sourceNode.layer === layer && targetNode.layer > layer) {
            const groupKey = targetNode.layer;

            if (!outgoingGroups.has(groupKey)) {
                outgoingGroups.set(groupKey, []);
            }

            outgoingGroups.get(groupKey).push(edge);
        }

        if (targetNode.layer === layer && sourceNode.layer < layer) {
            const groupKey = sourceNode.layer;

            if (!incomingGroups.has(groupKey)) {
                incomingGroups.set(groupKey, []);
            }

            incomingGroups.get(groupKey).push(edge);
        }
    });

    let crossingScore = 0;
    outgoingGroups.forEach((groupEdges) => {
        crossingScore += countEdgeCrossings(groupEdges, nodeLookup);
    });
    incomingGroups.forEach((groupEdges) => {
        crossingScore += countEdgeCrossings(groupEdges, nodeLookup);
    });

    return crossingScore;
}

function optimizeLayerBySwaps(layerNodes, edges, nodeLookup) {
    if (layerNodes.length < 2) {
        return;
    }

    const layer = layerNodes[0].layer;
    let updated = true;

    while (updated) {
        updated = false;

        for (let index = 0; index < layerNodes.length - 1; index += 1) {
            const currentScore = countLayerCrossings(layer, edges, nodeLookup);
            const firstNode = layerNodes[index];
            const secondNode = layerNodes[index + 1];

            layerNodes[index] = secondNode;
            layerNodes[index + 1] = firstNode;
            reindexLayerNodes(layerNodes);

            const swappedScore = countLayerCrossings(layer, edges, nodeLookup);

            if (swappedScore < currentScore) {
                updated = true;
                continue;
            }

            layerNodes[index] = firstNode;
            layerNodes[index + 1] = secondNode;
            reindexLayerNodes(layerNodes);
        }
    }
}

function buildChartLimitButtons(activeLimit, supportedLimits) {
    return supportedLimits.map((limit) => `
        <button
            type="button"
            class="btn btn-secondary btn-sm detail-chart-limit${String(activeLimit) === String(limit.value) ? " is-active" : ""}"
            data-chart-limit="${escapeHtml(limit.value)}"
        >
            ${escapeHtml(limit.label)}
        </button>
    `).join("");
}

function buildDonutChartMarkup(segments, total, centerTitle, centerValue) {
    const normalizedSegments = segments
        .map((segment) => ({
            ...segment,
            ratio: Math.max(0, Number(segment.ratio || 0)),
        }))
        .filter((segment) => segment.ratio > 0);
    const totalRatio = normalizedSegments.reduce((sum, segment) => sum + segment.ratio, 0);
    let currentAngle = 0;
    const gradientStops = [];

    normalizedSegments.forEach((segment) => {
        const normalizedRatio = totalRatio > 0 ? segment.ratio / totalRatio : 0;
        const startAngle = currentAngle;
        const endAngle = currentAngle + normalizedRatio * 360;
        gradientStops.push(`${segment.color} ${startAngle.toFixed(2)}deg ${endAngle.toFixed(2)}deg`);
        currentAngle = endAngle;
    });

    const gradient = gradientStops.length
        ? `conic-gradient(${gradientStops.join(", ")})`
        : "conic-gradient(#e2e8f0 0deg 360deg)";

    return `
        <div class="detail-pie-card">
            <div class="detail-pie-chart" role="img" aria-label="${escapeHtml(centerTitle)}" style="background:${escapeHtml(gradient)}">
                <div class="detail-pie-hole">
                    <span class="detail-pie-center-label">${escapeHtml(centerTitle)}</span>
                    <strong class="detail-pie-center-value">${escapeHtml(centerValue)}</strong>
                    <small class="detail-pie-center-meta">全 ${escapeHtml(Number(total || 0).toLocaleString("ja-JP"))} 件</small>
                </div>
            </div>
            <div class="detail-pie-legend">
                ${normalizedSegments.map((segment) => {
                    const normalizedRatio = totalRatio > 0 ? segment.ratio / totalRatio : 0;
                    return `
                        <div class="detail-pie-legend-item" title="${escapeHtml(segment.tooltip || "")}">
                            <span class="detail-pie-dot" style="background:${escapeHtml(segment.color)}"></span>
                            <span class="detail-pie-legend-label">${escapeHtml(segment.label)}</span>
                            <span class="detail-pie-legend-value">${escapeHtml((normalizedRatio * 100).toFixed(1))}%</span>
                        </div>
                    `;
                }).join("")}
            </div>
        </div>
    `;
}

function renderFrequencyChart(analysis) {
    const allRows = Array.isArray(analysis.rows) ? analysis.rows : [];
    const visibleLimit = frequencyChartLimit === "all" ? allRows.length : Math.min(allRows.length, Number(frequencyChartLimit) || 10);
    const chartRows = allRows.slice(0, visibleLimit);

    if (!chartRows.length) {
        chartPanel.className = "result-panel";
        chartTitle.textContent = "頻度分析グラフ";
        chartNote.textContent = "表示できるデータがありません。";
        chartContainer.innerHTML = '<p class="empty-state">表示できるデータがありません。</p>';
        return;
    }

    const maxEventCount = Math.max(...chartRows.map((row) => Number(row["イベント件数"]) || 0), 1);
    const chartWidth = 1020;
    const labelWidth = 240;
    const barAreaWidth = 560;
    const chartLeft = 14;
    const barStartX = chartLeft + labelWidth + 16;
    const valueX = barStartX + barAreaWidth + 24;
    const rowHeight = 48;
    const chartHeight = 80 + chartRows.length * rowHeight;
    const scaleValues = [0, 0.25, 0.5, 0.75, 1];

    const gridLines = scaleValues.map((rate) => {
        const x = barStartX + barAreaWidth * rate;
        return `
            <line x1="${x}" y1="36" x2="${x}" y2="${chartHeight - 16}" class="frequency-svg-grid"></line>
            <text x="${x}" y="20" text-anchor="${rate === 0 ? "start" : rate === 1 ? "end" : "middle"}" class="frequency-svg-scale">
                ${escapeHtml(Math.round(maxEventCount * rate).toLocaleString("ja-JP"))}
            </text>
        `;
    }).join("");

    const topFiveRows = [...allRows]
        .sort((a, b) => (Number(b["イベント件数"]) || 0) - (Number(a["イベント件数"]) || 0))
        .slice(0, 5);
    const topFiveTotal = topFiveRows.reduce((sum, row) => sum + (Number(row["イベント件数"]) || 0), 0);
    const otherCount = Math.max(0, (allRows.reduce((sum, row) => sum + (Number(row["イベント件数"]) || 0), 0) - topFiveTotal));
    const donutPalette = ["#1e40af", "#2563eb", "#3b82f6", "#60a5fa", "#93c5fd"];
    const donutSegments = [
        ...topFiveRows.map((row, index) => ({
            label: String(row["アクティビティ"] || `Top${index + 1}`),
            ratio: (Number(row["イベント件数"]) || 0) / Math.max(1, topFiveTotal + otherCount),
            color: donutPalette[index] || "#bfdbfe",
            tooltip: `${row["アクティビティ"]}: ${Number(row["イベント件数"] || 0).toLocaleString("ja-JP")} 件`,
        })),
        ...(otherCount > 0 ? [{
            label: "その他",
            ratio: otherCount / Math.max(1, topFiveTotal + otherCount),
            color: "#e2e8f0",
            tooltip: `その他: ${otherCount.toLocaleString("ja-JP")} 件`,
        }] : []),
    ];

    const rowsSvg = chartRows.map((row, index) => {
        const activityName = String(row["アクティビティ"] || "").trim();
        const eventCount = Number(row["イベント件数"]) || 0;
        const averageDuration = row["平均処理時間(分)"] ?? row["平均時間(分)"] ?? "-";
        const medianDuration = row["中央値処理時間(分)"] ?? row["中央値時間(分)"] ?? "-";
        const ratio = row["イベント占有率(%)"] ?? row["event_ratio_pct"] ?? "-";
        const barWidth = Math.max(12, (eventCount / maxEventCount) * barAreaWidth);
        const rowCenterY = 54 + index * rowHeight;
        const labelLines = wrapJapaneseLabel(activityName, 14);
        const fillColor = index === 0
            ? "#2563eb"
            : index < 3
                ? "#3b82f6"
                : `rgba(59, 130, 246, ${Math.max(0.28, 0.84 - index * 0.06)})`;

        return `
            <g class="detail-chart-bar-group">
                <title>${escapeHtml(`${activityName} / 件数 ${eventCount.toLocaleString("ja-JP")} / 平均 ${averageDuration}分 / 中央値 ${medianDuration}分 / 占有率 ${ratio}%`)}</title>
                ${labelLines.map((line, lineIndex) => `
                    <text x="${chartLeft}" y="${rowCenterY + (lineIndex === 0 ? -4 : 12)}" class="frequency-svg-label">${escapeHtml(line)}</text>
                `).join("")}
                <rect x="${barStartX}" y="${rowCenterY - 10}" width="${barAreaWidth}" height="18" rx="9" ry="9" class="frequency-svg-track"></rect>
                <rect x="${barStartX}" y="${rowCenterY - 10}" width="${barWidth}" height="18" rx="9" ry="9" fill="${fillColor}"></rect>
                <text x="${valueX}" y="${rowCenterY + 4}" class="frequency-svg-count">${escapeHtml(eventCount.toLocaleString("ja-JP"))}件</text>
            </g>
        `;
    }).join("");

    chartPanel.className = "result-panel";
    chartTitle.textContent = "頻度分析";
    chartNote.textContent = "件数を軸に比較し、詳細な時間情報はホバーで確認できます。";
    chartContainer.innerHTML = `
        <div class="detail-chart-toolbar">
            <div class="detail-chart-toolbar-copy">
                <strong>アクティビティ頻度</strong>
                <span>Top10 を初期表示し、イベント占有率は右のドーナツで確認できます。</span>
            </div>
            <div class="detail-chart-toolbar-actions">
                ${buildChartLimitButtons(frequencyChartLimit, [
                    { value: 10, label: "Top10" },
                    { value: 20, label: "Top20" },
                    { value: "all", label: "全件" },
                ])}
            </div>
        </div>
        <div class="detail-chart-split">
            <div class="detail-chart-main">
                <svg class="frequency-chart-svg" viewBox="0 0 ${chartWidth} ${chartHeight}" role="img" aria-label="頻度分析グラフ" preserveAspectRatio="xMinYMin meet">
                    ${gridLines}
                    ${rowsSvg}
                </svg>
            </div>
            ${buildDonutChartMarkup(
                donutSegments,
                topFiveTotal + otherCount,
                "上位5件占有率",
                `${Math.round((topFiveTotal / Math.max(1, topFiveTotal + otherCount)) * 100)}%`,
            )}
        </div>
    `;

    chartContainer.querySelectorAll("[data-chart-limit]").forEach((buttonElement) => {
        buttonElement.addEventListener("click", () => {
            frequencyChartLimit = buttonElement.dataset.chartLimit === "all"
                ? "all"
                : Number(buttonElement.dataset.chartLimit || 10);
            renderFrequencyChart(analysis);
        });
    });
}

let transitionDirectionFilter = "all";

function renderTransitionChart(analysis) {
    const allRows = Array.isArray(analysis.rows) ? analysis.rows : [];

    const reverseLookupFull = new Set(
        allRows.map((row) => `${row["前処理アクティビティ名"]}|||${row["後処理アクティビティ名"]}`)
    );
    const filteredRows = transitionDirectionFilter === "forward"
        ? allRows.filter((row) => !reverseLookupFull.has(`${row["後処理アクティビティ名"]}|||${row["前処理アクティビティ名"]}`))
        : transitionDirectionFilter === "reverse"
            ? allRows.filter((row) => reverseLookupFull.has(`${row["後処理アクティビティ名"]}|||${row["前処理アクティビティ名"]}`))
            : allRows;

    const visibleLimit = transitionChartLimit === "all" ? filteredRows.length : Math.min(filteredRows.length, Number(transitionChartLimit) || 15);
    const chartRows = filteredRows.slice(0, visibleLimit);

    if (!chartRows.length) {
        chartPanel.className = "result-panel";
        chartTitle.textContent = "前後処理分析グラフ";
        chartNote.textContent = "表示できるデータがありません。";
        chartContainer.innerHTML = '<p class="empty-state">表示できるデータがありません。</p>';
        return;
    }

    const reverseLookup = reverseLookupFull;
    const maxTransitionCount = Math.max(...chartRows.map((row) => Number(row["遷移件数"]) || Number(row["ケース数"]) || 0), 1);
    const chartWidth = 1080;
    const labelWidth = 300;
    const barAreaWidth = 540;
    const chartLeft = 14;
    const barStartX = chartLeft + labelWidth + 16;
    const valueX = barStartX + barAreaWidth + 24;
    const rowHeight = 54;
    const chartHeight = 84 + chartRows.length * rowHeight;
    const scaleValues = [0, 0.25, 0.5, 0.75, 1];

    const gridLines = scaleValues.map((rate) => {
        const x = barStartX + barAreaWidth * rate;
        return `
            <line x1="${x}" y1="36" x2="${x}" y2="${chartHeight - 16}" class="transition-svg-grid"></line>
            <text x="${x}" y="20" text-anchor="${rate === 0 ? "start" : rate === 1 ? "end" : "middle"}" class="transition-svg-scale">
                ${escapeHtml(Math.round(maxTransitionCount * rate).toLocaleString("ja-JP"))}
            </text>
        `;
    }).join("");

    const topFiveRows = [...allRows]
        .sort((a, b) => ((Number(b["遷移件数"]) || Number(b["ケース数"]) || 0) - (Number(a["遷移件数"]) || Number(a["ケース数"]) || 0)))
        .slice(0, 5);
    const topFiveTotal = topFiveRows.reduce((sum, row) => sum + (Number(row["遷移件数"]) || Number(row["ケース数"]) || 0), 0);
    const allTotal = allRows.reduce((sum, row) => sum + (Number(row["遷移件数"]) || Number(row["ケース数"]) || 0), 0);
    const otherCount = Math.max(0, allTotal - topFiveTotal);
    const transitionDonutPalette = ["#2563eb", "#3b82f6", "#10b981", "#f59e0b", "#94a3b8"];
    const donutSegments = [
        ...topFiveRows.map((row, index) => ({
            label: `${row["前処理アクティビティ名"]} → ${row["後処理アクティビティ名"]}`,
            ratio: (Number(row["遷移件数"]) || Number(row["ケース数"]) || 0) / Math.max(1, allTotal),
            color: transitionDonutPalette[index] || "#e2e8f0",
            tooltip: `${row["前処理アクティビティ名"]} → ${row["後処理アクティビティ名"]}: ${Number(row["遷移件数"] || row["ケース数"] || 0).toLocaleString("ja-JP")} 件`,
        })),
        ...(otherCount > 0 ? [{
            label: "その他",
            ratio: otherCount / Math.max(1, allTotal),
            color: "#e2e8f0",
            tooltip: `その他: ${otherCount.toLocaleString("ja-JP")} 件`,
        }] : []),
    ];

    const rowsSvg = chartRows.map((row) => {
        const fromActivity = String(row["前処理アクティビティ名"] || "").trim();
        const toActivity = String(row["後処理アクティビティ名"] || "").trim();
        const transitionLabel = `${fromActivity} → ${toActivity}`;
        const transitionCount = Number(row["遷移件数"]) || Number(row["ケース数"]) || 0;
        const avgDuration = row["平均所要時間(分)"] ?? row["平均時間(分)"] ?? row["平均待ち時間(分)"] ?? "-";
        const medianDuration = row["中央値所要時間(分)"] ?? row["中央値時間(分)"] ?? "-";
        const maxDuration = row["最大所要時間(分)"] ?? row["最大時間(分)"] ?? "-";
        const transitionRatio = row["遷移比率(%)"] ?? row["割合(%)"] ?? "-";
        const isReverse = reverseLookup.has(`${toActivity}|||${fromActivity}`);
        const barWidth = Math.max(12, (transitionCount / maxTransitionCount) * barAreaWidth);
        const rowCenterY = 56 + chartRows.indexOf(row) * rowHeight;
        const fillColor = isReverse ? "#ef4444" : chartRows.indexOf(row) < 3 ? "#3b82f6" : "#93c5fd";

        return `
            <g class="detail-chart-bar-group">
                <title>${escapeHtml(`${transitionLabel} / 件数 ${transitionCount.toLocaleString("ja-JP")} / 平均 ${avgDuration}分 / 中央値 ${medianDuration}分 / 最大 ${maxDuration}分 / 比率 ${transitionRatio}%`)}</title>
                <text x="${chartLeft}" y="${rowCenterY + 4}" class="transition-svg-label">${escapeHtml(transitionLabel)}</text>
                <rect x="${barStartX}" y="${rowCenterY - 10}" width="${barAreaWidth}" height="18" rx="9" ry="9" class="transition-svg-track"></rect>
                <rect x="${barStartX}" y="${rowCenterY - 10}" width="${barWidth}" height="18" rx="9" ry="9" fill="${fillColor}"></rect>
                <text x="${valueX}" y="${rowCenterY + 4}" class="transition-svg-count">${escapeHtml(transitionCount.toLocaleString("ja-JP"))}件</text>
            </g>
        `;
    }).join("");

    chartPanel.className = "result-panel";
    chartTitle.textContent = "前後処理分析";
    chartNote.textContent = "遷移件数を中心に比較し、逆方向遷移は赤で強調しています。";
    chartContainer.innerHTML = `
        <div class="detail-chart-toolbar">
            <div class="detail-chart-toolbar-copy">
                <strong>遷移頻度</strong>
                <span>詳細な処理時間はホバーで確認できます。</span>
            </div>
            <div class="detail-chart-toolbar-actions">
                <div class="chart-limit-buttons">
                    ${["all", "forward", "reverse"].map((dir) => `
                        <button type="button" class="btn btn-sm${transitionDirectionFilter === dir ? " active" : ""}" data-transition-dir="${dir}">
                            ${{ all: "全遷移", forward: "順方向", reverse: "逆方向" }[dir]}
                        </button>
                    `).join("")}
                </div>
                ${buildChartLimitButtons(transitionChartLimit, [
                    { value: 10, label: "Top10" },
                    { value: 15, label: "Top15" },
                    { value: 20, label: "Top20" },
                    { value: "all", label: "全件" },
                ])}
            </div>
        </div>
        <div class="detail-chart-split">
            <div class="detail-chart-main">
                <svg class="transition-chart-svg" viewBox="0 0 ${chartWidth} ${chartHeight}" role="img" aria-label="前後処理分析グラフ" preserveAspectRatio="xMinYMin meet">
                    ${gridLines}
                    ${rowsSvg}
                </svg>
            </div>
            ${buildDonutChartMarkup(
                donutSegments,
                allTotal,
                "上位5遷移占有率",
                `${Math.round((topFiveTotal / Math.max(1, allTotal)) * 100)}%`,
            )}
        </div>
    `;

    chartContainer.querySelectorAll("[data-chart-limit]").forEach((buttonElement) => {
        buttonElement.addEventListener("click", () => {
            transitionChartLimit = buttonElement.dataset.chartLimit === "all"
                ? "all"
                : Number(buttonElement.dataset.chartLimit || 15);
            renderTransitionChart(analysis);
        });
    });

    chartContainer.querySelectorAll("[data-transition-dir]").forEach((buttonElement) => {
        buttonElement.addEventListener("click", () => {
            transitionDirectionFilter = buttonElement.dataset.transitionDir || "all";
            renderTransitionChart(analysis);
        });
    });
}

function buildProcessFlowData(patternRows, transitionRows = [], frequencyRows = []) {
    const nodeMap = new Map();
    const edgeMap = new Map();

    function ensureNode(name) {
        if (!nodeMap.has(name)) {
            nodeMap.set(name, {
                name,
                weight: 0,
                caseWeight: 0,
                positionTotal: 0,
                positionWeight: 0,
                incoming: 0,
                outgoing: 0,
                layerScore: 0,
                layer: 0,
                orderScore: 0,
            });
        }

        return nodeMap.get(name);
    }

    frequencyRows.forEach((row) => {
        const activityName = String(row["アクティビティ"] || "").trim();

        if (!activityName) {
            return;
        }

        const node = ensureNode(activityName);
        node.weight = Math.max(node.weight, Number(row["イベント件数"]) || 0);
        node.caseWeight = Math.max(node.caseWeight, Number(row["ケース数"]) || 0);

        // Extract duration metrics for tooltips
        if (row["平均処理時間(分)"] !== undefined || row["平均時間(分)"] !== undefined) {
            node.avgDuration = Number(row["平均処理時間(分)"] ?? row["平均時間(分)"]) || 0;
        }
        if (row["最大処理時間(分)"] !== undefined || row["最大時間(分)"] !== undefined) {
            node.maxDuration = Number(row["最大処理時間(分)"] ?? row["最大時間(分)"]) || 0;
        }
    });

    patternRows.forEach((row) => {
        const caseCount = Number(row["ケース数"]) || 0;
        const steps = String(row["処理順パターン"])
            .split("→")
            .map((step) => step.trim())
            .filter(Boolean);

        steps.forEach((step, stepIndex) => {
            const node = ensureNode(step);
            node.positionTotal += stepIndex * caseCount;
            node.positionWeight += caseCount;

            if (node.weight === 0) {
                node.weight += caseCount;
            }

            if (node.caseWeight === 0) {
                node.caseWeight += caseCount;
            }

            if (stepIndex === steps.length - 1) {
                return;
            }

            const nextStep = steps[stepIndex + 1];
            ensureNode(nextStep);

            if (transitionRows.length) {
                return;
            }

            const edgeKey = `${step}|||${nextStep}`;

            if (!edgeMap.has(edgeKey)) {
                edgeMap.set(edgeKey, {
                    source: step,
                    target: nextStep,
                    count: 0,
                });
            }

            edgeMap.get(edgeKey).count += caseCount;
        });
    });

    transitionRows.forEach((row) => {
        const sourceName = String(row["前処理アクティビティ名"] || "").trim();
        const targetName = String(row["後処理アクティビティ名"] || "").trim();
        const transitionCount = Number(row["遷移件数"]) || 0;

        if (!sourceName || !targetName || transitionCount <= 0) {
            return;
        }

        ensureNode(sourceName);
        ensureNode(targetName);

        const edgeKey = `${sourceName}|||${targetName}`;

        if (!edgeMap.has(edgeKey)) {
            edgeMap.set(edgeKey, {
                source: sourceName,
                target: targetName,
                count: 0,
            });
        }

        edgeMap.get(edgeKey).count += transitionCount;
    });

    const nodes = Array.from(nodeMap.values());
    const edges = Array.from(edgeMap.values())
        .filter((edge) => edge.source !== edge.target)
        .sort((left, right) => right.count - left.count);
    const nodeLookup = new Map(nodes.map((node) => [node.name, node]));
    const nodesByLayer = new Map();

    edges.forEach((edge) => {
        const sourceNode = nodeLookup.get(edge.source);
        const targetNode = nodeLookup.get(edge.target);

        if (sourceNode) {
            sourceNode.outgoing += edge.count;
        }

        if (targetNode) {
            targetNode.incoming += edge.count;
        }
    });

    nodes.forEach((node) => {
        node.layerScore = node.positionWeight
            ? node.positionTotal / node.positionWeight
            : 0;
        node.layer = Math.max(0, Math.round(node.layerScore));

        if (node.weight === 0) {
            node.weight = Math.max(node.incoming, node.outgoing, node.caseWeight, 1);
        }
    });

    const rawLayers = Array.from(new Set(nodes.map((node) => node.layer))).sort((left, right) => left - right);
    const compactLayerMap = new Map(rawLayers.map((layer, index) => [layer, index]));

    nodes.forEach((node) => {
        node.layer = compactLayerMap.get(node.layer) || 0;

        if (!nodesByLayer.has(node.layer)) {
            nodesByLayer.set(node.layer, []);
        }

        nodesByLayer.get(node.layer).push(node);
    });

    const maxLayer = Math.max(...nodes.map((node) => node.layer), 0);

    for (let layer = 0; layer <= maxLayer; layer += 1) {
        const layerNodes = nodesByLayer.get(layer) || [];
        layerNodes.sort((left, right) => {
            if (right.weight !== left.weight) {
                return right.weight - left.weight;
            }

            return left.name.localeCompare(right.name, "ja");
        });

        reindexLayerNodes(layerNodes);
    }

    for (let iteration = 0; iteration < 6; iteration += 1) {
        for (let layer = 1; layer <= maxLayer; layer += 1) {
            const layerNodes = nodesByLayer.get(layer) || [];
            layerNodes.sort((left, right) => {
                const leftEdges = edges.filter((edge) => edge.target === left.name);
                const rightEdges = edges.filter((edge) => edge.target === right.name);
                const leftWeight = leftEdges.reduce((total, edge) => {
                    const sourceNode = nodeLookup.get(edge.source);
                    const distance = sourceNode ? Math.max(1, left.layer - sourceNode.layer) : 1;
                    return total + edge.count / distance;
                }, 0);
                const rightWeight = rightEdges.reduce((total, edge) => {
                    const sourceNode = nodeLookup.get(edge.source);
                    const distance = sourceNode ? Math.max(1, right.layer - sourceNode.layer) : 1;
                    return total + edge.count / distance;
                }, 0);
                const leftScore = leftEdges.reduce((total, edge) => {
                    const sourceNode = nodeLookup.get(edge.source);
                    const distance = sourceNode ? Math.max(1, left.layer - sourceNode.layer) : 1;
                    return sourceNode ? total + sourceNode.orderScore * (edge.count / distance) : total;
                }, 0);
                const rightScore = rightEdges.reduce((total, edge) => {
                    const sourceNode = nodeLookup.get(edge.source);
                    const distance = sourceNode ? Math.max(1, right.layer - sourceNode.layer) : 1;
                    return sourceNode ? total + sourceNode.orderScore * (edge.count / distance) : total;
                }, 0);
                const leftAverage = leftWeight ? leftScore / leftWeight : left.orderScore;
                const rightAverage = rightWeight ? rightScore / rightWeight : right.orderScore;

                if (leftAverage !== rightAverage) {
                    return leftAverage - rightAverage;
                }

                return right.weight - left.weight;
            });

            reindexLayerNodes(layerNodes);
        }

        for (let layer = maxLayer - 1; layer >= 0; layer -= 1) {
            const layerNodes = nodesByLayer.get(layer) || [];
            layerNodes.sort((left, right) => {
                const leftEdges = edges.filter((edge) => edge.source === left.name);
                const rightEdges = edges.filter((edge) => edge.source === right.name);
                const leftWeight = leftEdges.reduce((total, edge) => {
                    const targetNode = nodeLookup.get(edge.target);
                    const distance = targetNode ? Math.max(1, targetNode.layer - left.layer) : 1;
                    return total + edge.count / distance;
                }, 0);
                const rightWeight = rightEdges.reduce((total, edge) => {
                    const targetNode = nodeLookup.get(edge.target);
                    const distance = targetNode ? Math.max(1, targetNode.layer - right.layer) : 1;
                    return total + edge.count / distance;
                }, 0);
                const leftScore = leftEdges.reduce((total, edge) => {
                    const targetNode = nodeLookup.get(edge.target);
                    const distance = targetNode ? Math.max(1, targetNode.layer - left.layer) : 1;
                    return targetNode ? total + targetNode.orderScore * (edge.count / distance) : total;
                }, 0);
                const rightScore = rightEdges.reduce((total, edge) => {
                    const targetNode = nodeLookup.get(edge.target);
                    const distance = targetNode ? Math.max(1, targetNode.layer - right.layer) : 1;
                    return targetNode ? total + targetNode.orderScore * (edge.count / distance) : total;
                }, 0);
                const leftAverage = leftWeight ? leftScore / leftWeight : left.orderScore;
                const rightAverage = rightWeight ? rightScore / rightWeight : right.orderScore;

                if (leftAverage !== rightAverage) {
                    return leftAverage - rightAverage;
                }

                return right.weight - left.weight;
            });

            reindexLayerNodes(layerNodes);
        }
    }

    for (let layer = 1; layer < maxLayer; layer += 1) {
        const layerNodes = nodesByLayer.get(layer) || [];
        optimizeLayerBySwaps(layerNodes, edges, nodeLookup);
    }
    
    // CELONIS STYLE: Extract main spine
    // Find highest throughput path from top to bottom
    const mainSpineNodes = new Set();
    const mainSpineEdges = new Set();
    
    if (nodes.length > 0) {
        // Start from node with layer 0 and highest weight
        let currentNodes = nodes.filter(n => n.layer === 0).sort((a, b) => b.weight - a.weight);
        if(currentNodes.length > 0) {
            let currentNode = currentNodes[0];
            mainSpineNodes.add(currentNode.name);
            
            // Greedily follow the heaviest outgoing edge
            while (currentNode) {
                const outgoing = edges.filter(e => e.source === currentNode.name && nodeLookup.get(e.target).layer > currentNode.layer);
                if (outgoing.length === 0) break;
                
                // Sort by weight/count
                outgoing.sort((a, b) => b.count - a.count);
                const heaviestEdge = outgoing[0];
                const nextNodeName = heaviestEdge.target;
                
                // Prevent infinite loops just in case
                if(mainSpineNodes.has(nextNodeName)) break;
                
                mainSpineEdges.add(getProcessFlowEdgeKey(heaviestEdge));
                mainSpineNodes.add(nextNodeName);
                
                currentNode = nodeLookup.get(nextNodeName);
            }
        }
    }
    
    // Tag nodes and edges
    nodes.forEach(node => {
        node.isMainSpine = mainSpineNodes.has(node.name);
    });
    
    edges.forEach(edge => {
        edge.isMainSpine = mainSpineEdges.has(getProcessFlowEdgeKey(edge));
    });

    return { nodes, edges, mainSpineNodes, mainSpineEdges };
}

function filterProcessFlowData(sourceNodes, sourceEdges, activityPercent = 100, connectionPercent = 100) {
    if (!sourceNodes.length) {
        return {
            nodes: [],
            edges: [],
            totalNodeCount: 0,
            totalEdgeCount: sourceEdges.length,
        };
    }

    const activityLimit = Math.min(
        sourceNodes.length,
        Math.max(2, Math.ceil(sourceNodes.length * (activityPercent / 100)))
    );
    const selectedNodes = [...sourceNodes]
        .sort((left, right) => {
            if (right.weight !== left.weight) {
                return right.weight - left.weight;
            }

            return left.name.localeCompare(right.name, "ja");
        })
        .slice(0, activityLimit);
    const selectedNodeNames = new Set(selectedNodes.map((node) => node.name));
    const candidateEdges = sourceEdges.filter((edge) => {
        return selectedNodeNames.has(edge.source) && selectedNodeNames.has(edge.target);
    });

    const connectionLimit = candidateEdges.length
        ? Math.min(
            candidateEdges.length,
            Math.max(1, Math.ceil(candidateEdges.length * (connectionPercent / 100)))
        )
        : 0;
    const selectedEdges = candidateEdges.slice(0, connectionLimit);

    // Show all selected nodes regardless of whether they have visible edges.
    // This prevents the flow from going empty when selected nodes have no mutual connections.
    const visibleNodes = selectedNodes.map((node) => ({ ...node }));
    const visibleEdges = selectedEdges.map((edge) => ({ ...edge }));

    return {
        nodes: visibleNodes,
        edges: visibleEdges,
        totalNodeCount: sourceNodes.length,
        totalEdgeCount: sourceEdges.length,
    };
}

function getProcessFlowEdgeKey(edge) {
    return `${edge.source}|||${edge.target}`;
}

function buildProcessMapLabelState(edges, labelPercent = 100) {
    const sortedEdges = [...edges]
        .sort((left, right) => {
            if (right.count !== left.count) {
                return right.count - left.count;
            }

            return getProcessFlowEdgeKey(left).localeCompare(getProcessFlowEdgeKey(right), "ja");
        });

    const clampedLabelPercent = Math.max(0, Math.min(100, labelPercent));
    const labelLimit = clampedLabelPercent <= 0
        ? 0
        : Math.min(
            sortedEdges.length,
            Math.max(1, Math.ceil(sortedEdges.length * (clampedLabelPercent / 100)))
        );
    const visibleLabelKeys = new Set(
        sortedEdges
            .slice(0, labelLimit)
            .map((edge) => getProcessFlowEdgeKey(edge))
    );

    return {
        visibleLabelKeys,
        visibleLabelCount: visibleLabelKeys.size,
        totalLabelCount: sortedEdges.length,
    };
}

function normalizeProcessMapLabelMode(labelMode) {
    return labelMode === "duration" ? "duration" : "count";
}

function getProcessMapEdgeLabelText(edge, labelMode = "count") {
    const normalizedLabelMode = normalizeProcessMapLabelMode(labelMode);
    const countLabel = `${Number(edge?.count || 0).toLocaleString("ja-JP")}件`;
    const durationText = String(edge?.avg_duration_text || "").trim();

    if (normalizedLabelMode === "duration" && durationText) {
        return `平均 ${durationText}`;
    }

    return countLabel;
}

function applyProcessMapLabelMode(viewportElement, labelMode = "count") {
    const svgElement = viewportElement?.querySelector("svg.process-map-svg");
    if (!svgElement) {
        return;
    }

    const normalizedLabelMode = normalizeProcessMapLabelMode(labelMode);
    svgElement.querySelectorAll(".process-map-edge-label").forEach((labelElement) => {
        const fallbackText = String(labelElement.dataset.labelCount || labelElement.textContent || "").trim();
        const nextText = normalizedLabelMode === "duration"
            ? String(labelElement.dataset.labelDuration || fallbackText).trim()
            : fallbackText;
        labelElement.textContent = nextText || fallbackText;
        labelElement.dataset.labelMode = normalizedLabelMode;
    });
}

function downloadBlob(blob, fileName) {
    const objectUrl = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = objectUrl;
    link.download = fileName;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(objectUrl);
}

async function downloadDetailExcelExport(runId, options = {}) {
    const exportUrl = buildDetailExcelExportUrl(runId, options);
    const fallbackFileName = `process_mining_${analysisKey || "detail"}.xlsx`;

    if (detailExportExcelButton) {
        detailExportExcelButton.disabled = true;
    }
    setStatus("Excel を生成しています...", "info");

    try {
        const response = await fetch(exportUrl);

        if (!response.ok) {
            let errorMessage = "Excel 出力に失敗しました。";
            const contentType = response.headers.get("Content-Type") || "";
            if (contentType.includes("application/json")) {
                const payload = await response.json();
                errorMessage = payload.detail || payload.error || errorMessage;
            } else {
                const responseText = await response.text();
                errorMessage = responseText || errorMessage;
            }
            throw new Error(errorMessage);
        }

        const exportBlob = await response.blob();
        downloadBlob(exportBlob, getDownloadFileName(response, fallbackFileName));
        setStatus("Excel をダウンロードしました。", "success");
    } catch (error) {
        setStatus(error.message, "error");
    } finally {
        if (detailExportExcelButton) {
            detailExportExcelButton.disabled = false;
        }
    }
}

function buildProcessMapExportSvg() {
    const svgElement = document.querySelector("#process-map-viewport svg");

    if (!svgElement) {
        return null;
    }

    const clonedSvg = svgElement.cloneNode(true);
    
    // Reset transform for export to ensure it saves at 100% original size
    const exportWrap = clonedSvg.querySelector("g.viewport-wrap");
    if (exportWrap) {
        exportWrap.removeAttribute("transform");
    }
    
    const viewBox = clonedSvg.getAttribute("viewBox") || "0 0 1200 600";
    const [, , widthValue, heightValue] = viewBox.split(" ").map(Number);
    const exportStyles = `
        .process-map-edge {
            fill: none;
            stroke: #2458d3;
            stroke-linecap: round;
        }
        .process-map-edge--return {
            stroke: #6f83aa;
            stroke-dasharray: 10 8;
        }
        .process-map-edge-label {
            fill: rgba(36, 88, 211, 0.82);
            font-size: 10px;
            font-weight: 700;
            text-anchor: middle;
            paint-order: stroke;
            stroke: #ffffff;
            stroke-width: 4px;
            stroke-linejoin: round;
            font-family: inherit;
        }
        .process-map-edge-label--return {
            fill: rgba(207, 122, 69, 0.88);
        }
        .process-map-node {
            stroke-width: 1.2;
        }
        .process-map-node-label {
            font-size: 14px;
            font-weight: 700;
            font-family: "BIZ UDPGothic", "Yu Gothic UI", sans-serif;
        }
    `;

    clonedSvg.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    clonedSvg.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
    clonedSvg.setAttribute("width", String(widthValue || 1200));
    clonedSvg.setAttribute("height", String(heightValue || 600));

    const backgroundRect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    backgroundRect.setAttribute("x", "0");
    backgroundRect.setAttribute("y", "0");
    backgroundRect.setAttribute("width", "100%");
    backgroundRect.setAttribute("height", "100%");
    backgroundRect.setAttribute("fill", "#f5f7fa"); // Unified with var(--bg)
    clonedSvg.insertBefore(backgroundRect, clonedSvg.firstChild);

    const styleElement = document.createElementNS("http://www.w3.org/2000/svg", "style");
    styleElement.textContent = exportStyles;
    clonedSvg.insertBefore(styleElement, clonedSvg.firstChild);

    return {
        svgText: new XMLSerializer().serializeToString(clonedSvg),
        width: widthValue || 1200,
        height: heightValue || 600,
    };
}

function exportProcessMapSvg(fileName) {
    const exportData = buildProcessMapExportSvg();

    if (!exportData) {
        return;
    }

    downloadBlob(
        new Blob([exportData.svgText], { type: "image/svg+xml;charset=utf-8" }),
        fileName
    );
}

function exportProcessMapPng(fileName) {
    const exportData = buildProcessMapExportSvg();

    if (!exportData) {
        return;
    }

    const image = new Image();
    const svgBlob = new Blob([exportData.svgText], { type: "image/svg+xml;charset=utf-8" });
    const objectUrl = URL.createObjectURL(svgBlob);

    image.onload = () => {
        const scale = 2;
        const canvas = document.createElement("canvas");
        const context = canvas.getContext("2d");

        canvas.width = exportData.width * scale;
        canvas.height = exportData.height * scale;

        if (!context) {
            URL.revokeObjectURL(objectUrl);
            return;
        }

        context.scale(scale, scale);
        context.drawImage(image, 0, 0, exportData.width, exportData.height);
        canvas.toBlob((blob) => {
            if (blob) {
                downloadBlob(blob, fileName);
            }

            URL.revokeObjectURL(objectUrl);
        }, "image/png");
    };

    image.onerror = () => {
        URL.revokeObjectURL(objectUrl);
    };

    image.src = objectUrl;
}

function calculateProcessFlowLayout(nodes, edges, options = {}) {
    if (!nodes.length) return { chartWidth: 0, chartHeight: 0, mainSpineX: 0 };

    const compactMode = Boolean(options.compactMode);
    const chartLeft = 40;
    const chartTop = 60;
    const baseNodeWidth = 140;
    const baseNodeHeight = 44;
    const layerGap = 280;
    const rowGap = compactMode ? 184 : 200;
    const returnRouteBaseOffset = compactMode ? 150 : 210;
    const returnRouteLayerOffset = compactMode ? 24 : 32;
    const nodesByLayer = new Map();

    nodes.forEach((node) => {
        if (!nodesByLayer.has(node.layer)) {
            nodesByLayer.set(node.layer, []);
        }
        nodesByLayer.get(node.layer).push(node);
    });

    const layerKeys = Array.from(nodesByLayer.keys()).sort((a, b) => a - b);
    const maxNodesInLayer = Math.max(...Array.from(nodesByLayer.values()).map(arr => arr.length), 1);
    const svgWidth = Math.max(1460, maxNodesInLayer * layerGap + 400); 
    const mainSpineX = Math.floor(svgWidth / 2) - Math.floor(baseNodeWidth / 2);

    layerKeys.forEach((layerKey) => {
        const layerNodes = nodesByLayer.get(layerKey);
        const spineNodes = layerNodes.filter(n => n.isMainSpine);
        const branchNodes = layerNodes.filter(n => !n.isMainSpine);
        
        let nextLeftOffset = 1;
        let nextRightOffset = 1;
        
        if (spineNodes.length > 0) {
            spineNodes[0].centerX = mainSpineX + (baseNodeWidth / 2);
            spineNodes[0].y = chartTop + layerKey * rowGap;
        }
        
        branchNodes.sort((a, b) => b.weight - a.weight);
        branchNodes.forEach((node, idx) => {
            node.y = chartTop + layerKey * rowGap;
            if (idx % 2 === 0) {
                node.centerX = mainSpineX + (nextRightOffset * layerGap) + (baseNodeWidth / 2);
                nextRightOffset++;
            } else {
                node.centerX = mainSpineX - (nextLeftOffset * layerGap) + (baseNodeWidth / 2);
                node.centerX = Math.max(chartLeft + (baseNodeWidth / 2), node.centerX);
                nextLeftOffset++;
            }
        });
    });

    const maxNodeWeightForSize = Math.max(...nodes.map((node) => node.weight), 1);
    nodes.forEach(node => {
        const scale = Math.sqrt(node.weight / maxNodeWeightForSize);
        node.calcWidth = baseNodeWidth + scale * 80;
        node.calcHeight = baseNodeHeight + scale * 20;
        node.x = node.centerX - (node.calcWidth / 2);
    });

    const nodeLookup = new Map(nodes.map(n => [n.name, n]));
    const maxNodeBottom = Math.max(...nodes.map(n => n.y + n.calcHeight), chartTop + baseNodeHeight);
    
    let maxRouteY = maxNodeBottom;
    edges.forEach(edge => {
        const sourceNode = nodeLookup.get(edge.source);
        const targetNode = nodeLookup.get(edge.target);
        if (sourceNode && targetNode && targetNode.layer <= sourceNode.layer) {
            const startY = sourceNode.y + sourceNode.calcHeight;
            const endY = targetNode.y;
            const routeY = Math.max(startY, endY)
                + returnRouteBaseOffset
                + Math.abs(targetNode.layer - sourceNode.layer) * returnRouteLayerOffset;
            if (routeY > maxRouteY) maxRouteY = routeY;
        }
    });

    const bottomPadding = compactMode ? 36 : 60;
    return {
        chartWidth: svgWidth,
        chartHeight: Math.max(compactMode ? 320 : 400, maxRouteY + bottomPadding),
        mainSpineX: mainSpineX
    };
}

function getProcessMapBezierPoint(p0, p1, p2, p3, t) {
    const mt = 1 - t;
    return {
        x: (mt ** 3 * p0.x)
            + (3 * mt * mt * t * p1.x)
            + (3 * mt * t * t * p2.x)
            + (t ** 3 * p3.x),
        y: (mt ** 3 * p0.y)
            + (3 * mt * mt * t * p1.y)
            + (3 * mt * t * t * p2.y)
            + (t ** 3 * p3.y),
    };
}

function getProcessMapBezierTangent(p0, p1, p2, p3, t) {
    const mt = 1 - t;
    return {
        x: (3 * mt * mt * (p1.x - p0.x))
            + (6 * mt * t * (p2.x - p1.x))
            + (3 * t * t * (p3.x - p2.x)),
        y: (3 * mt * mt * (p1.y - p0.y))
            + (6 * mt * t * (p2.y - p1.y))
            + (3 * t * t * (p3.y - p2.y)),
    };
}

function getProcessMapCurveLabelPosition(p0, p1, p2, p3, options = {}) {
    const anchorT = Number.isFinite(options.t) ? options.t : 0.5;
    const offset = Number.isFinite(options.offset) ? options.offset : 16;
    const preferredDirectionX = Number(options.preferredDirectionX || 0);
    const preferredDirectionY = Number(options.preferredDirectionY || 0);
    const anchorPoint = getProcessMapBezierPoint(p0, p1, p2, p3, anchorT);
    const tangent = getProcessMapBezierTangent(p0, p1, p2, p3, anchorT);
    const tangentLength = Math.hypot(tangent.x, tangent.y) || 1;
    let normalX = -tangent.y / tangentLength;
    let normalY = tangent.x / tangentLength;

    if (preferredDirectionX && (normalX * preferredDirectionX) < 0) {
        normalX *= -1;
        normalY *= -1;
    } else if (!preferredDirectionX && preferredDirectionY && (normalY * preferredDirectionY) < 0) {
        normalX *= -1;
        normalY *= -1;
    }

    return {
        x: anchorPoint.x + (normalX * offset),
        y: anchorPoint.y + (normalY * offset),
    };
}

function clampProcessMapLabelPosition(x, y, chartWidth, chartHeight, paddingX = 24, paddingY = 16) {
    return {
        x: Math.max(paddingX, Math.min(chartWidth - paddingX, x)),
        y: Math.max(paddingY, Math.min(chartHeight - paddingY, y)),
    };
}

function renderProcessFlowMapFromData(flowData, options = {}) {
    const activityPercent = Number(options.activityPercent ?? 100);
    const connectionPercent = Number(options.connectionPercent ?? 100);
    const labelPercent = Number(options.labelPercent ?? 100);
    const labelMode = normalizeProcessMapLabelMode(options.labelMode);
    const compactMode = Boolean(options.compactMode);
    const filteredData = filterProcessFlowData(
        flowData.nodes,
        flowData.edges,
        activityPercent,
        connectionPercent
    );
    const { nodes, edges } = filteredData;

    if (!nodes.length) {
        return '<p class="empty-state">フロー図を作れるデータがありません。</p>';
    }

    // Reuse pre-calculated layout if available, otherwise calculate once
    const layout = calculateProcessFlowLayout(nodes, edges, { compactMode });
    const { chartWidth, chartHeight, mainSpineX } = layout;
    const layerGap = 280;
    const returnRouteBaseOffset = compactMode ? 150 : 210;
    const returnRouteLayerOffset = compactMode ? 24 : 32;

    const nodeLookup = new Map(nodes.map(n => [n.name, n]));
    const maxEdgeCount = Math.max(...edges.map(e => e.count), 1);
    const maxNodeWeight = Math.max(...nodes.map(n => n.weight), 1);
    const labelState = buildProcessMapLabelState(edges, labelPercent);
    const outgoingEdgeMap = new Map();
    const incomingEdgeMap = new Map();

    edges.forEach(edge => {
        if (!outgoingEdgeMap.has(edge.source)) outgoingEdgeMap.set(edge.source, []);
        if (!incomingEdgeMap.has(edge.target)) incomingEdgeMap.set(edge.target, []);
        outgoingEdgeMap.get(edge.source).push(edge);
        incomingEdgeMap.get(edge.target).push(edge);
    });

    nodes.forEach(node => {
        const outEdges = outgoingEdgeMap.get(node.name) || [];
        const inEdges = incomingEdgeMap.get(node.name) || [];

        outEdges.sort((a, b) => {
            const aX = (nodeLookup.get(a.target) || {x: 0}).x;
            const bX = (nodeLookup.get(b.target) || {x: 0}).x;
            return aX !== bX ? aX - bX : b.count - a.count;
        }).forEach((edge, i) => {
            edge.sourceOffsetX = edge.isMainSpine ? node.x + (node.calcWidth / 2) : node.x + 8 + ((i + 1) * (node.calcWidth - 16)) / (outEdges.length + 1);
        });

        inEdges.sort((a, b) => {
            const aX = (nodeLookup.get(a.source) || {x: 0}).x;
            const bX = (nodeLookup.get(b.source) || {x: 0}).x;
            return aX !== bX ? aX - bX : b.count - a.count;
        }).forEach((edge, i) => {
            edge.targetOffsetX = edge.isMainSpine ? node.x + (node.calcWidth / 2) : node.x + 8 + ((i + 1) * (node.calcWidth - 16)) / (inEdges.length + 1);
        });
    });

    const edgesSvg = edges.map(edge => {
        const s = nodeLookup.get(edge.source);
        const t = nodeLookup.get(edge.target);
        if (!s || !t) return "";

        const isSpine = edge.isMainSpine;
        const isBack = t.layer <= s.layer;
        const edgeWeight = edge.count / maxEdgeCount;
        let opacity, strokeWidth;
        if (isSpine) { opacity = 0.9; strokeWidth = 14; }
        else if (isBack) { opacity = 0.28 + edgeWeight * 0.18; strokeWidth = 2.2 + edgeWeight * 1.4; }
        else {
            opacity = 0.16 + edgeWeight * 0.48;
            strokeWidth = 1.2 + edgeWeight * 8.6;
        }

        const startX = edge.sourceOffsetX, startY = s.y + s.calcHeight;
        const endX = edge.targetOffsetX, endY = t.y;
        let pathD = "", lblX = 0, lblY = 0;

        if (!isBack) {
            if (isSpine) {
                pathD = `M ${startX} ${startY} L ${endX} ${endY}`;
                lblX = ((startX + endX) / 2) + 18;
                lblY = (startY + endY) / 2;
            }
            else {
                const off = Math.max(120, (endY - startY) * 0.5);
                pathD = `M ${startX} ${startY} C ${startX} ${startY + off}, ${endX} ${endY - off}, ${endX} ${endY}`;
                const labelPoint = getProcessMapCurveLabelPosition(
                    { x: startX, y: startY },
                    { x: startX, y: startY + off },
                    { x: endX, y: endY - off },
                    { x: endX, y: endY },
                    {
                        t: 0.52,
                        offset: 16,
                        preferredDirectionX: endX >= startX ? 1 : -1,
                    }
                );
                lblX = labelPoint.x;
                lblY = labelPoint.y;
            }
        } else {
            const rY = Math.max(startY, endY)
                + returnRouteBaseOffset
                + Math.abs(t.layer - s.layer) * returnRouteLayerOffset;
            const rXOff = s.x >= mainSpineX ? 220 + layerGap : -(220 + layerGap); 
            pathD = `M ${startX} ${startY} C ${startX} ${rY}, ${endX + rXOff} ${rY}, ${endX} ${endY}`;
            const labelPoint = getProcessMapCurveLabelPosition(
                { x: startX, y: startY },
                { x: startX, y: rY },
                { x: endX + rXOff, y: rY },
                { x: endX, y: endY },
                {
                    t: 0.36,
                    offset: 18,
                    preferredDirectionX: Math.sign(rXOff) || (s.x >= mainSpineX ? 1 : -1),
                }
            );
            lblX = labelPoint.x;
            lblY = labelPoint.y;
        }

        const clampedLabelPosition = clampProcessMapLabelPosition(lblX, lblY, chartWidth, chartHeight);
        lblX = clampedLabelPosition.x;
        lblY = clampedLabelPosition.y;

        const showLabel = labelState.visibleLabelKeys.has(getProcessFlowEdgeKey(edge));
        const transitionKey = buildTransitionKey(edge.source, edge.target);
        const strokeColor = isSpine
            ? "#0a3b8c"
            : isBack
                ? "#6f83aa"
                : "#2d5ec4";
        // SVG glow filters can collapse on perfectly vertical/horizontal paths
        // because their bbox width/height becomes zero in Chromium.
        const edgeFilter = (
            Math.abs(startX - endX) < 0.5
            || Math.abs(startY - endY) < 0.5
        )
            ? "none"
            : "var(--edge-heat-filter, none)";
        return `
            <path d="${pathD}" class="${isBack ? "process-map-edge process-map-edge--return" : "process-map-edge"}" marker-end="url(#${isBack ? "process-map-arrow-return" : "process-map-arrow"})" data-source="${escapeHtml(edge.source)}" data-target="${escapeHtml(edge.target)}" data-transition-key="${escapeHtml(transitionKey)}" style="stroke-width: ${strokeWidth}; opacity: ${opacity}; fill: none; stroke: var(--edge-heat-stroke, ${strokeColor}); filter: ${edgeFilter};"></path>
            ${showLabel ? `<text x="${lblX}" y="${lblY}" class="${isBack ? "process-map-edge-label process-map-edge-label--return" : "process-map-edge-label"}" data-source="${escapeHtml(edge.source)}" data-target="${escapeHtml(edge.target)}" data-transition-key="${escapeHtml(transitionKey)}" data-label-count="${escapeHtml(getProcessMapEdgeLabelText(edge, "count"))}" data-label-duration="${escapeHtml(getProcessMapEdgeLabelText(edge, "duration"))}" data-label-mode="${escapeHtml(labelMode)}" dominant-baseline="middle" alignment-baseline="middle">${escapeHtml(getProcessMapEdgeLabelText(edge, labelMode))}</text>` : ""}
        `;
    }).join("");

    const nodesSvg = nodes.map(node => {
        const lines = wrapJapaneseLabel(node.name, 10, 2);
        const isSpine = node.isMainSpine, isStart = node.incoming === 0, isEnd = node.outgoing === 0;
        const ratio = node.weight / maxNodeWeight;
        let fill, stroke, lblCol, strokeW, rx;

        if (isStart) { fill = "rgba(38, 166, 91, 0.9)"; stroke = "#1e8248"; lblCol = "#ffffff"; strokeW = "2.5"; rx = "32"; }
        else if (isEnd) { fill = "rgba(28, 43, 89, 0.9)"; stroke = "#13204a"; lblCol = "#ffffff"; strokeW = "2.5"; rx = "32"; }
        else if (isSpine) { fill = `rgba(18, 55, 148, ${0.4 + ratio * 0.6})`; stroke = "#0a2e7a"; lblCol = "#ffffff"; strokeW = "2.5"; rx = "14"; }
        else {
            fill = `rgba(48, 96, 212, ${0.1 + ratio * 0.5})`;
            stroke = `rgba(35, 75, 176, ${0.3 + ratio * 0.4})`;
            lblCol = ratio >= 0.55 ? "#ffffff" : "#1f335e";
            strokeW = "1.2"; rx = "14";
        }

        const labelSvg = lines.map((line, i) => {
            const yOff = lines.length > 1 ? (i - (lines.length - 1) / 2) * 16 : 0;
            return `<text x="${node.x + node.calcWidth / 2}" y="${node.y + node.calcHeight / 2 + yOff}" class="process-map-node-label" text-anchor="middle" dominant-baseline="middle" alignment-baseline="middle" style="fill: ${lblCol}; pointer-events: none;">${escapeHtml(line)}</text>`;
        }).join("");

        let tooltip = `【${node.name}】\n実行回数: ${node.weight.toLocaleString('ja-JP')}件`;
        if (node.avgDuration !== undefined) tooltip += `\n平均処理時間: ${node.avgDuration.toFixed(1)}分`;

        return `
            <g class="process-map-node-group" data-node="${escapeHtml(node.name)}" style="cursor: pointer;">
                <title>${escapeHtml(tooltip)}</title>
                <rect x="${node.x}" y="${node.y}" width="${node.calcWidth}" height="${node.calcHeight}" rx="${rx}" ry="${rx}" class="process-map-node" data-activity="${escapeHtml(node.name)}" style="fill: ${fill}; stroke: var(--node-heat-stroke, ${stroke}); stroke-width: ${strokeW}; filter: var(--node-heat-filter, url(#drop-shadow));"></rect>
                ${labelSvg}
            </g>
        `;
    }).join("");

    return `
        <div class="process-map-wrap">
            <svg class="process-map-svg" width="${chartWidth}" height="${chartHeight}" viewBox="0 0 ${chartWidth} ${chartHeight}" role="img" aria-label="業務全体フロー図" preserveAspectRatio="xMinYMin meet">
                <defs>
                    <filter id="drop-shadow" x="-20%" y="-20%" width="140%" height="140%"><feDropShadow dx="0" dy="8" stdDeviation="6" flood-color="rgba(10, 20, 40, 0.15)" /></filter>
                    <filter id="drop-shadow-heat-4" x="-25%" y="-25%" width="150%" height="150%">
                        <feDropShadow in="SourceAlpha" dx="0" dy="8" stdDeviation="6" flood-color="rgba(10, 20, 40, 0.15)" result="shadow"></feDropShadow>
                        <feFlood flood-color="#d78b74" flood-opacity="0.11" result="heatFill"></feFlood>
                        <feComposite in="heatFill" in2="SourceGraphic" operator="in" result="innerTint"></feComposite>
                        <feDropShadow in="SourceAlpha" dx="0" dy="0" stdDeviation="2.4" flood-color="rgba(214, 120, 82, 0.32)" result="glow"></feDropShadow>
                        <feMerge>
                            <feMergeNode in="shadow"></feMergeNode>
                            <feMergeNode in="glow"></feMergeNode>
                            <feMergeNode in="innerTint"></feMergeNode>
                            <feMergeNode in="SourceGraphic"></feMergeNode>
                        </feMerge>
                    </filter>
                    <filter id="drop-shadow-heat-5" x="-30%" y="-30%" width="160%" height="160%">
                        <feDropShadow in="SourceAlpha" dx="0" dy="8" stdDeviation="6" flood-color="rgba(10, 20, 40, 0.15)" result="shadow"></feDropShadow>
                        <feFlood flood-color="#d66f5c" flood-opacity="0.14" result="heatFill"></feFlood>
                        <feComposite in="heatFill" in2="SourceGraphic" operator="in" result="innerTint"></feComposite>
                        <feDropShadow in="SourceAlpha" dx="0" dy="0" stdDeviation="3.1" flood-color="rgba(196, 88, 70, 0.38)" result="glow"></feDropShadow>
                        <feMerge>
                            <feMergeNode in="shadow"></feMergeNode>
                            <feMergeNode in="glow"></feMergeNode>
                            <feMergeNode in="innerTint"></feMergeNode>
                            <feMergeNode in="SourceGraphic"></feMergeNode>
                        </feMerge>
                    </filter>
                    <filter id="edge-heat-glow-4" x="-20%" y="-20%" width="140%" height="140%">
                        <feDropShadow dx="0" dy="0" stdDeviation="1.5" flood-color="rgba(208, 118, 78, 0.22)" />
                    </filter>
                    <filter id="edge-heat-glow-5" x="-25%" y="-25%" width="150%" height="150%">
                        <feDropShadow dx="0" dy="0" stdDeviation="2.0" flood-color="rgba(192, 92, 68, 0.28)" />
                    </filter>
                    <marker id="process-map-arrow" markerUnits="userSpaceOnUse" markerWidth="12" markerHeight="12" refX="11" refY="6" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#2458d3"></path></marker>
                    <marker id="process-map-arrow-return" markerUnits="userSpaceOnUse" markerWidth="12" markerHeight="12" refX="11" refY="6" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#6f83aa"></path></marker>
                </defs>
                <g class="viewport-wrap">
                    ${edgesSvg}
                    ${nodesSvg}
                </g>
            </svg>
            <div class="process-map-zoom-indicator" style="position: absolute; bottom: 16px; right: 16px; background: rgba(255, 255, 255, 0.9); padding: 4px 10px; border-radius: 4px; font-size: 14px; font-weight: 700; color: #1f335e; box-shadow: 0 2px 6px rgba(0,0,0,0.15); pointer-events: none;">100%</div>
        </div>
    `;
}

function renderProcessFlowMap(patternRows, transitionRows = [], frequencyRows = [], options = {}) {
    const flowData = buildProcessFlowData(patternRows, transitionRows, frequencyRows);
    return renderProcessFlowMapFromData(flowData, options);
}

function renderProcessMapEmpty(message) {
    return `
        <div class="process-map-empty">
            <p>${escapeHtml(message)}</p>
        </div>
    `;
}

function renderProcessHeatLegend() {
    const legendLevels = ["heat-1", "heat-2", "heat-3", "heat-4", "heat-5"];

    return `
        <section class="process-explorer-legend" aria-label="ヒートマップ凡例">
            <div class="process-explorer-control-head">
                <span>ヒートマップ</span>
                <strong>平均所要時間</strong>
            </div>
            <div class="process-explorer-legend-body">
                <p class="process-explorer-legend-copy">遷移ごとの平均所要時間を色で示します。線 (遷移) は次のステップまでの所要時間、ノード (アクティビティ) はそこを経由する遷移の所要時間を反映します。赤いほど所要時間が長い箇所です。</p>
                <div class="process-explorer-legend-scale">
                    <span class="process-explorer-legend-boundary">HIGH</span>
                    <div class="process-explorer-legend-swatches">
                        ${legendLevels.slice().reverse().map((levelClassName) => `
                            <div class="process-explorer-legend-item">
                                <span class="process-explorer-legend-node ${levelClassName}"></span>
                                <span class="process-explorer-legend-edge ${levelClassName}"></span>
                            </div>
                        `).join("")}
                    </div>
                    <span class="process-explorer-legend-boundary">LOW</span>
                </div>
            </div>
        </section>
    `;
}

function renderProcessRuleLegend(options = {}) {
    const { showVariantRules = false } = options;
    const baseItems = [
        {
            key: "main-spine",
            label: "メインルート (実線・太)",
            description: "最も件数の多い主経路です。",
            toneClass: "process-rule-legend-visual--main-spine",
        },
        {
            key: "forward",
            label: "順方向の遷移 (実線)",
            description: "前のステップから後のステップへ進む遷移です。",
            toneClass: "process-rule-legend-visual--forward",
        },
        {
            key: "return",
            label: "戻り・ループ (破線)",
            description: "後のステップから前のステップへ戻る遷移です。",
            toneClass: "process-rule-legend-visual--return",
        },
        {
            key: "selected",
            label: "選択強調 (外枠 / シャドウ)",
            description: "アクティビティ / 遷移 / ケース追跡で選択中の対象です。",
            toneClass: "process-rule-legend-visual--selected",
        },
        {
            key: "dimmed",
            label: "非選択 (薄表示)",
            description: "選択中以外のアクティビティ / 遷移を薄く表示します。",
            toneClass: "process-rule-legend-visual--dimmed",
        },
    ];
    const variantItems = [
        {
            key: "variant-added",
            label: "バリアント固有ルート (差分色)",
            description: "比較基準バリアントに無いアクティビティ / 遷移を差分色で示します。",
            toneClass: "process-rule-legend-visual--variant-added",
        },
        {
            key: "variant-common",
            label: "バリアント共通ルート (共通色)",
            description: "比較基準バリアントと共通するルートです。",
            toneClass: "process-rule-legend-visual--variant-common",
        },
        {
            key: "branch-focus",
            label: "分岐ポイント選択 (専用色)",
            description: "差分サマリーの分岐ポイントで選択した区間です。",
            toneClass: "process-rule-legend-visual--branch-focus",
        },
    ];
    const ruleItems = showVariantRules ? baseItems.concat(variantItems) : baseItems;

    return `
        <section class="process-rule-legend" aria-label="フロー図の表示ルール">
            <div class="process-explorer-control-head">
                <span>表示ルール</span>
                <strong>ガイド</strong>
            </div>
            <div class="process-rule-legend-grid">
                ${ruleItems.map((item) => `
                    <article class="process-rule-legend-item" data-process-rule-key="${escapeHtml(item.key)}">
                        <div class="process-rule-legend-visual ${escapeHtml(item.toneClass)}" aria-hidden="true">
                            <span class="process-rule-legend-node"></span>
                            <span class="process-rule-legend-edge"></span>
                        </div>
                        <div class="process-rule-legend-copy">
                            <strong>${escapeHtml(item.label)}</strong>
                            <p>${escapeHtml(item.description)}</p>
                        </div>
                    </article>
                `).join("")}
            </div>
            <p class="process-rule-legend-note">所要時間の色分けはヒートマップを参照してください。</p>
        </section>
    `;
}

// -----------------------------------------------------------------------------
// Detail page interactive flow explorer
// -----------------------------------------------------------------------------

async function initializePatternFlowExplorer(runId, impact = null) {
    const mapViewport = document.getElementById("process-map-viewport");
    const patternsSlider = document.getElementById("process-map-patterns-slider");
    const activitiesSlider = document.getElementById("process-map-activities-slider");
    const labelModeCountButton = document.getElementById("process-map-label-mode-count");
    const labelModeDurationButton = document.getElementById("process-map-label-mode-duration");
    const labelModeMeta = document.getElementById("process-map-label-mode-meta");
    const patternsValue = document.getElementById("process-map-patterns-value");
    const activitiesValue = document.getElementById("process-map-activities-value");
    const patternsMeta = document.getElementById("process-map-patterns-meta");
    const activitiesMeta = document.getElementById("process-map-activities-meta");
    const exportSvgButton = document.getElementById("process-map-export-svg");
    const exportPngButton = document.getElementById("process-map-export-png");
    const variantList = document.getElementById("variant-list");
    const variantPagination = document.getElementById("variant-pagination");
    const variantResetButton = document.getElementById("variant-reset-button");
    const variantCoverageMeta = document.getElementById("variant-coverage-meta");
    const variantSelectionTitle = document.getElementById("variant-selection-title");
    const variantSelectionMeta = document.getElementById("variant-selection-meta");
    const variantSelectionSequence = document.getElementById("variant-selection-sequence");
    const variantSelectionDiff = document.getElementById("variant-selection-diff");
    const variantSearchInput = document.getElementById("variant-search-input");
    const variantSortSelect = document.getElementById("variant-sort-select");
    const variantDisplayLimitSelect = document.getElementById("variant-display-limit-select");
    const variantResultsMeta = document.getElementById("variant-results-meta");
    const impactPanel = chartContainer.querySelector(".impact-panel");
    const detailFilterForm = document.getElementById("detail-light-filter-form");
    const detailFilterDateFromInput = document.getElementById("detail-light-date-from");
    const detailFilterDateToInput = document.getElementById("detail-light-date-to");
    const detailFilterActivityModeSelect = document.getElementById("detail-light-activity-mode");
    const detailFilterActivityValuesSelect = document.getElementById("detail-light-activity-values");
    const detailFilterResetButton = document.getElementById("detail-light-filter-reset");
    const filterSummaryMeta = document.getElementById("detail-filter-summary");
    const filterCountMeta = document.getElementById("detail-filter-counts");
    const currentSelectionState = document.getElementById("current-selection-state");
    const currentSelectionStateTitle = document.getElementById("current-selection-state-title");
    const currentSelectionStateMeta = document.getElementById("current-selection-state-meta");
    const caseTraceForm = document.getElementById("case-trace-form");
    const caseTraceInput = document.getElementById("case-trace-input");
    const caseTraceResult = document.getElementById("case-trace-result");
    const detailFilterValueSelects = FILTER_SLOT_KEYS.map((_, index) => document.getElementById(`detail-light-filter-value-${index + 1}`));
    const detailFilterLabelElements = FILTER_SLOT_KEYS.map((_, index) => document.getElementById(`detail-light-filter-label-${index + 1}`));
    let selectedVariantId = null;
    let selectedActivity = "";
    let selectedTransitionKey = "";
    const initialPatternPercent = Number(patternsSlider.value);
    const initialActivityPercent = Number(activitiesSlider.value);
    let caseTraceActivities = new Set();
    let caseTraceTransitions = new Set();
    let variants = [];
    let variantCoverage = null;
    let variantCoverageTotalCaseCount = 0;
    let variantErrorMessage = "";
    let variantViewState = getDefaultVariantViewState();
    let variantDiffState = buildVariantDiffState([], null);
    let variantBranchFocusState = null;
    let currentSelectionSource = "";
    let bottleneckSummary = null;
    let bottleneckErrorMessage = "";
    let transitionCaseRows = [];
    let transitionCaseErrorMessage = "";
    let searchedCaseId = "";
    let caseTracePayload = null;
    let caseTraceErrorMessage = "";
    let filterDefinitions = buildDefaultFilterDefinitions();
    let impactSummary = impact || { has_data: false, rows: [] };
    let impactViewState = getDefaultImpactViewState();
    let draftDetailFilters = cloneDetailFilters(activeDetailFilters);
    let filterOptionsPayload = null;
    let currentLabelMode = "count";
    let filteredCounts = {
        caseCount: 0,
        eventCount: 0,
    };
    const savedFlowSelection = loadFlowSelection(runId);

    if (savedFlowSelection) {
        selectedVariantId = savedFlowSelection.variant_id ?? null;
        selectedActivity = savedFlowSelection.selected_activity || "";
        selectedTransitionKey = savedFlowSelection.selected_transition_key || "";
    }

    if (!runId) {
        if (mapViewport) {
            mapViewport.innerHTML = renderProcessMapEmpty("分析結果が見つかりません。TOP 画面から再度実行してください。");
        }
        return;
    }

    if (
        !mapViewport
        || !patternsSlider
        || !activitiesSlider
        || !labelModeCountButton
        || !labelModeDurationButton
        || !labelModeMeta
        || !variantList
        || !variantPagination
        || !variantResetButton
        || !variantCoverageMeta
        || !variantSelectionTitle
        || !variantSelectionMeta
        || !variantSelectionSequence
        || !variantSelectionDiff
        || !variantSearchInput
        || !variantSortSelect
        || !variantDisplayLimitSelect
        || !variantResultsMeta
        || !detailFilterForm
        || !detailFilterDateFromInput
        || !detailFilterDateToInput
        || !detailFilterActivityModeSelect
        || !detailFilterActivityValuesSelect
        || !detailFilterResetButton
        || !filterSummaryMeta
        || !filterCountMeta
        || !currentSelectionState
        || !currentSelectionStateTitle
        || !currentSelectionStateMeta
        || !caseTraceForm
        || !caseTraceInput
        || !caseTraceResult
    ) {
        return;
    }

    let requestVersion = 0;

    if (detailExportExcelButton) {
        detailExportExcelButton.onclick = () => {
            void downloadDetailExcelExport(runId, {
                analysisKeyName: analysisKey,
                patternDisplayLimit: variantViewState.displayLimit,
                filters: activeDetailFilters,
                variantId: selectedVariantId,
                selectedActivity,
                selectedTransitionKey,
                caseId: caseTracePayload?.found ? caseTracePayload.case_id : "",
            });
        };
    }
    syncDetailExportPanel(detailPageTitle?.textContent?.trim() || "処理順パターン分析", {
        filters: activeDetailFilters,
        variantId: selectedVariantId,
        selectedActivity,
        selectedTransitionKey,
        caseId: caseTracePayload?.found ? caseTracePayload.case_id : "",
        filterDefs: filterDefinitions,
    });

    function resolveTransitionLabel(transitionItem, transitionKey = "") {
        if (transitionItem) {
            return `${transitionItem.from_activity} → ${transitionItem.to_activity}`;
        }
        return String(transitionKey || "").replace("__TO__", " → ");
    }

    function findSelectedTransitionDetails() {
        if (!selectedTransitionKey) {
            return null;
        }

        const bottleneckTransition = (bottleneckSummary?.transition_bottlenecks || []).find((item) => {
            const transitionKey = item.transition_key || buildTransitionKey(item.from_activity, item.to_activity);
            return transitionKey === selectedTransitionKey;
        });
        if (bottleneckTransition) {
            return bottleneckTransition;
        }

        const impactTransition = (impactSummary?.rows || []).find((item) => {
            const transitionKey = item.transition_key || buildTransitionKey(item.from_activity, item.to_activity);
            return transitionKey === selectedTransitionKey;
        });
        if (impactTransition) {
            return impactTransition;
        }

        return null;
    }

    function syncProcessMapLabelModeControls(visibleLabelCount = 0, totalLabelCount = 0) {
        const normalizedLabelMode = normalizeProcessMapLabelMode(currentLabelMode);
        labelModeCountButton.classList.toggle("is-active", normalizedLabelMode === "count");
        labelModeDurationButton.classList.toggle("is-active", normalizedLabelMode === "duration");
        labelModeCountButton.setAttribute("aria-pressed", normalizedLabelMode === "count" ? "true" : "false");
        labelModeDurationButton.setAttribute("aria-pressed", normalizedLabelMode === "duration" ? "true" : "false");
        labelModeMeta.textContent = totalLabelCount > 0
            ? `表示中: ${normalizedLabelMode === "count" ? "件数" : "平均所要時間"} / ${visibleLabelCount} ラベル`
            : `表示中: ${normalizedLabelMode === "count" ? "件数" : "平均所要時間"}`;
        applyProcessMapLabelMode(mapViewport, normalizedLabelMode);
    }

    function getSelectableTransitionKeys() {
        const transitionKeys = new Set();

        (bottleneckSummary?.transition_bottlenecks || []).forEach((item) => {
            const transitionKey = item.transition_key || buildTransitionKey(item.from_activity, item.to_activity);
            if (transitionKey) {
                transitionKeys.add(transitionKey);
            }
        });

        (impactSummary?.rows || []).forEach((item) => {
            const transitionKey = item.transition_key || buildTransitionKey(item.from_activity, item.to_activity);
            if (transitionKey) {
                transitionKeys.add(transitionKey);
            }
        });

        return transitionKeys;
    }

    function renderFilterSummary() {
        filterSummaryMeta.textContent = buildFilterSelectionSummary(activeDetailFilters, filterDefinitions);
        filterCountMeta.textContent = `対象ケース数 ${Number(filteredCounts.caseCount || 0).toLocaleString("ja-JP")} / 対象イベント数 ${Number(filteredCounts.eventCount || 0).toLocaleString("ja-JP")}`;
    }

    function syncDetailFilterControls() {
        const nextDraftFilters = cloneDetailFilters(draftDetailFilters);

        if (detailFilterDateFromInput.value !== nextDraftFilters.date_from) {
            detailFilterDateFromInput.value = nextDraftFilters.date_from;
        }
        if (detailFilterDateToInput.value !== nextDraftFilters.date_to) {
            detailFilterDateToInput.value = nextDraftFilters.date_to;
        }
        if (detailFilterActivityModeSelect.value !== nextDraftFilters.activity_mode) {
            detailFilterActivityModeSelect.value = nextDraftFilters.activity_mode;
        }

        const activityOptions = buildActivityFilterOptions(variants, nextDraftFilters.activity_values);
        replaceMultiSelectOptions(detailFilterActivityValuesSelect, activityOptions, nextDraftFilters.activity_values);
        detailFilterActivityModeSelect.disabled = activityOptions.length === 0 && nextDraftFilters.activity_values.length === 0;
        detailFilterActivityValuesSelect.disabled = activityOptions.length === 0 && nextDraftFilters.activity_values.length === 0;

        filterDefinitions.forEach((definition, index) => {
            const detailFilterLabelElement = detailFilterLabelElements[index];
            const detailFilterValueSelect = detailFilterValueSelects[index];
            const selectedValue = nextDraftFilters[definition.slot] || "";

            if (detailFilterLabelElement) {
                detailFilterLabelElement.textContent = definition.label || DEFAULT_FILTER_LABELS[definition.slot];
            }
            if (detailFilterValueSelect) {
                replaceSingleSelectOptions(
                    detailFilterValueSelect,
                    definition.options || [],
                    selectedValue,
                    "全て"
                );
                detailFilterValueSelect.disabled = !definition.column_name && !selectedValue;
            }
        });
    }

    function readDraftDetailFilters() {
        const nextDraftFilters = cloneDetailFilters({
            date_from: detailFilterDateFromInput.value,
            date_to: detailFilterDateToInput.value,
            filter_value_1: detailFilterValueSelects[0]?.value || "",
            filter_value_2: detailFilterValueSelects[1]?.value || "",
            filter_value_3: detailFilterValueSelects[2]?.value || "",
            activity_mode: detailFilterActivityModeSelect.value,
            activity_values: readMultiSelectValues(detailFilterActivityValuesSelect),
        });

        draftDetailFilters = nextDraftFilters;
        return nextDraftFilters;
    }

    async function refreshDetailAnalysisPanels() {
        const detailData = await loadAnalysisPage(runId, 0, activeDetailFilters, getInitialDetailLoadOptions());
        const analysis = detailData.analyses[analysisKey];

        if (!analysis) {
            throw new Error("指定した分析結果が見つかりません。");
        }

        currentDetailColumnSettings = detailData.column_settings || currentDetailColumnSettings;
        filterDefinitions = normalizeFilterDefinitions(
            filterOptionsPayload?.options?.filters || [],
            currentDetailColumnSettings
        );
        activeDetailFilters = cloneDetailFilters(detailData.applied_filters || DEFAULT_DETAIL_FILTERS);
        draftDetailFilters = cloneDetailFilters(activeDetailFilters);
        impactSummary = detailData.impact || { has_data: false, rows: [] };
        currentRenderedAnalysis = analysis;
        currentDetailSummaryData = {
            source_file_name: detailData.source_file_name,
            case_count: detailData.case_count,
            event_count: detailData.event_count,
            dashboard: detailData.dashboard,
            impact: detailData.impact,
            insights: detailData.insights,
            root_cause: detailData.root_cause,
            applied_filters: detailData.applied_filters,
            column_settings: detailData.column_settings,
        };
        pendingDetailSupplementSections = new Set(detailData.deferred_sections || []);
        detailSupplementErrorMessage = "";
        filteredCounts = {
            caseCount: Number(detailData.case_count || 0),
            eventCount: Number(detailData.event_count || 0),
        };

        renderSummary(currentDetailSummaryData, analysis);
        if (analysisKey === "pattern") {
            resultPanel.className = "result-panel hidden";
            resultPanel.innerHTML = "";
        } else {
            renderResult(analysis, runId, detailPageAnalysisLoader);
        }
        syncDetailExportPanel(analysis.analysis_name, {
            filters: activeDetailFilters,
            variantId: selectedVariantId,
            selectedActivity,
            selectedTransitionKey,
            caseId: caseTracePayload?.found ? caseTracePayload.case_id : "",
            filterDefs: filterDefinitions,
        });
        renderFilterSummary();
        syncDetailFilterControls();
        void syncAiInsightsPanel(runId, analysis.analysis_name);
        void refreshDeferredDetailSections(runId);
        return detailData;
    }

    async function applyDetailFilters() {
        readDraftDetailFilters();
        activeDetailFilters = cloneDetailFilters(draftDetailFilters);
        resetSelectionState();
        setStatus("分析対象条件を適用しています...", "info");

        try {
            await refreshDetailAnalysisPanels();
            await Promise.all([
                refreshVariantSummary(),
                refreshBottleneckSummary(),
            ]);
            syncDetailFilterControls();
            syncVariantPanel();
            syncBottleneckPanel();
            syncImpactPanel();
            renderCaseTracePanel();
            renderTransitionCasePanel();
            await updateProcessMap();
            hideStatus();
        } catch (error) {
            setStatus(error.message, "error");
        }
    }

    async function resetDetailFilters() {
        draftDetailFilters = cloneDetailFilters(DEFAULT_DETAIL_FILTERS);
        syncDetailFilterControls();
        patternsSlider.value = initialPatternPercent;
        patternsValue.textContent = `${initialPatternPercent}%`;
        activitiesSlider.value = initialActivityPercent;
        activitiesValue.textContent = `${initialActivityPercent}%`;
        await applyDetailFilters();
    }

    function getFallbackSelectionSource() {
        if (caseTraceActivities.size > 0 || caseTraceTransitions.size > 0) {
            return "case-trace";
        }
        if (variantBranchFocusState?.focusId) {
            return "branch-focus";
        }
        if (selectedTransitionKey) {
            return "transition";
        }
        if (selectedActivity) {
            return "activity-bottleneck";
        }
        if (selectedVariantId !== null) {
            return "variant";
        }
        return "";
    }

    function getActiveSelectionSource() {
        switch (currentSelectionSource) {
        case "case-trace":
            return (caseTraceActivities.size > 0 || caseTraceTransitions.size > 0)
                ? currentSelectionSource
                : getFallbackSelectionSource();
        case "branch-focus":
            return variantBranchFocusState?.focusId
                ? currentSelectionSource
                : getFallbackSelectionSource();
        case "transition-bottleneck":
        case "impact":
        case "transition":
            return selectedTransitionKey
                ? currentSelectionSource
                : getFallbackSelectionSource();
        case "activity-bottleneck":
            return selectedActivity
                ? currentSelectionSource
                : getFallbackSelectionSource();
        case "variant":
            return selectedVariantId !== null
                ? currentSelectionSource
                : getFallbackSelectionSource();
        default:
            return getFallbackSelectionSource();
        }
    }

    function buildCurrentSelectionState() {
        const activeSource = getActiveSelectionSource();
        const selectedVariant = variants.find((variant) => Number(variant.variant_id) === Number(selectedVariantId));
        const selectedTransition = selectedTransitionKey
            ? findSelectedTransitionDetails()
            : null;
        const transitionLabel = selectedTransition
            ? resolveTransitionLabel(selectedTransition, selectedTransitionKey)
            : resolveTransitionLabel(null, selectedTransitionKey);
        const branchLabel = variantBranchFocusState?.anchor || "差分区間";
        const caseTraceCaseId = caseTracePayload?.case_id || searchedCaseId || "";

        switch (activeSource) {
        case "case-trace":
            return {
                source: activeSource,
                title: `ケース追跡: ${caseTraceCaseId || "-"}`,
                meta: "ケースID検索で見つかったケース経路を強調しています。全体表示で解除できます。",
            };
        case "branch-focus":
            return {
                source: activeSource,
                title: `分岐ポイント選択: ${branchLabel}`,
                meta: "バリアント差分サマリーで選択した差分区間を強調しています。全体表示で解除できます。",
            };
        case "impact":
            return {
                source: activeSource,
                title: `改善インパクト: ${transitionLabel}`,
                meta: "改善インパクト分析で選択した遷移を強調しています。全体表示で解除できます。",
            };
        case "transition-bottleneck":
            return {
                source: activeSource,
                title: `遷移ボトルネック: ${transitionLabel}`,
                meta: "Bottleneck Analysis で選択した遷移を強調しています。全体表示で解除できます。",
            };
        case "transition":
            return {
                source: activeSource,
                title: `遷移選択: ${transitionLabel}`,
                meta: "選択した遷移を強調しています。全体表示で解除できます。",
            };
        case "activity-bottleneck":
            return {
                source: activeSource,
                title: `アクティビティボトルネック: ${selectedActivity || "-"}`,
                meta: "Bottleneck Analysis で選択したアクティビティを強調しています。全体表示で解除できます。",
            };
        case "variant":
            return {
                source: activeSource,
                title: selectedVariant
                    ? `Variant #${selectedVariant.variant_id} 選択中`
                    : `Variant #${selectedVariantId} 選択中`,
                meta: "選択したバリアントに属するケースでフロー図と分析結果を表示しています。全体表示で解除できます。",
            };
        default:
            return {
                source: "none",
                title: "全体表示中",
                meta: "現在は全ケースを使ったフロー図を表示しています。パターン / バリアント一覧の全体表示で解除できます。",
            };
        }
    }

    function renderCurrentSelectionState() {
        const selectionState = buildCurrentSelectionState();
        currentSelectionState.dataset.selectionSource = selectionState.source || "none";
        currentSelectionStateTitle.textContent = selectionState.title;
        currentSelectionStateMeta.textContent = selectionState.meta;
    }

    function getActiveProcessRuleKeys() {
        const activeKeys = new Set();
        const hasCaseTraceSelection = caseTraceActivities.size > 0 || caseTraceTransitions.size > 0;
        const hasTransitionSelection = Boolean(selectedActivity || selectedTransitionKey);

        if (variantDiffState?.enabled) {
            activeKeys.add("variant-added");
            activeKeys.add("variant-common");
        }

        if (variantBranchFocusState?.focusId) {
            activeKeys.add("branch-focus");
            activeKeys.add("dimmed");
            return activeKeys;
        }

        if (hasCaseTraceSelection || hasTransitionSelection) {
            activeKeys.add("selected");
            activeKeys.add("dimmed");
        }

        return activeKeys;
    }

    function syncProcessRuleLegend() {
        const activeKeys = getActiveProcessRuleKeys();
        chartContainer.querySelectorAll("[data-process-rule-key]").forEach((legendItemElement) => {
            const ruleKey = legendItemElement.dataset.processRuleKey || "";
            legendItemElement.classList.toggle("is-active", activeKeys.has(ruleKey));
        });
    }

    function syncVariantControls() {
        if (variantSearchInput.value !== variantViewState.searchTerm) {
            variantSearchInput.value = variantViewState.searchTerm;
        }
        if (variantSortSelect.value !== variantViewState.sortKey) {
            variantSortSelect.value = variantViewState.sortKey;
        }
        if (variantDisplayLimitSelect.value !== String(variantViewState.displayLimit)) {
            variantDisplayLimitSelect.value = String(variantViewState.displayLimit);
        }
    }

    function resetVariantBranchFocusState() {
        variantBranchFocusState = null;
    }

    function findVariantBranchFocusState(focusId) {
        if (!focusId || !Array.isArray(variantDiffState?.branchPoints)) {
            return null;
        }

        return variantDiffState.branchPoints.find((branchPoint) => branchPoint.focusId === focusId) || null;
    }

    function scrollVariantBranchFocusIntoView(viewportElement, focusState) {
        if (!focusState) {
            return;
        }

        const firstActivity = Array.isArray(focusState.focusActivities)
            ? focusState.focusActivities.find(Boolean)
            : "";
        const processMapWrap = viewportElement.querySelector(".process-map-wrap");
        const nodeElement = firstActivity
            ? viewportElement.querySelector(`.process-map-node-group[data-node="${CSS.escape(firstActivity)}"]`)
            : null;

        if (processMapWrap && nodeElement) {
            const wrapRect = processMapWrap.getBoundingClientRect();
            const nodeRect = nodeElement.getBoundingClientRect();
            const nextTop = processMapWrap.scrollTop
                + (nodeRect.top - wrapRect.top)
                - Math.max(0, (wrapRect.height - nodeRect.height) / 2);
            const nextLeft = processMapWrap.scrollLeft
                + (nodeRect.left - wrapRect.left)
                - Math.max(0, (wrapRect.width - nodeRect.width) / 2);

            processMapWrap.scrollTo({
                top: Math.max(0, nextTop),
                left: Math.max(0, nextLeft),
                behavior: "smooth",
            });
        }
    }

    async function applyVariantBranchFocus(nextFocusId) {
        const nextFocusState = variantBranchFocusState?.focusId === nextFocusId
            ? null
            : findVariantBranchFocusState(nextFocusId);

        variantBranchFocusState = nextFocusState;
        currentSelectionSource = nextFocusState
            ? "branch-focus"
            : (selectedVariantId !== null ? "variant" : "");
        selectedActivity = "";
        selectedTransitionKey = "";
        resetCaseTraceHighlightState();
        transitionCaseRows = [];
        transitionCaseErrorMessage = "";
        saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
        syncVariantPanel();
        syncBottleneckPanel();
        syncImpactPanel();
        renderTransitionCasePanel();
        await updateProcessMap();
    }

    function bindImpactNumberInput(inputElement, stateKey) {
        if (!inputElement) {
            return;
        }

        const applyInputValue = () => {
            impactViewState[stateKey] = String(inputElement.value || "").trim();
            syncImpactPanel();
        };

        inputElement.addEventListener("change", applyInputValue);
        inputElement.addEventListener("keydown", (event) => {
            if (event.key !== "Enter") {
                return;
            }
            event.preventDefault();
            applyInputValue();
        });
    }

    function syncImpactPanel() {
        let currentImpactPanel = chartContainer.querySelector(".impact-panel");
        if (!currentImpactPanel) {
            return;
        }

        currentImpactPanel.outerHTML = buildImpactSectionHtml(impactSummary, impactViewState);
        currentImpactPanel = chartContainer.querySelector(".impact-panel");

        currentImpactPanel.querySelector("#impact-sort-select")?.addEventListener("change", (event) => {
            impactViewState.sortKey = event.target.value || "impact_score";
            syncImpactPanel();
        });
        bindImpactNumberInput(currentImpactPanel.querySelector("#impact-min-case-count-input"), "minCaseCount");
        bindImpactNumberInput(currentImpactPanel.querySelector("#impact-min-avg-hours-input"), "minAvgDurationHours");
        currentImpactPanel.querySelector("#impact-display-limit-select")?.addEventListener("change", (event) => {
            impactViewState.displayLimit = Number(event.target.value || 10);
            syncImpactPanel();
        });

        const rowElements = Array.from(currentImpactPanel.querySelectorAll("tbody tr"));
        rowElements.forEach((rowElement) => {
            const transitionKey = rowElement.dataset.impactTransitionKey || "";
            if (!transitionKey) {
                return;
            }
            const isSelected = Boolean(selectedTransitionKey) && transitionKey === selectedTransitionKey;

            rowElement.classList.add("transition-step-row");
            rowElement.classList.toggle("transition-step-row--selected", isSelected);
            rowElement.setAttribute("tabindex", "0");
            rowElement.setAttribute("aria-selected", isSelected ? "true" : "false");
            rowElement.onclick = () => {
                void applyTransitionSelection(transitionKey, "impact");
            };
            rowElement.onkeydown = (event) => {
                if (event.key !== "Enter" && event.key !== " ") {
                    return;
                }
                event.preventDefault();
                void applyTransitionSelection(transitionKey, "impact");
            };
        });
    }

    function resetSelectionState() {
        selectedVariantId = null;
        selectedActivity = "";
        selectedTransitionKey = "";
        caseTraceActivities = new Set();
        caseTraceTransitions = new Set();
        resetVariantBranchFocusState();
        currentSelectionSource = "";
        transitionCaseRows = [];
        transitionCaseErrorMessage = "";
        saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
    }

    function resetCaseTraceHighlightState() {
        caseTraceActivities = new Set();
        caseTraceTransitions = new Set();
    }

    function applyCaseTraceHighlight(payload) {
        const activitySet = new Set();
        const transitionSet = new Set();
        const events = Array.isArray(payload?.events) ? payload.events : [];

        events.forEach((eventRow) => {
            const activityName = String(eventRow.activity || "").trim();
            const nextActivityName = String(eventRow.next_activity || "").trim();

            if (activityName) {
                activitySet.add(activityName);
            }
            if (activityName && nextActivityName) {
                transitionSet.add(buildTransitionKey(activityName, nextActivityName));
            }
        });

        caseTraceActivities = activitySet;
        caseTraceTransitions = transitionSet;
    }

    async function applyTransitionSelection(nextTransitionKey, selectionSource = "transition") {
        selectedTransitionKey = selectedTransitionKey === nextTransitionKey ? "" : nextTransitionKey;
        currentSelectionSource = selectedTransitionKey
            ? selectionSource
            : (selectedVariantId !== null ? "variant" : "");
        selectedActivity = "";
        resetCaseTraceHighlightState();
        resetVariantBranchFocusState();
        transitionCaseRows = [];
        transitionCaseErrorMessage = "";
        saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
        syncVariantPanel();
        syncBottleneckPanel();
        syncImpactPanel();
        await updateProcessMap();
        await loadSelectedTransitionCases();
    }

    function getDrilldownPanelConfig() {
        if (selectedTransitionKey) {
            const selectedTransition = findSelectedTransitionDetails();
            return {
                title: "遷移ケースドリルダウン",
                meta: selectedTransition
                    ? resolveTransitionLabel(selectedTransition, selectedTransitionKey)
                    : resolveTransitionLabel(null, selectedTransitionKey),
                emptyMessage: "遷移別ボトルネックを選択すると、時間の長いケースを表示します。",
                note: "上位 20 件を表示します。所要時間の降順です。",
            };
        }

        if (selectedActivity) {
            return {
                title: "アクティビティケースドリルダウン",
                meta: `アクティビティボトルネック「${selectedActivity}」の所要時間が長いケースです。`,
                emptyMessage: `アクティビティボトルネック「${selectedActivity}」を選択すると、時間の長いケースを表示します。`,
                note: "上位 20 件を表示します。所要時間の降順です。",
            };
        }

        return {
            title: "ボトルネックケースドリルダウン",
            meta: "アクティビティまたは遷移のボトルネックを選択すると、時間の長いケースを表示します。",
            emptyMessage: "アクティビティまたは遷移のボトルネックを選択すると、時間の長いケースを表示します。",
            note: "上位 20 件を表示します。所要時間の降順です。",
        };
    }

    function renderTransitionCasePanel() {
        if (!transitionCasePanel) return;
        const drilldownConfig = getDrilldownPanelConfig();
        const hasTransitionSelection = Boolean(selectedTransitionKey || selectedActivity);
        transitionCasePanel.className = "result-panel";

        if (!hasTransitionSelection) {
            transitionCasePanel.innerHTML = `
                <div class="result-header">
                    <div>
                        <h2>${escapeHtml(drilldownConfig.title)}</h2>
                        <p class="result-meta">${escapeHtml(drilldownConfig.meta)}</p>
                    </div>
                </div>
                <p class="empty-state">${escapeHtml(drilldownConfig.emptyMessage)}</p>
            `;
            return;
        }

        if (transitionCaseErrorMessage) {
            transitionCasePanel.innerHTML = `
                <div class="result-header">
                    <div>
                        <h2>${escapeHtml(drilldownConfig.title)}</h2>
                        <p class="result-meta">${escapeHtml(drilldownConfig.meta)}</p>
                    </div>
                </div>
                <p class="empty-state">${escapeHtml(transitionCaseErrorMessage)}</p>
            `;
            return;
        }

        transitionCasePanel.innerHTML = `
            <div class="result-header">
                <div>
                    <h2>${escapeHtml(drilldownConfig.title)}</h2>
                    <p class="result-meta">${escapeHtml(drilldownConfig.meta)}</p>
                </div>
            </div>
            <p class="panel-note">${escapeHtml(drilldownConfig.note)}</p>
            ${buildCaseDrilldownTable(transitionCaseRows)}
        `;
    }

    async function loadSelectedTransitionCases() {
        if (!transitionCasePanel) return;
        if (!selectedTransitionKey && !selectedActivity) {
            transitionCaseRows = [];
            transitionCaseErrorMessage = "";
            renderTransitionCasePanel();
            return;
        }

        transitionCaseRows = [];
        transitionCaseErrorMessage = "";
        transitionCasePanel.className = "result-panel";
        const drilldownConfig = getDrilldownPanelConfig();
        transitionCasePanel.innerHTML = `
            <div class="result-header">
                <div>
                    <h2>${escapeHtml(drilldownConfig.title)}</h2>
                    <p class="result-meta">${escapeHtml(drilldownConfig.meta)}</p>
                </div>
            </div>
            <p class="panel-note">読み込み中...</p>
        `;

        try {
            if (selectedTransitionKey) {
                const selectedTransition = findSelectedTransitionDetails();

                if (!selectedTransition) {
                    transitionCaseRows = [];
                    transitionCaseErrorMessage = "遷移の詳細を読み込めませんでした。";
                    renderTransitionCasePanel();
                    return;
                }

                const payload = await loadTransitionCases(
                    runId,
                    selectedTransition.from_activity,
                    selectedTransition.to_activity,
                    20,
                    selectedVariantId,
                    activeDetailFilters,
                );
                transitionCaseRows = Array.isArray(payload.cases) ? payload.cases : [];
            } else if (selectedActivity) {
                const payload = await loadActivityCases(
                    runId,
                    selectedActivity,
                    20,
                    selectedVariantId,
                    activeDetailFilters,
                );
                transitionCaseRows = Array.isArray(payload.cases) ? payload.cases : [];
            }
        } catch (error) {
            transitionCaseErrorMessage = error.message;
        }

        renderTransitionCasePanel();
    }

    function renderCaseTracePanel() {
        if (!searchedCaseId) {
            caseTraceResult.innerHTML = '<p class="empty-state">ケースID を入力すると、ケースの通過順序と所要時間を表示します。</p>';
            return;
        }

        if (caseTraceErrorMessage) {
            caseTraceResult.innerHTML = `<p class="empty-state">${escapeHtml(caseTraceErrorMessage)}</p>`;
            return;
        }

        if (!caseTracePayload) {
            caseTraceResult.innerHTML = '<p class="panel-note">読み込み中...</p>';
            return;
        }

        if (!caseTracePayload.found) {
            caseTraceResult.innerHTML = `<p class="empty-state">ケースID「${escapeHtml(searchedCaseId)}」は見つかりませんでした。</p>`;
            return;
        }

        caseTraceResult.innerHTML = `
            ${buildCaseTraceSummaryHtml(caseTracePayload.case_id, caseTracePayload.summary)}
            <p class="panel-note">run 全体から検索したケース履歴です。時刻順にイベントを表示しています。</p>
            ${buildCaseTraceEventsTable(caseTracePayload.events || [])}
        `;
    }

    async function searchCaseTrace(caseId) {
        const normalizedCaseId = String(caseId || "").trim();
        caseTraceInput.value = normalizedCaseId;
        searchedCaseId = normalizedCaseId;
        caseTracePayload = null;
        caseTraceErrorMessage = "";

        if (!normalizedCaseId) {
            resetCaseTraceHighlightState();
            currentSelectionSource = getFallbackSelectionSource();
            renderCaseTracePanel();
            syncVariantPanel();
            await updateProcessMap();
            return;
        }

        // Keep case lookup stable even when a variant is selected.
        renderCaseTracePanel();

        try {
            caseTracePayload = await loadCaseTrace(runId, normalizedCaseId);
        } catch (error) {
            caseTraceErrorMessage = error.message;
        }

        renderCaseTracePanel();

        if (!caseTracePayload?.found) {
            resetCaseTraceHighlightState();
            resetVariantBranchFocusState();
            currentSelectionSource = getFallbackSelectionSource();
            syncVariantPanel();
            await updateProcessMap();
            return;
        }

        selectedVariantId = null;
        selectedActivity = "";
        selectedTransitionKey = "";
        resetVariantBranchFocusState();
        currentSelectionSource = "case-trace";
        transitionCaseRows = [];
        transitionCaseErrorMessage = "";
        applyCaseTraceHighlight(caseTracePayload);
        saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
        syncVariantPanel();
        syncBottleneckPanel();
        syncImpactPanel();
        renderTransitionCasePanel();
        await updateProcessMap();
    }

    async function refreshBottleneckSummary() {
        try {
            bottleneckErrorMessage = "";
            bottleneckSummary = await loadBottleneckSummary(runId, 5, selectedVariantId, activeDetailFilters);
            filteredCounts = {
                caseCount: Number(bottleneckSummary?.filtered_case_count || 0),
                eventCount: Number(bottleneckSummary?.filtered_event_count || 0),
            };
            renderFilterSummary();
            if (selectedActivity && !(bottleneckSummary.activity_bottlenecks || []).some((item) => item.activity === selectedActivity)) {
                selectedActivity = "";
            }
            if (selectedTransitionKey && !getSelectableTransitionKeys().has(selectedTransitionKey)) {
                selectedTransitionKey = "";
                transitionCaseRows = [];
                transitionCaseErrorMessage = "";
            }
            saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
        } catch (error) {
            bottleneckSummary = null;
            bottleneckErrorMessage = error.message;
            filteredCounts = {
                caseCount: 0,
                eventCount: 0,
            };
            renderFilterSummary();
        }
    }

    async function refreshVariantSummary() {
        try {
            variantErrorMessage = "";
            const variantPayload = await loadVariantList(runId, 0, activeDetailFilters);
            variants = Array.isArray(variantPayload.variants) ? variantPayload.variants : [];
            filteredCounts = {
                caseCount: Number(variantPayload.filtered_case_count || 0),
                eventCount: Number(variantPayload.filtered_event_count || 0),
            };
            variantCoverageTotalCaseCount = filteredCounts.caseCount;
            variantCoverage = buildVariantCoveragePayload(variants, variantCoverageTotalCaseCount, 10);
            renderFilterSummary();

            if (selectedVariantId !== null && !variants.some((variant) => Number(variant.variant_id) === Number(selectedVariantId))) {
                selectedVariantId = null;
                selectedActivity = "";
                selectedTransitionKey = "";
                transitionCaseRows = [];
                transitionCaseErrorMessage = "";
                saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
            }
            syncDetailFilterControls();
        } catch (error) {
            variantErrorMessage = error.message;
            variants = [];
            variantCoverage = null;
            variantCoverageTotalCaseCount = 0;
            filteredCounts = {
                caseCount: 0,
                eventCount: 0,
            };
            renderFilterSummary();
            syncDetailFilterControls();
        }
    }

    function syncBottleneckPanel() {
        // bottleneck panel removed; heatmap data is still applied via applyProcessMapDecorators
    }

    function syncVariantPanel() {
        if (variantErrorMessage) {
            variantDiffState = buildVariantDiffState([], null);
            variantList.innerHTML = `<p class="empty-state">${escapeHtml(variantErrorMessage)}</p>`;
            variantPagination.className = "result-pagination variant-pagination hidden";
            variantPagination.innerHTML = "";
            variantCoverageMeta.innerHTML = '<p class="panel-note">カバー率を読み込めませんでした。</p>';
            variantSelectionTitle.textContent = "パターン / バリアント情報を読み込めませんでした";
            variantSelectionMeta.textContent = "一覧の読み込みに失敗しました。";
            variantSelectionSequence.textContent = "";
            variantSelectionSequence.title = "";
            variantSelectionDiff.innerHTML = "";
            variantResultsMeta.textContent = "";
            variantSearchInput.disabled = true;
            variantSortSelect.disabled = true;
            variantDisplayLimitSelect.disabled = true;
            variantResetButton.disabled = true;
            patternsSlider.disabled = false;
            activitiesSlider.disabled = false;
            renderCurrentSelectionState();
            syncProcessRuleLegend();
            return;
        }

        const variantPageState = getVariantPageState(variants, variantViewState);
        variantViewState.page = variantPageState.currentPage;
        const visibleVariants = variantPageState.visibleVariants;
        const hasSearchTerm = Boolean(String(variantViewState.searchTerm || "").trim());
        const emptyMessage = hasSearchTerm
            ? "検索条件に一致するルートがありません。"
            : "表示できるルートがありません。";
        variantDiffState = buildVariantDiffState(variants, selectedVariantId);
        if (variantBranchFocusState && !findVariantBranchFocusState(variantBranchFocusState.focusId)) {
            resetVariantBranchFocusState();
        }

        syncVariantControls();
        variantSearchInput.disabled = false;
        variantSortSelect.disabled = false;
        variantDisplayLimitSelect.disabled = false;
        variantList.innerHTML = buildVariantCardsHtml(
            visibleVariants,
            selectedVariantId,
            emptyMessage,
            runId,
            Math.max(0, Number(variantPageState.startRowNumber || 1) - 1)
        );
        variantResetButton.disabled = !(
            selectedVariantId !== null
            || Boolean(selectedActivity)
            || Boolean(selectedTransitionKey)
            || Boolean(variantBranchFocusState?.focusId)
            || caseTraceActivities.size > 0
            || caseTraceTransitions.size > 0
        );
        patternsSlider.disabled = selectedVariantId !== null;
        activitiesSlider.disabled = selectedVariantId !== null;
        variantCoverage = buildVariantCoveragePayload(
            variantPageState.limitedVariants,
            variantCoverageTotalCaseCount,
            variantPageState.maxVisibleCount,
            { isAllDisplay: variantPageState.isAllDisplay }
        );
        variantCoverageMeta.innerHTML = buildVariantCoverageHtml(variantCoverage);
        variantResultsMeta.textContent = [
            `対象ルート ${Number(variants.length || 0).toLocaleString("ja-JP")} 件`,
            `条件一致 ${Number(variantPageState.filteredVariants.length || 0).toLocaleString("ja-JP")} 件`,
            `表示対象 ${Number(variantPageState.limitedVariants.length || 0).toLocaleString("ja-JP")} 件`,
            variantPageState.filteredVariants.length
                ? `${Number(variantPageState.startRowNumber || 0).toLocaleString("ja-JP")} - ${Number(variantPageState.endRowNumber || 0).toLocaleString("ja-JP")} 件目を表示`
                : "表示対象なし",
            buildVariantSortLabel(variantViewState.sortKey),
            variantPageState.totalPages > 1
                ? `${Number(variantPageState.pageSize || 0).toLocaleString("ja-JP")} 件ずつページ切り替え`
                : "",
        ].filter(Boolean).join(" / ");
        variantPagination.innerHTML = buildVariantPaginationHtml(variantPageState);
        variantPagination.className = variantPageState.totalPages > 1
            ? "result-pagination variant-pagination"
            : "result-pagination variant-pagination hidden";

        const selectionState = buildVariantSelectionState(variants, selectedVariantId);
        variantSelectionTitle.textContent = selectionState.title;
        variantSelectionMeta.textContent = selectionState.meta;
        variantSelectionSequence.textContent = selectionState.sequence;
        variantSelectionSequence.title = selectionState.titleAttribute;
        variantSelectionDiff.innerHTML = buildVariantDiffHtml(variantDiffState, variantBranchFocusState?.focusId || "");
        renderCurrentSelectionState();
        syncProcessRuleLegend();
        variantSelectionDiff.querySelectorAll("[data-branch-focus-id]").forEach((buttonElement) => {
            buttonElement.addEventListener("click", async () => {
                const focusId = buttonElement.dataset.branchFocusId || "";
                await applyVariantBranchFocus(focusId);
            });
        });

        variantList.querySelectorAll("[data-variant-id]").forEach((element) => {
            element.addEventListener("click", async (event) => {
                if (event.target.closest("a")) return;
                const clickedVariantId = Number(element.dataset.variantId);
                const nextVariantId = selectedVariantId === clickedVariantId
                    ? null
                    : clickedVariantId;
                await applyVariantSelection(nextVariantId);
            });
            element.addEventListener("keydown", async (event) => {
                if (event.key !== "Enter" && event.key !== " ") return;
                event.preventDefault();
                const clickedVariantId = Number(element.dataset.variantId);
                const nextVariantId = selectedVariantId === clickedVariantId
                    ? null
                    : clickedVariantId;
                await applyVariantSelection(nextVariantId);
            });
        });
        variantPagination.querySelectorAll("[data-variant-page]").forEach((buttonElement) => {
            buttonElement.addEventListener("click", () => {
                const nextPage = Number(buttonElement.dataset.variantPage || 1);
                if (!Number.isFinite(nextPage) || nextPage === variantViewState.page) {
                    return;
                }
                variantViewState.page = nextPage;
                syncVariantPanel();
                variantList.scrollIntoView({ block: "start", behavior: "smooth" });
            });
        });
    }

    function syncRuleLegend() {
        const container = document.getElementById("process-rule-legend-container");
        if (container) {
            container.innerHTML = renderProcessRuleLegend({ showVariantRules: selectedVariantId !== null });
        }
    }

    async function applyVariantSelection(nextVariantId) {
        if (nextVariantId === null) {
            resetSelectionState();
        } else {
            selectedVariantId = nextVariantId;
            currentSelectionSource = "variant";
            selectedActivity = "";
            selectedTransitionKey = "";
            resetCaseTraceHighlightState();
            resetVariantBranchFocusState();
            transitionCaseRows = [];
            transitionCaseErrorMessage = "";
            saveFlowSelection(runId, selectedVariantId, selectedActivity, selectedTransitionKey);
        }
        syncRuleLegend();
        await refreshBottleneckSummary();
        syncVariantPanel();
        syncBottleneckPanel();
        syncImpactPanel();
        await updateProcessMap();
    }

    async function updateProcessMap() {
        const currentVersion = requestVersion + 1;
        requestVersion = currentVersion;
        const patternPercent = Number(patternsSlider.value);
        const activityPercent = Number(activitiesSlider.value);
        const connectionPercent = 100;
        const labelPercent = 100;

        // Update labels instantly
        patternsValue.textContent = `${patternPercent}%`;
        activitiesValue.textContent = `${activityPercent}%`;

        mapViewport.innerHTML = renderProcessMapEmpty("フロー図を読み込んでいます...");

        try {
            const params = new URLSearchParams({
                pattern_percent: String(patternPercent),
                activity_percent: String(activityPercent),
                connection_percent: String(connectionPercent),
            });
            if (selectedVariantId !== null) {
                params.set("variant_id", String(selectedVariantId));
            }
            buildFilterQueryParams(activeDetailFilters).forEach((value, key) => {
                params.set(key, value);
            });

            const snapshot = await fetchJson(
                `/api/runs/${encodeURIComponent(runId)}/pattern-flow?${params.toString()}`,
                "処理フロー図の読み込みに失敗しました。",
                DETAIL_HEAVY_FETCH_TIMEOUT_MS
            );

            if (currentVersion !== requestVersion) {
                return;
            }

            const flowData = snapshot.flow_data || { nodes: [], edges: [] };
            const labelState = buildProcessMapLabelState(flowData.edges || [], labelPercent);
            filteredCounts = {
                caseCount: Number(snapshot.filtered_case_count || 0),
                eventCount: Number(snapshot.filtered_event_count || 0),
            };
            renderFilterSummary();
            const performanceSuffix = snapshot.is_large_dataset_optimized
                ? " / 大規模データ高速表示"
                : "";

            if (snapshot.selected_variant) {
                patternsMeta.textContent = `Variant #${snapshot.selected_variant.variant_id} / 1 パターン${performanceSuffix}`;
            } else {
                patternsMeta.textContent = `${snapshot.pattern_window.used_pattern_count} / ${snapshot.pattern_window.effective_pattern_count} パターン${performanceSuffix}`;
            }
            activitiesMeta.textContent = `${snapshot.activity_window.visible_activity_count} / ${snapshot.activity_window.available_activity_count} アクティビティ`;

            if (!flowData.nodes.length) {
                mapViewport.innerHTML = renderProcessMapEmpty("表示できるフロー図がありません。表示率を広げてください。");
                return;
            }

            const edgeCount = flowData.edges.length;
            const totalToRender = flowData.nodes.length + edgeCount;
            if (totalToRender > AGGRESSIVE_LIMIT) {
                mapViewport.innerHTML = renderProcessMapEmpty(
                    `図が複雑すぎるため（要素数: ${totalToRender.toLocaleString()}）、現在の表示率では描画を停止しました。スライダーを下げてください。`
                );
                return;
            }

            if (totalToRender > RENDERING_LIMIT || edgeCount > EDGE_LIMIT) {
                const reason = totalToRender > RENDERING_LIMIT
                    ? `要素数: ${totalToRender.toLocaleString()}`
                    : `線の数: ${edgeCount.toLocaleString()}`;
                mapViewport.innerHTML = renderProcessMapEmpty(
                    `図が複雑すぎるため（${reason}）、ブラウザのフリーズを防ぐために描画を停止しました。スライダーで表示率を下げてください。`
                );
                return;
            }

            mapViewport.innerHTML = renderProcessFlowMapFromData(flowData, {
                labelPercent,
                labelMode: currentLabelMode,
                activityPercent: 100,
                connectionPercent: 100,
                compactMode: selectedVariantId !== null,
            });
            applyProcessMapDecorators(mapViewport, {
                activityHeatmap: bottleneckSummary?.activity_heatmap || {},
                transitionHeatmap: bottleneckSummary?.transition_heatmap || {},
                selectedActivity,
                selectedTransitionKey,
                caseTraceActivities,
                caseTraceTransitions,
                variantDiffState,
                variantBranchFocusState,
            });
            scrollVariantBranchFocusIntoView(mapViewport, variantBranchFocusState);
            attachProcessMapInteractions(mapViewport);
            syncProcessMapLabelModeControls(labelState.visibleLabelCount, labelState.totalLabelCount);
        } catch (error) {
            if (currentVersion !== requestVersion) {
                return;
            }
            patternsMeta.textContent = "";
            activitiesMeta.textContent = "";
            filteredCounts = {
                caseCount: 0,
                eventCount: 0,
            };
            renderFilterSummary();
            mapViewport.innerHTML = renderProcessMapEmpty(error.message);
            syncProcessMapLabelModeControls();
        }
    }

    const debouncedUpdate = debounce(updateProcessMap, 300);

    patternsSlider.addEventListener("input", () => {
        patternsValue.textContent = `${patternsSlider.value}%`;
        debouncedUpdate();
    });
    activitiesSlider.addEventListener("input", () => {
        activitiesValue.textContent = `${activitiesSlider.value}%`;
        debouncedUpdate();
    });
    labelModeCountButton.addEventListener("click", () => {
        currentLabelMode = "count";
        syncProcessMapLabelModeControls();
    });
    labelModeDurationButton.addEventListener("click", () => {
        currentLabelMode = "duration";
        syncProcessMapLabelModeControls();
    });
    if (exportSvgButton) {
        exportSvgButton.addEventListener("click", () => {
            exportProcessMapSvg(
                `process_flow_map_${patternsSlider.value}_${activitiesSlider.value}.svg`
            );
        });
    }
    if (exportPngButton) {
        exportPngButton.addEventListener("click", () => {
            exportProcessMapPng(
                `process_flow_map_${patternsSlider.value}_${activitiesSlider.value}.png`
            );
        });
    }

    caseTraceForm.addEventListener("submit", async (event) => {
        event.preventDefault();
        await searchCaseTrace(caseTraceInput.value);
    });

    detailFilterForm.addEventListener("submit", (event) => {
        event.preventDefault();
        void applyDetailFilters();
    });

    detailFilterResetButton.addEventListener("click", () => {
        void resetDetailFilters();
    });

    variantSearchInput.addEventListener("input", () => {
        variantViewState.searchTerm = String(variantSearchInput.value || "").trim();
        variantViewState.page = 1;
        syncVariantPanel();
    });

    variantSortSelect.addEventListener("change", () => {
        variantViewState.sortKey = variantSortSelect.value || "count";
        variantViewState.page = 1;
        syncVariantPanel();
    });

    variantDisplayLimitSelect.addEventListener("change", () => {
        variantViewState.displayLimit = variantDisplayLimitSelect.value || "10";
        variantViewState.page = 1;
        syncVariantPanel();
    });

    variantResetButton.addEventListener("click", () => {
        void applyVariantSelection(null);
    });

    filterDefinitions = buildDefaultFilterDefinitions(currentDetailColumnSettings);
    draftDetailFilters = cloneDetailFilters(activeDetailFilters);
    try {
        filterOptionsPayload = await loadDetailFilterOptions(runId);
    } catch {
        filterOptionsPayload = null;
    }
    filterDefinitions = normalizeFilterDefinitions(
        filterOptionsPayload?.options?.filters || [],
        filterOptionsPayload?.column_settings || currentDetailColumnSettings
    );
    syncDetailFilterControls();
    renderFilterSummary();

    await Promise.all([
        refreshVariantSummary(),
        refreshBottleneckSummary(),
    ]);

    syncVariantPanel();
    syncBottleneckPanel();
    syncImpactPanel();
    syncProcessMapLabelModeControls();
    renderCaseTracePanel();
    if (selectedTransitionKey) {
        await loadSelectedTransitionCases();
    }
    await updateProcessMap();
}

function attachProcessMapInteractions(viewportElement) {
    if (previousProcessMapMouseMoveHandler) {
        window.removeEventListener("mousemove", previousProcessMapMouseMoveHandler);
        previousProcessMapMouseMoveHandler = null;
    }
    if (previousProcessMapMouseUpHandler) {
        window.removeEventListener("mouseup", previousProcessMapMouseUpHandler);
        previousProcessMapMouseUpHandler = null;
    }

    const svgElement = viewportElement.querySelector("svg.process-map-svg");
    const zoomIndicator = viewportElement.querySelector(".process-map-zoom-indicator");
    if (!svgElement) return;

    let isDragging = false;
    let startPanX = 0;
    let startPanY = 0;
    
    // Check if we already have a transform applied
    let currentScale = 1;
    let currentPanX = 0;
    let currentPanY = 0;

    svgElement.style.cursor = "grab";

    svgElement.addEventListener("mousedown", (e) => {
        isDragging = true;
        svgElement.style.cursor = "grabbing";
        startPanX = e.clientX - currentPanX;
        startPanY = e.clientY - currentPanY;
        e.preventDefault();
    });

    const mouseMoveHandler = (e) => {
        if (!isDragging) return;
        currentPanX = e.clientX - startPanX;
        currentPanY = e.clientY - startPanY;
        applyTransform();
    };

    const mouseUpHandler = () => {
        isDragging = false;
        if (svgElement) svgElement.style.cursor = "grab";
    };

    window.addEventListener("mousemove", mouseMoveHandler);
    window.addEventListener("mouseup", mouseUpHandler);
    previousProcessMapMouseMoveHandler = mouseMoveHandler;
    previousProcessMapMouseUpHandler = mouseUpHandler;

    svgElement.addEventListener("wheel", (e) => {
        e.preventDefault();
        
        const zoomIntensity = 0.1;
        const delta = e.deltaY > 0 ? -zoomIntensity : zoomIntensity;
        
        // Calculate new scale
        let newScale = currentScale * (1 + delta);
        newScale = Math.max(0.1, Math.min(newScale, 5)); // Limit zoom
        
        // Zoom towards cursor
        const rect = svgElement.getBoundingClientRect();
        const cursorX = e.clientX - rect.left;
        const cursorY = e.clientY - rect.top;
        
        currentPanX = cursorX - (cursorX - currentPanX) * (newScale / currentScale);
        currentPanY = cursorY - (cursorY - currentPanY) * (newScale / currentScale);
        
        currentScale = newScale;
        applyTransform();
    }, { passive: false });
    
    // Set up click-to-focus highlighting
    svgElement.addEventListener("click", (e) => {
        // If they were just dragging, ignore the click
        if (isDragging) return;

        const nodeGroup = e.target.closest(".process-map-node-group");
        
        if (!nodeGroup) {
            // Clicked background, clear focus
            svgElement.classList.remove("is-focusing");
            svgElement.querySelectorAll(".is-focused").forEach(el => el.classList.remove("is-focused"));
            return;
        }

        const focusedNodeName = nodeGroup.getAttribute("data-node");
        if (!focusedNodeName) return;

        // Toggle focus off if clicking the already focused node
        if (nodeGroup.classList.contains("is-focused") && svgElement.classList.contains("is-focusing")) {
            svgElement.classList.remove("is-focusing");
            svgElement.querySelectorAll(".is-focused").forEach(el => el.classList.remove("is-focused"));
            return;
        }

        // Apply focusing state
        svgElement.classList.add("is-focusing");
        svgElement.querySelectorAll(".is-focused").forEach(el => el.classList.remove("is-focused"));

        // Focus the clicked node
        nodeGroup.classList.add("is-focused");

        // Find and focus connected edges and their other connected nodes
        const edges = svgElement.querySelectorAll(".process-map-edge, .process-map-edge-label");
        const connectedNodes = new Set();
        
        edges.forEach(edge => {
            const source = edge.getAttribute("data-source");
            const target = edge.getAttribute("data-target");

            if (source === focusedNodeName || target === focusedNodeName) {
                edge.classList.add("is-focused");
                connectedNodes.add(source);
                connectedNodes.add(target);
            }
        });

        // Focus the connected nodes
        const allNodes = svgElement.querySelectorAll(".process-map-node-group");
        allNodes.forEach(n => {
            if (connectedNodes.has(n.getAttribute("data-node"))) {
                n.classList.add("is-focused");
            }
        });
    });
    
    function applyTransform() {
        const gWrap = svgElement.querySelector(".viewport-wrap");
        if (!gWrap) return;

        // Use the SVG transform attribute instead of CSS transform.
        // CSS transforms on <g> can drop path/marker rendering at certain zoom levels.
        gWrap.setAttribute(
            "transform",
            `translate(${currentPanX} ${currentPanY}) scale(${currentScale})`
        );
        
        if (zoomIndicator) {
            zoomIndicator.textContent = Math.round(currentScale * 100) + "%";
        }
    }
    
    // Apply initial transform to wrap properly
    applyTransform();
}

function renderPatternChart(analysis, runId, impact = null) {
    const initialFlowSettings = getInitialPatternFlowSettings(analysis.row_count ?? analysis.rows.length);

    if (!analysis.rows.length) {
        chartPanel.className = "result-panel";
        chartTitle.textContent = "業務全体フロー図";
        chartNote.textContent = "表示できるデータがありません。";
        chartContainer.innerHTML = '<p class="empty-state">表示できるデータがありません。</p>';
        return;
    }

    chartPanel.className = "result-panel";
    chartTitle.textContent = "業務全体フロー図";
    chartNote.textContent = "大きなデータでは初期表示を自動で絞っています。ラベルは件数と平均所要時間を切り替えられ、パターン / バリアントを選ぶとそのルートのケースだけでフロー図を再描画します。";
    chartContainer.innerHTML = `
        <details class="detail-filter-panel">
            <summary>分析対象条件を絞り込む</summary>
            <form id="detail-light-filter-form" class="detail-filter-form">
                <label class="detail-filter-field">
                    <span>開始日</span>
                    <input id="detail-light-date-from" type="date">
                </label>
                <label class="detail-filter-field">
                    <span>終了日</span>
                    <input id="detail-light-date-to" type="date">
                </label>
                <label class="detail-filter-field">
                    <span>アクティビティ 条件</span>
                    <select id="detail-light-activity-mode">
                        <option value="include">含む</option>
                        <option value="exclude">除外</option>
                    </select>
                </label>
                <label class="detail-filter-field detail-filter-field--wide">
                    <span>アクティビティ 絞り込み</span>
                    <select id="detail-light-activity-values" multiple size="4"></select>
                </label>
                <div class="detail-filter-actions">
                    <button type="submit" class="detail-link process-explorer-button process-explorer-button--primary">適用</button>
                    <button id="detail-light-filter-reset" type="button" class="detail-link process-explorer-button">リセット</button>
                </div>
            </form>
            <div class="detail-filter-meta">
                <p id="detail-filter-summary" class="panel-note">分析対象条件未適用</p>
                <p id="detail-filter-counts" class="panel-note">対象ケース数 0 / 対象イベント数 0</p>
            </div>
            <div id="current-selection-state" class="selection-state-banner" data-selection-source="none">
                <span class="selection-state-label">現在の選択状態</span>
                <strong id="current-selection-state-title" class="selection-state-title">全体表示中</strong>
                <p id="current-selection-state-meta" class="selection-state-meta">パターン / バリアント一覧の全体表示で解除できます。</p>
            </div>
        </details>
        <div class="process-explorer-shell">
            <div class="process-explorer-map-panel">
                <div id="process-map-viewport" class="process-map-viewport"></div>
            </div>
            <aside class="process-explorer-sidebar">
                <div class="process-explorer-export">
                    <button id="process-map-export-svg" type="button" class="detail-link process-explorer-button">SVG保存</button>
                    <button id="process-map-export-png" type="button" class="detail-link process-explorer-button">PNG保存</button>
                </div>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>表示ルート</span>
                        <strong id="process-map-patterns-value">${initialFlowSettings.patterns}%</strong>
                    </div>
                    <div class="process-explorer-slider-wrap">
                        <span class="process-explorer-slider-top">100%</span>
                        <input
                            id="process-map-patterns-slider"
                            class="process-explorer-slider"
                            type="range"
                            min="10"
                            max="100"
                            step="10"
                            value="${initialFlowSettings.patterns}"
                        >
                        <span class="process-explorer-slider-bottom">10%</span>
                    </div>
                    <p id="process-map-patterns-meta" class="process-explorer-meta"></p>
                </section>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>表示アクティビティ</span>
                        <strong id="process-map-activities-value">${initialFlowSettings.activities}%</strong>
                    </div>
                    <div class="process-explorer-slider-wrap">
                        <span class="process-explorer-slider-top">100%</span>
                        <input
                            id="process-map-activities-slider"
                            class="process-explorer-slider"
                            type="range"
                            min="10"
                            max="100"
                            step="10"
                            value="${initialFlowSettings.activities}"
                        >
                        <span class="process-explorer-slider-bottom">10%</span>
                    </div>
                    <p id="process-map-activities-meta" class="process-explorer-meta"></p>
                </section>
                <section class="process-explorer-control">
                    <div class="process-explorer-control-head">
                        <span>ラベル</span>
                        <strong>切替</strong>
                    </div>
                    <div class="process-map-label-toggle" role="group" aria-label="フロー図ラベル表示">
                        <button id="process-map-label-mode-count" type="button" class="process-map-label-toggle-button is-active" aria-pressed="true">件数</button>
                        <button id="process-map-label-mode-duration" type="button" class="process-map-label-toggle-button" aria-pressed="false">平均所要時間</button>
                    </div>
                    <p id="process-map-label-mode-meta" class="process-explorer-meta">表示中: 件数</p>
                </section>
                <div id="process-rule-legend-container">${renderProcessRuleLegend()}</div>
                ${renderProcessHeatLegend()}
            </aside>
        </div>
        <section class="variant-panel">
            <div class="result-header variant-panel-header">
                <div>
                    <h3>パターン / バリアント一覧</h3>
                    <p class="result-meta">件数の多いルートを一覧で確認できます。行クリックでフロー図をそのルートに切り替え、詳細ボタンで個別ページへ移動できます。</p>
                </div>
                <button id="variant-reset-button" type="button" class="ghost-link process-explorer-button">全体表示</button>
            </div>
            <div class="variant-panel-summary">
                <article id="variant-coverage-meta" class="variant-coverage-card">
                    <span class="variant-coverage-label">上位カバー率</span>
                    <strong class="variant-coverage-value">計算中...</strong>
                </article>
                <article class="variant-selection-card">
                    <p id="variant-selection-title" class="variant-selection-title">全体表示中</p>
                    <p id="variant-selection-meta" class="panel-note">パターン / バリアントを選択すると、そのルートに属するケースだけでフロー図を再描画します。</p>
                    <p id="variant-selection-sequence" class="variant-selection-sequence">現在は全ケースを使ったフロー図を表示しています。</p>
                    <div id="variant-selection-diff"></div>
                </article>
            </div>
            <div class="variant-controls">
                <label class="field">
                    <span>検索</span>
                    <input id="variant-search-input" type="search" placeholder="パターン名 / アクティビティ名 で検索">
                </label>
                <label class="field">
                    <span>並び順</span>
                    <select id="variant-sort-select">
                        <option value="count">件数順</option>
                        <option value="ratio">比率順</option>
                        <option value="avg_case_duration_sec">平均処理時間順</option>
                        <option value="activity_count">アクティビティ数順</option>
                    </select>
                </label>
                <label class="field">
                    <span>表示件数</span>
                    <select id="variant-display-limit-select">
                        <option value="10">Top 10</option>
                        <option value="20">Top 20</option>
                        <option value="50">Top 50</option>
                        <option value="100">Top 100</option>
                        <option value="all">すべて</option>
                    </select>
                </label>
            </div>
            <p id="variant-results-meta" class="panel-note">読み込み中...</p>
            <div class="variant-list-table">
                <div class="variant-list-head" aria-hidden="true">
                    <span>順位</span>
                    <span>パターン / バリアント</span>
                    <span>繰り返し</span>
                    <span>件数</span>
                    <span>比率</span>
                    <span>平均処理時間</span>
                    <span>ルート</span>
                </div>
                <div id="variant-list" class="variant-list"></div>
            </div>
            <div id="variant-pagination" class="result-pagination variant-pagination hidden"></div>
        </section>
        ${buildImpactSectionHtml(impact)}
        <details id="case-trace-panel" class="result-panel">
            <summary>ケースID 検索 / ケース追跡</summary>
            <form id="case-trace-form" class="case-trace-form">
                <input
                    id="case-trace-input"
                    class="case-trace-input"
                    type="text"
                    name="case_id"
                    placeholder="ケースID を入力"
                    autocomplete="off"
                    spellcheck="false"
                >
                <button type="submit" class="detail-link process-explorer-button process-explorer-button--primary">検索</button>
            </form>
            <div id="case-trace-result" class="case-trace-result">
                <p class="empty-state">ケースID を入力すると、ケースの通過順序と各工程の所要時間を表示します。</p>
            </div>
        </details>
    `;
    return initializePatternFlowExplorer(runId, impact);
}

async function renderChart(analysis, runId, detailData = null) {
    chartPanel.className = "result-panel hidden";
    chartContainer.innerHTML = "";

    if (analysisKey === "frequency") {
        renderFrequencyChart(analysis);
        return;
    }

    if (analysisKey === "transition") {
        renderTransitionChart(analysis);
        return;
    }

    if (analysisKey === "pattern") {
        await renderPatternChart(analysis, runId, detailData?.impact || null);
    }
}

// -----------------------------------------------------------------------------
// Page bootstrapping
// -----------------------------------------------------------------------------

async function renderDetailPage() {
    const latestResult = loadLatestResult();
    const runId = getRunId(latestResult);
    let detailRequestVersion = 0;
    activeDetailFilters = cloneDetailFilters(DEFAULT_DETAIL_FILTERS);
    currentDetailColumnSettings = {};
    detailPageAnalysisLoader = null;
    pendingDetailSupplementSections = new Set();
    detailSupplementErrorMessage = "";
    currentDetailSummaryData = null;
    currentRenderedAnalysis = null;
    detailSupplementRequestVersion = 0;

    if (!analysisKey) {
        setStatus("分析キーを特定できませんでした。", "error");
        return;
    }

    if (!runId) {
        setStatus("分析結果が見つかりません。TOP 画面で分析を実行してから詳細ページを開いてください。", "error");
        return;
    }

    setStatus("詳細を読み込んでいます...", "info");

    try {
        const detailData = await loadAnalysisPage(runId, 0, activeDetailFilters, getInitialDetailLoadOptions());
        const analysis = detailData.analyses[analysisKey];

        if (!analysis) {
            throw new Error("指定した分析結果が見つかりません。");
        }

        activeDetailFilters = cloneDetailFilters(detailData.applied_filters || DEFAULT_DETAIL_FILTERS);
        currentDetailColumnSettings = detailData.column_settings || {};
        currentRenderedAnalysis = analysis;
        currentDetailSummaryData = {
            source_file_name: detailData.source_file_name,
            case_count: detailData.case_count,
            event_count: detailData.event_count,
            dashboard: detailData.dashboard,
            impact: detailData.impact,
            insights: detailData.insights,
            root_cause: detailData.root_cause,
            applied_filters: detailData.applied_filters,
            column_settings: detailData.column_settings,
        };
        pendingDetailSupplementSections = new Set(detailData.deferred_sections || []);
        detailSupplementErrorMessage = "";

        if (detailExportExcelButton) {
            detailExportExcelButton.onclick = () => {
                void downloadDetailExcelExport(runId, {
                    analysisKeyName: analysisKey,
                    filters: activeDetailFilters,
                });
            };
        }
        if (aiInsightsButton) {
            aiInsightsButton.onclick = () => {
                void syncAiInsightsPanel(runId, analysis.analysis_name, { forceRefresh: true });
            };
        }

        const renderAnalysisPage = async (rowOffset) => {
            const currentVersion = detailRequestVersion + 1;
            detailRequestVersion = currentVersion;
            setStatus("表を読み込んでいます...", "info");

            try {
                const pageData = await loadAnalysisPage(runId, rowOffset, activeDetailFilters, getInitialDetailLoadOptions());

                if (currentVersion !== detailRequestVersion) {
                    return;
                }

                const pageAnalysis = pageData.analyses[analysisKey];
                if (!pageAnalysis) {
                    throw new Error("指定した分析結果が見つかりません。");
                }

                currentRenderedAnalysis = pageAnalysis;
                mergeDetailSummaryData(pageData);
                renderSummary(currentDetailSummaryData || pageData, pageAnalysis);
                if (analysisKey === "pattern") {
                    resultPanel.className = "result-panel hidden";
                    resultPanel.innerHTML = "";
                } else {
                    renderResult(pageAnalysis, runId, renderAnalysisPage);
                }
                syncDetailExportPanel(pageAnalysis.analysis_name, {
                    filters: activeDetailFilters,
                });
                hideStatus();
                void syncAiInsightsPanel(runId, pageAnalysis.analysis_name);
            } catch (error) {
                if (currentVersion !== detailRequestVersion) {
                    return;
                }

                setStatus(error.message, "error");
            }
        };

        detailPageAnalysisLoader = renderAnalysisPage;
        detailPageTitle.textContent = analysis.analysis_name;
        detailPageCopy.textContent = "指定した分析実行の全件結果を表示しています。";
        renderSummary(currentDetailSummaryData, analysis);
        await renderChart(analysis, runId, detailData);
        if (analysisKey === "pattern") {
            resultPanel.className = "result-panel hidden";
            resultPanel.innerHTML = "";
        } else {
            renderResult(analysis, runId, renderAnalysisPage);
        }
        syncDetailExportPanel(analysis.analysis_name, {
            filters: activeDetailFilters,
        });
        hideStatus();
        void syncAiInsightsPanel(runId, analysis.analysis_name);
        void refreshDeferredDetailSections(runId);
    } catch (error) {
        summaryPanel.className = "summary-panel hidden";
        chartPanel.className = "result-panel hidden";
        resultPanel.className = "result-panel hidden";
        setStatus(error.message, "error");
    }
}

void renderDetailPage();
