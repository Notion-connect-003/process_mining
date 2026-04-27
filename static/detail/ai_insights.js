/**
 * Detail page module.
 * Depends on: window.ProcessMiningShared and earlier detail scripts.
 * Exposes: script-scoped functions used by static/detail.js entrypoint.
 */
window.ProcessMiningDetail = window.ProcessMiningDetail || {};

const TIMESTAMP_DURATION_NOTE = "処理時間は1列のタイムスタンプから算出するため、休憩・待機・営業時間外など、実際に作業していない時間を含む可能性があります。";

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
        aiInsightsNote.textContent = payload?.note
            ? `${payload.note} ${TIMESTAMP_DURATION_NOTE}`
            : `まだ生成していません。${TIMESTAMP_DURATION_NOTE}`;
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
    aiInsightsNote.textContent = payload.note
        ? `${payload.note} ${TIMESTAMP_DURATION_NOTE}`
        : `現在の分析条件に対応する解説です。${TIMESTAMP_DURATION_NOTE}`;
    aiInsightsOutput.textContent = payload.text || "";
    aiInsightsOutput.classList.toggle("hidden", !payload.text);
    aiInsightsButton.disabled = false;
    aiInsightsButton.textContent = payload.cached ? "分析コメントを再生成" : "分析コメントを更新";

    if (payload.mode === "rule_based" || payload.mode === "fallback") {
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
    aiInsightsNote.textContent = `生成中です。完了すると画面切替後も保持されます。${TIMESTAMP_DURATION_NOTE}`;
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
    const exportConditionText = selectionItems.length
        ? `現在の絞り込みと ${selectionItems.join(" / ")} の選択状態も反映して出力します。`
        : "現在の絞り込み条件を反映して出力します。";
    detailExportNote.textContent = `${exportConditionText}${TIMESTAMP_DURATION_NOTE}`;
    detailExportScope.innerHTML = chips
        .filter(Boolean)
        .map((item) => `<span class="detail-export-chip">${escapeHtml(item)}</span>`)
        .join("");
    detailExportExcelButton.textContent = `${resolvedAnalysisName}をExcel出力`;
}

// -----------------------------------------------------------------------------
// Filter state helpers
// -----------------------------------------------------------------------------

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
