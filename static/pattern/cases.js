/**
 * Pattern page module.
 * Depends on: window.ProcessMiningShared and earlier pattern scripts.
 * Exposes: script-scoped functions used by static/pattern_detail.js entrypoint.
 */
window.ProcessMiningPattern = window.ProcessMiningPattern || {};

function getVisibleRows(rows, currentPage, pageSize) {
    const safePage = Math.max(1, Number(currentPage) || 1);
    const startIndex = (safePage - 1) * pageSize;
    return rows.slice(startIndex, startIndex + pageSize);
}

function buildPaginationHtml(totalRows, currentPage, pageSize, targetName) {
    const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
    if (totalPages <= 1) {
        return "";
    }

    const start = (currentPage - 1) * pageSize + 1;
    const end = Math.min(totalRows, currentPage * pageSize);

    return `
        <div class="result-pagination">
            <p class="result-pagination-meta">${escapeHtml(formatNumber(start))} - ${escapeHtml(formatNumber(end))} / ${escapeHtml(formatNumber(totalRows))} 件</p>
            <div class="result-pagination-actions">
                <button
                    type="button"
                    class="btn btn-secondary btn-sm result-pagination-button"
                    data-pagination-target="${escapeHtml(targetName)}"
                    data-pagination-direction="prev"
                    ${currentPage <= 1 ? "disabled" : ""}
                >
                    前へ
                </button>
                <button
                    type="button"
                    class="btn btn-secondary btn-sm result-pagination-button"
                    data-pagination-target="${escapeHtml(targetName)}"
                    data-pagination-direction="next"
                    ${currentPage >= totalPages ? "disabled" : ""}
                >
                    次へ
                </button>
            </div>
        </div>
    `;
}

function renderRepresentativeCases(detail) {
    const rows = Array.isArray(detail.case_examples) ? detail.case_examples : [];
    if (!rows.length) {
        return '<p class="empty-state">代表ケースを表示できません。</p>';
    }

    const visibleRows = getVisibleRows(rows, caseExamplesPage, CASE_PAGE_SIZE);
    const tableRows = visibleRows.map((row) => ({
        "ケースID": row.case_id,
        "総処理時間(分)": formatNumber(row.case_total_duration_min),
        "開始日時": formatDateTime(row.start_time),
        "終了日時": formatDateTime(row.end_time),
    }));

    return `
        <details class="case-list-details" open>
            <summary>代表ケース一覧</summary>
            <div class="case-list-details-body">
                ${buildTable(tableRows)}
                ${buildPaginationHtml(rows.length, caseExamplesPage, CASE_PAGE_SIZE, "examples")}
            </div>
        </details>
    `;
}

function renderTransitionCases(detail) {
    if (!selectedTransitionKey) {
        return '<p class="empty-state">上の遷移を選択すると、対象ケース一覧を表示します。</p>';
    }

    if (drilldownErrorMessage) {
        return `<p class="empty-state">${escapeHtml(drilldownErrorMessage)}</p>`;
    }

    if (!drilldownRows.length) {
        return '<p class="empty-state">選択した遷移に該当するケースはありません。</p>';
    }

    const selectedMetric = findSelectedMetric(detail);
    const titleText = selectedMetric?.transition_label || "選択中の遷移ケース";
    const visibleRows = getVisibleRows(drilldownRows, transitionCasesPage, CASE_PAGE_SIZE);
    const tableRows = visibleRows.map((row) => ({
        "ケースID": row.case_id,
        "遷移所要時間": row.duration_text || `${formatDurationSeconds(row.duration_sec)} 秒`,
        "開始日時": formatDateTime(row.from_time),
        "終了日時": formatDateTime(row.to_time),
    }));

    return `
        <details class="case-list-details" open>
            <summary>${escapeHtml(titleText)}</summary>
            <div class="case-list-details-body">
                ${buildTable(tableRows)}
                ${buildPaginationHtml(drilldownRows.length, transitionCasesPage, CASE_PAGE_SIZE, "transition")}
            </div>
        </details>
    `;
}

function renderCasePanel(detail) {
    const hasTransitionSelection = Boolean(selectedTransitionKey);
    const activeTab = hasTransitionSelection ? caseTab : "examples";
    caseTab = activeTab;

    casePanel.className = "result-panel";
    casePanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>ケース一覧</h2>
                <p class="result-meta">代表ケースと、選択した遷移に属するケースを切り替えて確認できます。</p>
            </div>
        </div>
        <div class="pattern-case-tabs" role="tablist" aria-label="ケース一覧タブ">
            <button
                type="button"
                class="pattern-case-tab${activeTab === "examples" ? " is-active" : ""}"
                data-case-tab="examples"
                role="tab"
                aria-selected="${activeTab === "examples" ? "true" : "false"}"
            >
                代表ケース
            </button>
            <button
                type="button"
                class="pattern-case-tab${activeTab === "transition" ? " is-active" : ""}"
                data-case-tab="transition"
                role="tab"
                aria-selected="${activeTab === "transition" ? "true" : "false"}"
                ${hasTransitionSelection ? "" : "disabled"}
            >
                選択遷移ケース
            </button>
        </div>
        <div class="pattern-case-tab-panel">
            ${activeTab === "transition" ? renderTransitionCases(detail) : renderRepresentativeCases(detail)}
        </div>
    `;
}



function bindCasePanel(detail) {
    casePanel.querySelectorAll("[data-case-tab]").forEach((buttonElement) => {
        buttonElement.addEventListener("click", () => {
            const nextTab = buttonElement.dataset.caseTab || "examples";
            if (nextTab === "transition" && !selectedTransitionKey) {
                return;
            }
            caseTab = nextTab;
            renderCasePanel(detail);
            bindCasePanel(detail);
        });
    });

    casePanel.querySelectorAll("[data-pagination-target]").forEach((buttonElement) => {
        buttonElement.addEventListener("click", () => {
            const direction = buttonElement.dataset.paginationDirection;
            const target = buttonElement.dataset.paginationTarget;
            if (target === "examples") {
                caseExamplesPage += direction === "next" ? 1 : -1;
                caseExamplesPage = Math.max(1, caseExamplesPage);
            } else if (target === "transition") {
                transitionCasesPage += direction === "next" ? 1 : -1;
                transitionCasesPage = Math.max(1, transitionCasesPage);
            }
            renderCasePanel(detail);
            bindCasePanel(detail);
        });
    });
}

async function renderPatternDetailPage() {
    const latestResult = loadLatestResult();
    const runId = getRunId(latestResult);
    currentRunId = runId;

    if (!runId) {
        setStatus("分析結果が見つかりません。TOP 画面から分析を実行してから詳細ページを開いてください。", "error");
        return;
    }

    if (!Number.isInteger(patternIndex)) {
        setStatus("パターン番号が不正です。", "error");
        return;
    }

    setStatus("パターン詳細を読み込んでいます...", "info");

    try {
        const detail = await loadPatternDetail(runId);
        const patternRows = latestResult?.analyses?.pattern?.rows;
        const patternCount = Array.isArray(patternRows) ? patternRows.length : null;
        const patternSteps = getPatternSteps(detail);
        const shortTitle = `Pattern #${patternIndex + 1}`;
        const caseCountText = `${Number(detail.case_count || 0).toLocaleString("ja-JP")}件`;
        const ratioText = `${formatNumber(detail.case_ratio_pct)}%`;

        updatePatternNavLinks(runId, patternCount);

        document.title = `${shortTitle} | ProcessLens`;
        pageTitle.innerHTML = `${escapeHtml(shortTitle)} <span class="pattern-title-meta">${escapeHtml(caseCountText)} / ${escapeHtml(ratioText)}</span>`;
        pageCopy.textContent = "選択した処理順パターンに属するケースだけを表示し、工程ごとの所要時間とボトルネックを確認できます。";

        if (stepsFlow) {
            if (patternSteps.length) {
                stepsFlow.innerHTML = patternSteps.map((step, index) => {
                    const chipHtml = `<span class="pattern-step-chip"><span>${escapeHtml(step)}</span></span>`;
                    const arrowHtml = index < patternSteps.length - 1
                        ? '<span class="pattern-step-arrow" aria-hidden="true">→</span>'
                        : "";
                    return chipHtml + arrowHtml;
                }).join("");
                stepsFlow.classList.remove("hidden");
            } else {
                stepsFlow.classList.add("hidden");
                stepsFlow.innerHTML = "";
            }
        }

        selectedTransitionKey = detail.bottleneck_transition?.transition_key || "";
        drilldownRows = [];
        drilldownErrorMessage = "";
        caseExamplesPage = 1;
        transitionCasesPage = 1;
        caseTab = selectedTransitionKey ? "transition" : "examples";

        renderSummary(detail);
        renderBottleneckPanel(detail);
        renderStepPanel(detail);
        bindTransitionSelection(detail);
        await renderDrilldownPanel(detail);
        renderCasePanel(detail);
        bindCasePanel(detail);
        hideStatus();
    } catch (error) {
        setStatus(error.message, "error");
    }
}
