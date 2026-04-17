/**
 * Pattern page module.
 * Depends on: window.ProcessMiningShared and earlier pattern scripts.
 * Exposes: script-scoped functions used by static/pattern_detail.js entrypoint.
 */
window.ProcessMiningPattern = window.ProcessMiningPattern || {};

const patternIndex = Number(document.body.dataset.patternIndex);
const statusPanel = document.getElementById("pattern-status-panel");
const summaryPanel = document.getElementById("pattern-summary-panel");
const bottleneckPanel = document.getElementById("pattern-bottleneck-panel");
const stepPanel = document.getElementById("pattern-step-panel");
const drilldownPanel = document.getElementById("pattern-drilldown-panel");
const casePanel = document.getElementById("pattern-case-panel");
const pageTitle = document.getElementById("pattern-page-title");
const pageCopy = document.getElementById("pattern-page-copy");
const stepsFlow = document.getElementById("pattern-steps-flow");
const prevLink = document.getElementById("pattern-prev-link");
const nextLink = document.getElementById("pattern-next-link");
const backLink = document.getElementById("pattern-back-link");

const DETAIL_FETCH_TIMEOUT_MS = 120000;
const TRANSITION_CASE_FETCH_LIMIT = 40;
const CASE_PAGE_SIZE = 8;

let selectedTransitionKey = "";
let currentRunId = "";
let drilldownRows = [];
let drilldownErrorMessage = "";
let caseTab = "examples";
let caseExamplesPage = 1;
let transitionCasesPage = 1;

const sharedUi = window.ProcessMiningShared;
const {
    buildTable,
    buildTransitionKey,
    escapeHtml,
    fetchJson,
    formatDateTime,
    formatDurationSeconds,
    formatNumber,
    getRunId,
    loadLatestResult,
} = sharedUi;
const setStatus = (message, type = "info") => sharedUi.setStatus(statusPanel, message, type);
const hideStatus = () => sharedUi.hideStatus(statusPanel);

function buildPatternListHref(runId = currentRunId) {
    return runId
        ? `/analysis/pattern?run_id=${encodeURIComponent(runId)}`
        : "/analysis/pattern";
}

function buildDashboardHref(runId = currentRunId) {
    return runId
        ? `/?run_id=${encodeURIComponent(runId)}`
        : "/";
}

function buildPatternDetailHref(targetPatternIndex, runId = currentRunId) {
    const base = `/analysis/patterns/${encodeURIComponent(String(targetPatternIndex))}`;
    return runId
        ? `${base}?run_id=${encodeURIComponent(runId)}`
        : base;
}

function buildPatternTransitionCasesApiUrl(runId, fromActivity, toActivity, limit = TRANSITION_CASE_FETCH_LIMIT) {
    const params = new URLSearchParams({
        from_activity: String(fromActivity || ""),
        to_activity: String(toActivity || ""),
        pattern_index: String(patternIndex),
        limit: String(Math.max(0, Number(limit) || 0)),
    });

    return `/api/runs/${encodeURIComponent(runId)}/transition-cases?${params.toString()}`;
}

function loadPatternTransitionCases(runId, fromActivity, toActivity, limit = TRANSITION_CASE_FETCH_LIMIT) {
    return fetchJson(
        buildPatternTransitionCasesApiUrl(runId, fromActivity, toActivity, limit),
        "遷移ケース詳細の取得に失敗しました。",
        DETAIL_FETCH_TIMEOUT_MS,
    );
}

function buildPatternDetailApiUrl(runId) {
    return `/api/runs/${encodeURIComponent(runId)}/patterns/${encodeURIComponent(String(patternIndex))}`;
}

function loadPatternDetail(runId) {
    return fetchJson(
        buildPatternDetailApiUrl(runId),
        "パターン詳細の取得に失敗しました。",
        DETAIL_FETCH_TIMEOUT_MS,
    );
}

function getStepMetrics(detail) {
    return Array.isArray(detail?.step_metrics) ? detail.step_metrics : [];
}

function getPatternSteps(detail) {
    if (Array.isArray(detail?.pattern_steps) && detail.pattern_steps.length) {
        return detail.pattern_steps.map((step) => String(step || "").trim()).filter(Boolean);
    }

    return String(detail?.pattern || "")
        .split("→")
        .map((step) => step.trim())
        .filter(Boolean);
}

function getTransitionKeyFromMetric(metric) {
    return metric?.transition_key || buildTransitionKey(metric?.activity || "", metric?.next_activity || "");
}

function findSelectedMetric(detail) {
    return getStepMetrics(detail).find((row) => getTransitionKeyFromMetric(row) === selectedTransitionKey) || null;
}

function compactPatternLabel(patternSteps) {
    if (!patternSteps.length) {
        return "ルート未設定";
    }

    if (patternSteps.length <= 4) {
        return patternSteps.join(" → ");
    }

    return `${patternSteps.slice(0, 3).join(" → ")} → ...（全${patternSteps.length}工程）`;
}

function buildSummaryCard(label, value, modifierClass = "") {
    return `
        <article class="summary-card${modifierClass ? ` ${modifierClass}` : ""}">
            <span class="summary-label">${escapeHtml(label)}</span>
            <strong>${escapeHtml(value)}</strong>
        </article>
    `;
}

function renderPatternSteps(patternSteps, bottleneckTransition = null) {
    if (!patternSteps.length) {
        return "";
    }

    const bnFrom = bottleneckTransition?.activity || "";
    const bnTo = bottleneckTransition?.next_activity || "";

    return `
        <div class="pattern-steps">
            ${patternSteps.map((step, index) => {
                const isBottleneckTo = bnTo && step === bnTo && index > 0 && patternSteps[index - 1] === bnFrom;
                const chipClass = isBottleneckTo ? "pattern-step-chip pattern-step-chip--bottleneck" : "pattern-step-chip";
                return `
                    <div class="${chipClass}">
                        <span class="pattern-step-index">${index + 1}</span>
                        <span>${escapeHtml(step)}</span>
                    </div>
                `;
            }).join("")}
        </div>
    `;
}

function buildStepMetricRowsHtml(stepMetrics) {
    if (!stepMetrics.length) {
        return '<p class="empty-state">表示できる遷移データがありません。</p>';
    }

    const maxAverage = Math.max(...stepMetrics.map((row) => Number(row.avg_duration_min) || 0), 0);
    const tableRowsHtml = stepMetrics.map((row) => {
        const transitionKey = getTransitionKeyFromMetric(row);
        const isSelected = transitionKey === selectedTransitionKey;
        const isMaxAverage = Number(row.avg_duration_min || 0) === maxAverage;
        const tooltip = [
            `遷移: ${row.transition_label}`,
            `ケース数: ${formatNumber(row.case_count)}`,
            `平均: ${formatNumber(row.avg_duration_min)} 分`,
            `中央値: ${formatNumber(row.median_duration_min)} 分`,
            `最小: ${formatNumber(row.min_duration_min)} 分`,
            `最大: ${formatNumber(row.max_duration_min)} 分`,
            `待機シェア: ${formatNumber(row.wait_share_pct)}%`,
        ].join("\n");

        return `
            <tr
                class="transition-step-row${isSelected ? " transition-step-row--selected" : ""}${isMaxAverage ? " transition-step-row--max" : ""}"
                data-transition-key="${escapeHtml(transitionKey)}"
                data-from-activity="${escapeHtml(row.activity)}"
                data-to-activity="${escapeHtml(row.next_activity)}"
                tabindex="0"
                aria-selected="${isSelected ? "true" : "false"}"
                title="${escapeHtml(tooltip)}"
            >
                <td>${escapeHtml(row.sequence_no)}</td>
                <td class="table-cell--wide">
                    <div class="cell-scroll-wrapper">
                        ${escapeHtml(row.transition_label)}
                        ${isMaxAverage ? '<span class="transition-step-badge">最大</span>' : ""}
                    </div>
                </td>
                <td class="num">${escapeHtml(formatNumber(row.case_count))}</td>
                <td class="num">${escapeHtml(formatNumber(row.avg_duration_min))}</td>
                <td class="num">${escapeHtml(formatNumber(row.median_duration_min))}</td>
                <td class="num">${escapeHtml(formatNumber(row.wait_share_pct))}%</td>
            </tr>
        `;
    }).join("");

    return `
        <div class="table-wrap">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>工程</th>
                        <th>遷移</th>
                        <th>ケース数</th>
                        <th>平均所要時間(分)</th>
                        <th>中央値(分)</th>
                        <th>待機シェア(%)</th>
                    </tr>
                </thead>
                <tbody>${tableRowsHtml}</tbody>
            </table>
        </div>
    `;
}

function updatePatternNavLinks(runId, patternCount) {
    const patternListHref = buildPatternListHref(runId);
    const dashboardHref = buildDashboardHref(runId);
    const previousIndex = patternIndex - 1;
    const nextIndex = patternIndex + 1;
    const hasPrevious = previousIndex >= 0;
    const hasNext = Number.isInteger(patternCount) ? nextIndex < patternCount : true;

    if (backLink) {
        backLink.href = patternListHref;
    }

    document.querySelectorAll('a[href="/analysis/pattern"]').forEach((linkElement) => {
        linkElement.href = patternListHref;
    });
    document.querySelectorAll('a[href="/"]').forEach((linkElement) => {
        linkElement.href = dashboardHref;
    });

    if (prevLink) {
        prevLink.href = hasPrevious ? buildPatternDetailHref(previousIndex, runId) : patternListHref;
        prevLink.classList.toggle("is-disabled", !hasPrevious);
        prevLink.setAttribute("aria-disabled", hasPrevious ? "false" : "true");
        prevLink.tabIndex = hasPrevious ? 0 : -1;
    }

    if (nextLink) {
        nextLink.href = hasNext ? buildPatternDetailHref(nextIndex, runId) : patternListHref;
        nextLink.classList.toggle("is-disabled", !hasNext);
        nextLink.setAttribute("aria-disabled", hasNext ? "false" : "true");
        nextLink.tabIndex = hasNext ? 0 : -1;
    }
}

function buildKpiCard(label, value, sub = "") {
    return `
        <article class="kpi-card">
            <div>
                <div class="kpi-label">${escapeHtml(label)}</div>
                <div class="kpi-value" style="font-size:20px">${escapeHtml(value)}</div>
                ${sub ? `<div class="kpi-sub">${escapeHtml(sub)}</div>` : ""}
            </div>
        </article>
    `;
}

function renderSummary(detail) {
    const patternSteps = getPatternSteps(detail);
    const bottleneckLabel = detail.bottleneck_transition?.transition_label || "該当なし";
    const repeatFlag = String(detail.repeat_flag || "").trim() || "なし";
    const fastestPatternFlag = String(detail.fastest_pattern_flag || "").trim() || "該当なし";
    const reviewFlag = String(detail.review_flag || "").trim();
    const commentText = String(detail.simple_comment || "").trim();

    const noteItems = [
        reviewFlag ? `確認区分: ${reviewFlag}` : "",
        commentText,
    ].filter(Boolean);

    summaryPanel.className = "summary-panel summary-panel--pattern-detail";
    summaryPanel.innerHTML = `
        <div class="grid-auto">
            ${buildKpiCard("パターン番号", `#${patternIndex + 1}`)}
            ${buildKpiCard("ケース数", `${formatNumber(detail.case_count)}件`, `全体の ${formatNumber(detail.case_ratio_pct)}%`)}
            ${buildKpiCard("平均処理時間", `${formatNumber(detail.avg_case_duration_min)}分`, `中央値 ${formatNumber(detail.median_case_duration_min)}分`)}
            ${buildKpiCard("最大ボトルネック", bottleneckLabel, detail.bottleneck_transition ? `Avg ${formatNumber(detail.bottleneck_transition.avg_duration_min)}分` : "")}
            ${buildKpiCard("代表ルート", compactPatternLabel(patternSteps))}
        </div>
        <details class="summary-details-extra" style="margin-top:12px">
            <summary style="cursor:pointer;color:var(--muted);font-size:0.85rem">詳細情報を表示</summary>
            <div class="summary-panel summary-panel--pattern-detail" style="margin-top:10px">
                ${buildSummaryCard("繰り返し", repeatFlag)}
                ${buildSummaryCard("繰り返し率区分 / 差分", `${String(detail.repeat_rate_band || "-")} / ${formatNumber(detail.avg_case_duration_diff_min || 0)}分`)}
                ${buildSummaryCard("改善優先度 / 全体影響度", `${formatNumber(detail.improvement_priority_score || 0)} / ${formatNumber(detail.overall_impact_pct || 0)}%`)}
                ${buildSummaryCard("最短処理", fastestPatternFlag)}
                ${noteItems.length ? `
                    <article class="summary-card summary-card--wide summary-note-card">
                        <span class="summary-label">コメント</span>
                        <strong>${escapeHtml(noteItems.join(" / "))}</strong>
                    </article>
                ` : ""}
            </div>
        </details>
    `;
}

function renderStepPanel(detail) {
    stepPanel.className = "result-panel";
    stepPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>工程別の所要時間</h2>
                <p class="result-meta">行をクリックすると、該当遷移だけのケースを下部タブで確認できます。</p>
            </div>
        </div>
        ${buildStepMetricRowsHtml(getStepMetrics(detail))}
    `;
}
