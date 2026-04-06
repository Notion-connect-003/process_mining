const patternIndex = Number(document.body.dataset.patternIndex);
const statusPanel = document.getElementById("pattern-status-panel");
const summaryPanel = document.getElementById("pattern-summary-panel");
const bottleneckPanel = document.getElementById("pattern-bottleneck-panel");
const stepPanel = document.getElementById("pattern-step-panel");
const drilldownPanel = document.getElementById("pattern-drilldown-panel");
const casePanel = document.getElementById("pattern-case-panel");
const pageTitle = document.getElementById("pattern-page-title");
const pageCopy = document.getElementById("pattern-page-copy");
let selectedTransitionKey = "";
let currentRunId = "";
let drilldownRows = [];
let drilldownErrorMessage = "";

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

function buildPatternTransitionCasesApiUrl(runId, fromActivity, toActivity, limit = 20) {
    const params = new URLSearchParams({
        from_activity: String(fromActivity || ""),
        to_activity: String(toActivity || ""),
        pattern_index: String(patternIndex),
        limit: String(Math.max(0, Number(limit) || 0)),
    });

    return `/api/runs/${encodeURIComponent(runId)}/transition-cases?${params.toString()}`;
}

function loadPatternTransitionCases(runId, fromActivity, toActivity, limit = 20) {
    return fetchJson(
        buildPatternTransitionCasesApiUrl(runId, fromActivity, toActivity, limit),
        "遷移ケース詳細の取得に失敗しました。",
    );
}

function buildPatternDetailApiUrl(runId) {
    return `/api/runs/${encodeURIComponent(runId)}/patterns/${encodeURIComponent(String(patternIndex))}`;
}

function loadPatternDetail(runId) {
    return fetchJson(
        buildPatternDetailApiUrl(runId),
        "処理順パターン詳細の取得に失敗しました。",
    );
}

function getStepMetrics(detail) {
    return Array.isArray(detail?.step_metrics) ? detail.step_metrics : [];
}

function getTransitionKeyFromMetric(metric) {
    return metric?.transition_key || buildTransitionKey(metric?.activity || "", metric?.next_activity || "");
}

function findSelectedMetric(detail) {
    return getStepMetrics(detail).find((row) => getTransitionKeyFromMetric(row) === selectedTransitionKey) || null;
}

function buildStepMetricRowsHtml(stepMetrics) {
    if (!stepMetrics.length) {
        return '<p class="empty-state">表示できる遷移データがありません。</p>';
    }

    const tableRowsHtml = stepMetrics.map((row) => {
        const transitionKey = getTransitionKeyFromMetric(row);
        const isSelected = transitionKey === selectedTransitionKey;

        return `
            <tr
                class="transition-step-row${isSelected ? " transition-step-row--selected" : ""}"
                data-transition-key="${escapeHtml(transitionKey)}"
                data-from-activity="${escapeHtml(row.activity)}"
                data-to-activity="${escapeHtml(row.next_activity)}"
                tabindex="0"
                aria-selected="${isSelected ? "true" : "false"}"
            >
                <td>${escapeHtml(row.sequence_no)}</td>
                <td class="table-cell--wide">
                    <div class="cell-scroll-wrapper">${escapeHtml(row.transition_label)}</div>
                </td>
                <td>${escapeHtml(row.case_count)}</td>
                <td>${escapeHtml(formatNumber(row.avg_duration_min))}</td>
                <td>${escapeHtml(formatNumber(row.median_duration_min))}</td>
                <td>${escapeHtml(formatNumber(row.min_duration_min))}</td>
                <td>${escapeHtml(formatNumber(row.max_duration_min))}</td>
                <td>${escapeHtml(formatNumber(row.wait_share_pct))}</td>
            </tr>
        `;
    }).join("");

    return `
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>順番</th>
                        <th>遷移</th>
                        <th>ケース数</th>
                        <th>平均所要時間(分)</th>
                        <th>中央値(分)</th>
                        <th>最小(分)</th>
                        <th>最大(分)</th>
                        <th>時間シェア(%)</th>
                    </tr>
                </thead>
                <tbody>${tableRowsHtml}</tbody>
            </table>
        </div>
    `;
}

async function selectTransition(detail, nextTransitionKey) {
    const resolvedTransitionKey = String(nextTransitionKey || "");
    selectedTransitionKey = selectedTransitionKey === resolvedTransitionKey ? "" : resolvedTransitionKey;
    drilldownRows = [];
    drilldownErrorMessage = "";
    renderBottleneckPanel(detail);
    renderStepPanel(detail);
    bindTransitionSelection(detail);
    await renderDrilldownPanel(detail);
}

function bindTransitionSelection(detail) {
    const bindSelectHandler = (element) => {
        element.addEventListener("click", async () => {
            await selectTransition(detail, element.dataset.transitionKey || "");
        });
        element.addEventListener("keydown", async (event) => {
            if (event.key !== "Enter" && event.key !== " ") {
                return;
            }
            event.preventDefault();
            await selectTransition(detail, element.dataset.transitionKey || "");
        });
    };

    bottleneckPanel.querySelectorAll("[data-transition-key]").forEach((buttonElement) => {
        bindSelectHandler(buttonElement);
    });

    stepPanel.querySelectorAll("[data-transition-key]").forEach((rowElement) => {
        bindSelectHandler(rowElement);
    });
}

function renderSummary(detail) {
    const bottleneckLabel = detail.bottleneck_transition
        ? detail.bottleneck_transition.transition_label
        : "該当なし";

    summaryPanel.className = "summary-panel";
    summaryPanel.innerHTML = `
        <article class="summary-card">
            <span class="summary-label">元ファイル</span>
            <strong>${escapeHtml(detail.source_file_name)}</strong>
        </article>
        <article class="summary-card">
            <span class="summary-label">ケース数 / 比率</span>
            <strong>${escapeHtml(detail.case_count)} / ${escapeHtml(formatNumber(detail.case_ratio_pct))}%</strong>
        </article>
        <article class="summary-card">
            <span class="summary-label">平均 / 中央ケース時間(分)</span>
            <strong>${escapeHtml(formatNumber(detail.avg_case_duration_min))} / ${escapeHtml(formatNumber(detail.median_case_duration_min))}</strong>
        </article>
        <article class="summary-card">
            <span class="summary-label">最大ボトルネック</span>
            <strong>${escapeHtml(bottleneckLabel)}</strong>
        </article>
    `;
}

function renderPatternSteps(patternSteps) {
    return `
        <div class="pattern-steps">
            ${patternSteps.map((step, index) => `
                <div class="pattern-step-chip">
                    <span class="pattern-step-index">${index + 1}</span>
                    <span>${escapeHtml(step)}</span>
                </div>
            `).join("")}
        </div>
    `;
}

function renderStepPanel(detail) {
    stepPanel.className = "result-panel";
    stepPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>遷移ごとの所要時間</h2>
                <p class="result-meta">同一パターン内で各遷移のケース統計を比較します。</p>
            </div>
        </div>
        ${buildStepMetricRowsHtml(getStepMetrics(detail))}
    `;
}

function renderCasePanel(detail) {
    const caseRows = detail.case_examples.map((row) => ({
        "ケースID": row.case_id,
        "ケース総時間(分)": formatNumber(row.case_total_duration_min),
        "開始時刻": formatDateTime(row.start_time),
        "終了時刻": formatDateTime(row.end_time),
    }));

    casePanel.className = "result-panel";
    casePanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>時間の長いケース</h2>
                <p class="result-meta">同じ処理順パターンでも時間がかかっているケースを上位から表示します。</p>
            </div>
        </div>
        ${buildTable(caseRows)}
    `;
}

function renderBottleneckPanel(detail) {
    const stepMetrics = getStepMetrics(detail);
    const maxAverage = Math.max(...stepMetrics.map((row) => Number(row.avg_duration_min) || 0), 1);
    const bottleneck = detail.bottleneck_transition;

    const calloutHtml = bottleneck
        ? `
            <div class="bottleneck-callout">
                <strong>最大ボトルネック: ${escapeHtml(bottleneck.transition_label)}</strong>
                <p class="panel-note">
                    平均 ${escapeHtml(formatNumber(bottleneck.avg_duration_min))} 分 /
                    中央 ${escapeHtml(formatNumber(bottleneck.median_duration_min))} 分 /
                    最大 ${escapeHtml(formatNumber(bottleneck.max_duration_min))} 分 /
                    シェア ${escapeHtml(formatNumber(bottleneck.wait_share_pct))}%
                </p>
            </div>
        `
        : `
            <div class="bottleneck-callout">
                <strong>ボトルネック遷移は見つかりませんでした。</strong>
            </div>
        `;

    const barsHtml = stepMetrics.map((row) => {
        const isBottleneck = bottleneck && row.sequence_no === bottleneck.sequence_no;
        const transitionKey = row.transition_key || buildTransitionKey(row.activity, row.next_activity);
        const isSelected = transitionKey === selectedTransitionKey;
        const widthPercent = maxAverage > 0
            ? Math.max(6, (Number(row.avg_duration_min) / maxAverage) * 100)
            : 0;

        return `
            <button
                type="button"
                class="bottleneck-bar-card${isBottleneck ? " bottleneck-bar-card--highlight" : ""}${isSelected ? " bottleneck-bar-card--selected" : ""}"
                data-transition-key="${escapeHtml(transitionKey)}"
                data-from-activity="${escapeHtml(row.activity)}"
                data-to-activity="${escapeHtml(row.next_activity)}"
                aria-pressed="${isSelected ? "true" : "false"}"
            >
                <div class="bottleneck-bar-head">
                    <p class="bottleneck-bar-label">${escapeHtml(row.transition_label)}</p>
                    <span class="bottleneck-bar-value">Avg ${escapeHtml(formatNumber(row.avg_duration_min))} min</span>
                </div>
                <div class="bottleneck-bar-track">
                    <div class="bottleneck-bar-fill" style="width: ${widthPercent}%"></div>
                </div>
                <p class="bottleneck-bar-meta">
                    Cases ${escapeHtml(row.case_count)} /
                    Median ${escapeHtml(formatNumber(row.median_duration_min))} min /
                    Max ${escapeHtml(formatNumber(row.max_duration_min))} min /
                    Share ${escapeHtml(formatNumber(row.wait_share_pct))}%
                </p>
            </button>
        `;
    }).join("");

    bottleneckPanel.className = "result-panel";
    bottleneckPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>ボトルネック分析</h2>
                <p class="result-meta">遷移バーをクリックすると、時間が長いケース例を下に表示します。</p>
            </div>
        </div>
        <p class="panel-note">${escapeHtml(detail.pattern)}</p>
        ${renderPatternSteps(detail.pattern_steps)}
        ${calloutHtml}
        <div class="bottleneck-bars">
            ${barsHtml}
        </div>
    `;
}

async function renderDrilldownPanel(detail) {
    drilldownPanel.className = "result-panel";

    if (!selectedTransitionKey) {
        drilldownPanel.innerHTML = `
            <div class="result-header">
                <div>
                    <h2>遷移ケース詳細</h2>
                    <p class="result-meta">遷移を選択すると、所要時間が長いケースを表示します。</p>
                </div>
            </div>
            <p class="empty-state">上の遷移バーまたは表を選択してください。</p>
        `;
        return;
    }

    const selectedMetric = findSelectedMetric(detail);
    const transitionLabel = selectedMetric
        ? selectedMetric.transition_label
        : selectedTransitionKey.replace("__TO__", " → ");

    drilldownPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>遷移ケース詳細</h2>
                <p class="result-meta">${escapeHtml(transitionLabel)}</p>
            </div>
        </div>
        <p class="panel-note">読み込み中...</p>
    `;

    if (selectedMetric && !drilldownRows.length && !drilldownErrorMessage) {
        try {
            const payload = await loadPatternTransitionCases(
                currentRunId,
                selectedMetric.activity,
                selectedMetric.next_activity,
                20,
            );
            drilldownRows = Array.isArray(payload.cases) ? payload.cases : [];
        } catch (error) {
            drilldownErrorMessage = error.message;
        }
    }

    if (drilldownErrorMessage) {
        drilldownPanel.innerHTML = `
            <div class="result-header">
                <div>
                    <h2>遷移ケース詳細</h2>
                    <p class="result-meta">${escapeHtml(transitionLabel)}</p>
                </div>
            </div>
            <p class="empty-state">${escapeHtml(drilldownErrorMessage)}</p>
        `;
        return;
    }

    if (!drilldownRows.length) {
        drilldownPanel.innerHTML = `
            <div class="result-header">
                <div>
                    <h2>遷移ケース詳細</h2>
                    <p class="result-meta">${escapeHtml(transitionLabel)}</p>
                </div>
            </div>
            <p class="panel-note">上位 20 件を対象に表示します。</p>
            <p class="empty-state">該当するケースはありません。</p>
        `;
        return;
    }

    const rows = drilldownRows.map((row) => ({
        "ケースID": row.case_id,
        "所要時間": row.duration_text,
        "所要時間(sec)": formatDurationSeconds(row.duration_sec),
        "開始時刻": formatDateTime(row.from_time),
        "終了時刻": formatDateTime(row.to_time),
    }));

    drilldownPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>遷移ケース詳細</h2>
                <p class="result-meta">${escapeHtml(transitionLabel)}</p>
            </div>
        </div>
        <p class="panel-note">${escapeHtml(drilldownRows.length)} 件を表示しています。所要時間の長い順です。</p>
        ${buildTable(rows)}
    `;
}

async function renderPatternDetailPage() {
    const latestResult = loadLatestResult();
    const runId = getRunId(latestResult);
    currentRunId = runId;

    if (!runId) {
        setStatus("分析結果が見つかりません。TOP 画面で分析を実行してから詳細ページを開いてください。", "error");
        return;
    }

    if (!Number.isInteger(patternIndex)) {
        setStatus("処理順パターン番号を取得できません。", "error");
        return;
    }

    setStatus("処理順パターンの詳細を読み込んでいます...", "info");

    try {
        const detail = await loadPatternDetail(runId);

        const patternLabel = detail.pattern || `処理順パターン ${patternIndex + 1}`;
        const patternMeta = [
            `Pattern #${patternIndex + 1}`,
            `${Number(detail.case_count || 0).toLocaleString("ja-JP")} cases`,
            `${formatNumber(detail.case_ratio_pct)}%`,
        ].join(" / ");

        document.title = `${patternLabel} | ProcessLens`;
        pageTitle.textContent = patternLabel;
        pageCopy.textContent = `${patternMeta}。各遷移の平均所要時間とケース例から、詰まりやすい箇所を確認できます。`;

        selectedTransitionKey = detail.bottleneck_transition?.transition_key || "";
        drilldownRows = [];
        drilldownErrorMessage = "";
        renderSummary(detail);
        renderBottleneckPanel(detail);
        renderStepPanel(detail);
        bindTransitionSelection(detail);
        await renderDrilldownPanel(detail);
        renderCasePanel(detail);
        hideStatus();
    } catch (error) {
        setStatus(error.message, "error");
    }
}

renderPatternDetailPage();
