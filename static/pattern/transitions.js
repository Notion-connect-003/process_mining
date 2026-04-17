/**
 * Pattern page module.
 * Depends on: window.ProcessMiningShared and earlier pattern scripts.
 * Exposes: script-scoped functions used by static/pattern_detail.js entrypoint.
 */
window.ProcessMiningPattern = window.ProcessMiningPattern || {};

function renderBottleneckPanel(detail) {
    const patternSteps = getPatternSteps(detail);
    const stepMetrics = getStepMetrics(detail);
    const maxAverage = Math.max(...stepMetrics.map((row) => Number(row.avg_duration_min) || 0), 1);
    const bottleneck = detail.bottleneck_transition;

    const calloutHtml = bottleneck
        ? `
            <div class="bottleneck-callout">
                <strong>最大ボトルネック: ${escapeHtml(bottleneck.transition_label)}</strong>
                <p class="panel-note">平均 ${escapeHtml(formatNumber(bottleneck.avg_duration_min))} 分 / 中央値 ${escapeHtml(formatNumber(bottleneck.median_duration_min))} 分 / 最大 ${escapeHtml(formatNumber(bottleneck.max_duration_min))} 分</p>
            </div>
        `
        : `
            <div class="bottleneck-callout">
                <strong>ボトルネック遷移は見つかりませんでした。</strong>
            </div>
        `;

    const sortedAvgList = [...stepMetrics]
        .map((r) => Number(r.avg_duration_min || 0))
        .sort((a, b) => b - a);
    const top3Threshold = sortedAvgList[2] ?? 0;

    const barsHtml = stepMetrics.map((row) => {
        const transitionKey = getTransitionKeyFromMetric(row);
        const isBottleneck = bottleneck && row.sequence_no === bottleneck.sequence_no;
        const avgVal = Number(row.avg_duration_min || 0);
        const isWarning = !isBottleneck && avgVal >= top3Threshold && avgVal > 0;
        const isSelected = transitionKey === selectedTransitionKey;
        const widthPercent = maxAverage > 0
            ? Math.max(8, (avgVal / maxAverage) * 100)
            : 0;
        const tooltipHtml = `<strong>${escapeHtml(row.transition_label)}</strong><br>ケース数: ${escapeHtml(formatNumber(row.case_count))}<br>平均: ${escapeHtml(formatNumber(row.avg_duration_min))} 分<br>中央値: ${escapeHtml(formatNumber(row.median_duration_min))} 分<br>最大: ${escapeHtml(formatNumber(row.max_duration_min))} 分<br>待機シェア: ${escapeHtml(formatNumber(row.wait_share_pct))}%`;

        const cardClass = [
            "bottleneck-bar-card",
            isBottleneck ? "bottleneck-bar-card--highlight" : "",
            isWarning ? "bottleneck-bar-card--warning" : "",
            isSelected ? "bottleneck-bar-card--selected" : "",
        ].filter(Boolean).join(" ");

        return `
            <button
                type="button"
                class="${cardClass}"
                data-transition-key="${escapeHtml(transitionKey)}"
                data-from-activity="${escapeHtml(row.activity)}"
                data-to-activity="${escapeHtml(row.next_activity)}"
                data-tooltip="${escapeHtml(tooltipHtml)}"
                aria-pressed="${isSelected ? "true" : "false"}"
            >
                <div class="bottleneck-bar-head">
                    <p class="bottleneck-bar-label">${escapeHtml(row.transition_label)}</p>
                    <span class="bottleneck-bar-value">${escapeHtml(formatNumber(row.avg_duration_min))} 分</span>
                </div>
                <div class="bottleneck-bar-track">
                    <div class="bottleneck-bar-fill" style="width: ${widthPercent}%"></div>
                </div>
                <p class="bottleneck-bar-meta">ケース ${escapeHtml(formatNumber(row.case_count))} / クリックで対象ケースを表示</p>
            </button>
        `;
    }).join("");

    bottleneckPanel.className = "result-panel";
    bottleneckPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>ボトルネック分析</h2>
                <p class="result-meta">平均所要時間の長い工程を上から並べています。詳細値はホバーで確認できます。</p>
            </div>
        </div>
        <p class="panel-note">${escapeHtml(compactPatternLabel(patternSteps))}</p>
        ${renderPatternSteps(patternSteps, bottleneck)}
        ${calloutHtml}
        <div class="bottleneck-bars">
            ${barsHtml || '<p class="empty-state">表示できる遷移はありません。</p>'}
        </div>
    `;

    bottleneckPanel.querySelectorAll("[data-tooltip]").forEach((el) => {
        el.addEventListener("mouseenter", (event) => {
            window.ProcessMiningShared.showTooltip(event, el.dataset.tooltip || "");
        });
        el.addEventListener("mouseleave", () => {
            window.ProcessMiningShared.hideTooltip();
        });
    });
}

function buildMiniKpi(label, value) {
    return `
        <article class="detail-mini-kpi">
            <span class="detail-mini-kpi-label">${escapeHtml(label)}</span>
            <strong>${escapeHtml(value)}</strong>
        </article>
    `;
}

async function renderDrilldownPanel(detail) {
    drilldownPanel.className = "result-panel";

    if (!selectedTransitionKey) {
        drilldownPanel.innerHTML = `
            <div class="result-header">
                <div>
                    <h2>選択中の遷移</h2>
                    <p class="result-meta">上のボトルネックバーまたは工程表から遷移を選択してください。</p>
                </div>
            </div>
            <p class="empty-state">遷移を選ぶと、所要時間の要約とケース一覧を表示します。</p>
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
                <h2>選択中の遷移</h2>
                <p class="result-meta">${escapeHtml(transitionLabel)}</p>
            </div>
        </div>
        <p class="panel-note">ケース詳細を読み込み中です。</p>
    `;

    if (selectedMetric && !drilldownRows.length && !drilldownErrorMessage) {
        try {
            const payload = await loadPatternTransitionCases(
                currentRunId,
                selectedMetric.activity,
                selectedMetric.next_activity,
                TRANSITION_CASE_FETCH_LIMIT,
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
                    <h2>選択中の遷移</h2>
                    <p class="result-meta">${escapeHtml(transitionLabel)}</p>
                </div>
            </div>
            <p class="empty-state">${escapeHtml(drilldownErrorMessage)}</p>
        `;
        return;
    }

    if (!selectedMetric) {
        drilldownPanel.innerHTML = `
            <div class="result-header">
                <div>
                    <h2>選択中の遷移</h2>
                    <p class="result-meta">${escapeHtml(transitionLabel)}</p>
                </div>
            </div>
            <p class="empty-state">選択した遷移の指標を表示できません。</p>
        `;
        return;
    }

    drilldownPanel.innerHTML = `
        <div class="result-header">
            <div>
                <h2>選択中の遷移</h2>
                <p class="result-meta">${escapeHtml(transitionLabel)}</p>
            </div>
        </div>
        <div class="detail-inline-kpis">
            ${buildMiniKpi("ケース数", `${formatNumber(selectedMetric.case_count)}件`)}
            ${buildMiniKpi("平均所要時間", `${formatNumber(selectedMetric.avg_duration_min)}分`)}
            ${buildMiniKpi("中央値", `${formatNumber(selectedMetric.median_duration_min)}分`)}
            ${buildMiniKpi("最大", `${formatNumber(selectedMetric.max_duration_min)}分`)}
            ${buildMiniKpi("待機シェア", `${formatNumber(selectedMetric.wait_share_pct)}%`)}
        </div>
        <p class="panel-note">ケース一覧は下のタブで確認できます。${drilldownRows.length ? `現在 ${formatNumber(drilldownRows.length)} 件を表示可能です。` : ""}</p>
    `;
}


async function selectTransition(detail, nextTransitionKey) {
    const resolvedTransitionKey = String(nextTransitionKey || "");
    selectedTransitionKey = selectedTransitionKey === resolvedTransitionKey ? "" : resolvedTransitionKey;
    drilldownRows = [];
    drilldownErrorMessage = "";
    transitionCasesPage = 1;
    caseTab = selectedTransitionKey ? "transition" : "examples";
    renderBottleneckPanel(detail);
    renderStepPanel(detail);
    bindTransitionSelection(detail);
    await renderDrilldownPanel(detail);
    renderCasePanel(detail);
    bindCasePanel(detail);
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
