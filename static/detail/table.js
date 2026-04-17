/**
 * Detail page module.
 * Depends on: window.ProcessMiningShared and earlier detail scripts.
 * Exposes: script-scoped functions used by static/detail.js entrypoint.
 */
window.ProcessMiningDetail = window.ProcessMiningDetail || {};

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

window.ProcessMiningDetail.renderDetailPage = renderDetailPage;
