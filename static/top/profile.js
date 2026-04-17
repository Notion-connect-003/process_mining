/**
 * Top page module.
 * Depends on: window.ProcessMiningShared and earlier top scripts.
 * Exposes: script-scoped functions used by static/app.js entrypoint.
 */
window.ProcessMiningTop = window.ProcessMiningTop || {};

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

function getCurrentActivityEndpointFilterState() {
    return {
        start_activity_values: sharedUi.readMultiSelectValues(startActivityValuesSelect),
        end_activity_values: sharedUi.readMultiSelectValues(endActivityValuesSelect),
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
    const latestRunId = isAnalyzing ? "" : String(loadLatestResult()?.run_id || "").trim();
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
            ${latestRunId ? '<button type="button" id="bulk-excel-btn" class="btn btn-primary dashboard-filter-export">Excel一括出力</button>' : ""}
        </div>
    `;

    filterChipBar.querySelector(".dashboard-filter-add")?.addEventListener("click", () => {
        setSetupSectionOpen(true);
        filterColumnRefs[1]?.columnSelect?.focus();
    });
    filterChipBar.querySelector("#bulk-excel-btn")?.addEventListener("click", (event) => {
        void downloadBulkExcelArchive(latestRunId, event.currentTarget);
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
