/**
 * Top page module.
 * Depends on: window.ProcessMiningShared and earlier top scripts.
 * Exposes: script-scoped functions used by static/app.js entrypoint.
 */
window.ProcessMiningTop = window.ProcessMiningTop || {};

function appendMappingSettings(formData) {
    const mappingState = getCurrentMappingState();
    Object.entries(mappingState).forEach(([fieldName, fieldValue]) => {
        if (fieldValue) {
            formData.set(fieldName, fieldValue);
        }
    });
}

function validateColumnSelections() {
    const caseIdColumn = String(caseIdColumnSelect?.value || "").trim();
    const activityColumn = String(activityColumnSelect?.value || "").trim();
    const timestampColumn = String(timestampColumnSelect?.value || "").trim();

    if (!caseIdColumn || !activityColumn || !timestampColumn) {
        return "ケースID列 / アクティビティ列 / タイムスタンプ列を選択してください。";
    }

    if (new Set([caseIdColumn, activityColumn, timestampColumn]).size !== 3) {
        return "ケースID列 / アクティビティ列 / タイムスタンプ列にはそれぞれ異なる列を選択してください。";
    }

    return "";
}

function validateFilterColumnSelections() {
    const selectedColumns = filterColumnRefs
        .map((filterRef) => String(filterRef.columnSelect?.value || "").trim())
        .filter(Boolean);

    if (selectedColumns.length !== new Set(selectedColumns).size) {
        return "グループ/カテゴリー フィルター①〜③ にはそれぞれ異なる列を選択してください。";
    }

    return "";
}

function validateAnalyzeForm() {
    if (!Array.isArray(currentProfilePayload?.headers) || !currentProfilePayload.headers.length) {
        return "ログ情報を読み込んでから、分析対象の CSV を選択してください。";
    }

    const fileSelectionError = validateFileSelectionState();
    if (fileSelectionError) {
        return fileSelectionError;
    }

    const columnError = validateColumnSelections();
    if (columnError) {
        return columnError;
    }

    const filterColumnError = validateFilterColumnSelections();
    if (filterColumnError) {
        return filterColumnError;
    }

    const selectedAnalysisCount = form.querySelectorAll('input[name="analysis_keys"]:checked').length;
    if (selectedAnalysisCount === 0) {
        return "少なくとも 1 つの分析を選択してください。";
    }

    return "";
}

async function downloadBulkExcelArchive(runId, buttonElement) {
    if (!runId) {
        return;
    }

    const originalText = buttonElement?.innerHTML || "Excel一括出力";
    if (buttonElement) {
        buttonElement.disabled = true;
        buttonElement.innerHTML = "生成中...";
    }

    try {
        const response = await fetch(`/api/runs/${encodeURIComponent(runId)}/excel-archive`);
        if (!response.ok) {
            let errorMessage = "Excel一括出力に失敗しました。";
            try {
                const payload = await response.json();
                errorMessage = payload?.error || payload?.detail || errorMessage;
            } catch {
                // Ignore JSON parsing errors for binary/error responses.
            }
            throw new Error(errorMessage);
        }

        const blob = await response.blob();
        const downloadUrl = URL.createObjectURL(blob);
        const anchor = document.createElement("a");
        anchor.href = downloadUrl;
        anchor.download = sharedUi.getDownloadFileName(response, "全分析レポート.zip");
        document.body.appendChild(anchor);
        anchor.click();
        document.body.removeChild(anchor);
        URL.revokeObjectURL(downloadUrl);
        setStatus("Excel一括出力を開始しました。", "success");
    } catch (error) {
        setStatus(error.message || "Excel一括出力に失敗しました。", "error");
    } finally {
        if (buttonElement) {
            buttonElement.disabled = false;
            buttonElement.innerHTML = originalText;
        }
    }
}

function buildAppliedFilterSummary(appliedFilters = {}, columnSettings = {}) {
    const filterDefinitions = Array.isArray(columnSettings?.filters) ? columnSettings.filters : [];
    const labelMap = new Map(filterDefinitions.map((definition) => [definition.slot, definition.label]));
    const appliedItems = [];

    if (appliedFilters?.date_from) {
        appliedItems.push(`開始日: ${appliedFilters.date_from}`);
    }
    if (appliedFilters?.date_to) {
        appliedItems.push(`終了日: ${appliedFilters.date_to}`);
    }

    appliedItems.push(...FILTER_SLOT_KEYS
        .filter((slot) => Boolean(appliedFilters?.[slot]))
        .map((slot) => `${labelMap.get(slot) || DEFAULT_FILTER_LABELS[slot]}: ${appliedFilters[slot]}`));

    if (appliedFilters?.start_activity_values) {
        appliedItems.push(`開始アクティビティ: ${appliedFilters.start_activity_values}`);
    }
    if (appliedFilters?.end_activity_values) {
        appliedItems.push(`終了アクティビティ: ${appliedFilters.end_activity_values}`);
    }

    return appliedItems.length ? appliedItems.join(" / ") : "フィルター未適用";
}

function initializeTopPage() {
    form.addEventListener("submit", async (event) => {
        event.preventDefault();

        const validationError = validateAnalyzeForm();
        if (validationError) {
            setStatus(validationError, "error");
            return;
        }

        isAnalyzing = true;
        syncSubmitState();
        renderFilterChipBar(loadLatestResult()?.applied_filters, loadLatestResult()?.column_settings);
        setStatus("分析を実行しています...", "info");
        summaryPanel.className = "summary-panel hidden";
        renderInsightPanel(null, "hidden");
        resultPanels.className = "result-stack";
        resultPanels.innerHTML = "";

        try {
            const formData = new FormData(form);
            appendActivityEndpointFilterSettings(formData);
            const response = await fetch("/api/analyze", {
                method: "POST",
                body: formData,
            });
            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || "分析に失敗しました。");
            }

            saveLatestResult(data);
            renderDashboard(data);
            saveTopPageState();
            setSetupSectionOpen(false);
            setStatus("分析が完了しました。", "success");
        } catch (error) {
            setStatus(error.message, "error");
        } finally {
            isAnalyzing = false;
            syncSubmitState();
            renderFilterChipBar(loadLatestResult()?.applied_filters, loadLatestResult()?.column_settings);
        }
    });

    setupToggleButton?.addEventListener("click", () => {
        setSetupSectionOpen(!setupSection?.classList.contains("is-open"));
    });

    setupSummaryOpenButton?.addEventListener("click", () => {
        setSetupSectionOpen(true);
        caseIdColumnSelect?.focus();
    });

    topOpenCsvButton?.addEventListener("click", () => {
        setSetupSectionOpen(true);
        csvFileInput?.click();
    });

    csvFileInput?.addEventListener("change", () => {
        requiresFileReselection = false;
        setSetupSectionOpen(true);
        void refreshLogProfile();
    });

    [caseIdColumnSelect, activityColumnSelect, timestampColumnSelect, ...filterColumnRefs.map((filterRef) => filterRef.columnSelect)]
        .forEach((element) => {
            element?.addEventListener("change", () => {
                hideStatus();
                updateSetupSummary(loadLatestResult(), currentDashboardSupplement);
                renderFilterChipBar(loadLatestResult()?.applied_filters, loadLatestResult()?.column_settings);
                saveTopPageState();
                void refreshLogProfile();
            });
        });

    filterColumnRefs.forEach((filterRef) => {
            filterRef.valueSelect?.addEventListener("change", () => {
                hideStatus();
                renderFilterChipBar(loadLatestResult()?.applied_filters, loadLatestResult()?.column_settings);
                saveTopPageState();
            });
        });

    [analysisDateFromInput, analysisDateToInput].forEach((element) => {
        element?.addEventListener("change", () => {
            hideStatus();
            renderFilterChipBar(loadLatestResult()?.applied_filters, loadLatestResult()?.column_settings);
            saveTopPageState();
        });
    });

    [startActivityValuesSelect, endActivityValuesSelect].forEach((element) => {
        element?.addEventListener("change", () => {
            hideStatus();
            renderFilterChipBar(loadLatestResult()?.applied_filters, loadLatestResult()?.column_settings);
            saveTopPageState();
        });
    });
    form.querySelectorAll('input[name="analysis_keys"]').forEach((element) => {
        element.addEventListener("change", () => {
            hideStatus();
            syncAnalysisSelectionChips();
            saveTopPageState();
        });
    });

    diagnosticsSampleLimitInput?.addEventListener("change", () => {
        hideStatus();
        saveTopPageState();
    });

    diagnosticsButton?.addEventListener("click", () => {
        void runLogDiagnostics();
    });

    diagnosticsExcelButton?.addEventListener("click", () => {
        void downloadLogDiagnosticsExcel();
    });

    resetFilterButton?.addEventListener("click", () => {
        if (analysisDateFromInput) analysisDateFromInput.value = "";
        if (analysisDateToInput) analysisDateToInput.value = "";
        filterColumnRefs.forEach((filterRef) => {
            if (filterRef.columnSelect) filterRef.columnSelect.value = "";
            replaceSelectOptions(filterRef.valueSelect, [], "", "すべて");
            if (filterRef.valueSelect) filterRef.valueSelect.disabled = true;
        });
        replaceMultiSelectOptions(startActivityValuesSelect, [], []);
        replaceMultiSelectOptions(endActivityValuesSelect, [], []);
        if (startActivityValuesSelect) startActivityValuesSelect.disabled = true;
        if (endActivityValuesSelect) endActivityValuesSelect.disabled = true;
        if (filterSelectionNote) {
            filterSelectionNote.textContent = "分析対象にしたい列を選ぶと、絞り込み候補を選択できます。";
        }
        hideStatus();
        renderFilterChipBar();
        saveTopPageState();
    });
    const latestResult = loadLatestResult();
    renderFilterChipBar(latestResult?.applied_filters, latestResult?.column_settings);
    if (latestResult) {
        renderDashboard(latestResult);
    }

    renderProfilePayload(currentProfilePayload);
    restoreTopPageState(restoredTopPageState);
    syncAnalysisSelectionChips();
    updateSetupSummary(latestResult, currentDashboardSupplement);
    setSetupSectionOpen(Boolean(requiresFileReselection));
    saveTopPageState();
    syncSubmitState();
    hideStatus();











}

window.ProcessMiningTop.initializeTopPage = initializeTopPage;
