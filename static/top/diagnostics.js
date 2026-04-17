/**
 * Top page module.
 * Depends on: window.ProcessMiningShared and earlier top scripts.
 * Exposes: script-scoped functions used by static/app.js entrypoint.
 */
window.ProcessMiningTop = window.ProcessMiningTop || {};

function buildMissingCountText(diagnostics) {
    const missingCounts = diagnostics?.missing_counts || {};
    const items = [
        `ケースID ${missingCounts.case_id ?? "-"}`,
        `アクティビティ ${missingCounts.activity ?? "-"}`,
        `タイムスタンプ ${missingCounts.timestamp ?? "-"}`,
    ];
    return items.join(" / ");
}

function buildLogPeriodText(diagnostics) {
    if (!diagnostics?.time_range?.min || !diagnostics?.time_range?.max) {
        return "ケースID / アクティビティ / タイムスタンプ列を選択すると表示します。";
    }
    return `${diagnostics.time_range.min} ～ ${diagnostics.time_range.max}`;
}

function buildDuplicateRateText(diagnostics) {
    const duplicateRate = Number(diagnostics?.duplicate_rate || 0);
    return `${(duplicateRate * 100).toFixed(1)}%`;
}

function renderDiagnostics(profilePayload) {
    if (!diagnosticsPanel) {
        return;
    }

    const diagnostics = profilePayload?.diagnostics;
    if (!diagnostics) {
        diagnosticsPanel.innerHTML = '<p class="empty-state">ログ診断を実行すると、件数・期間・欠損数・サンプル値を表示します。</p>';
        return;
    }

    const columns = Array.isArray(diagnostics.columns) ? diagnostics.columns : [];
    const columnRowsHtml = columns.map((column) => `
        <tr>
            <td>${escapeHtml(column.name)}</td>
            <td>${escapeHtml((column.sample_values || []).join(", ") || "-")}</td>
            <td>${escapeHtml(column.unique_count ?? "-")}</td>
            <td>${escapeHtml(column.missing_count ?? "-")}</td>
        </tr>
    `).join("");

    diagnosticsPanel.innerHTML = `
        <div class="diagnostics-summary-grid">
            <article class="diagnostic-card"><span class="summary-label">ログレコード数</span><strong>${escapeHtml(diagnostics.record_count ?? "-")}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">ケース数</span><strong>${escapeHtml(diagnostics.case_count ?? "-")}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">アクティビティ種類数</span><strong>${escapeHtml(diagnostics.activity_type_count ?? "-")}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">ログ期間</span><strong>${escapeHtml(buildLogPeriodText(diagnostics))}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">欠損数</span><strong>${escapeHtml(buildMissingCountText(diagnostics))}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">重複行数</span><strong>${escapeHtml(diagnostics.duplicate_row_count ?? 0)}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">重複あり/なし</span><strong>${escapeHtml(diagnostics.duplicate_status || "なし")}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">重複除外後レコード数</span><strong>${escapeHtml(diagnostics.deduplicated_record_count ?? "-")}</strong></article>
            <article class="diagnostic-card"><span class="summary-label">重複率</span><strong>${escapeHtml(buildDuplicateRateText(diagnostics))}</strong></article>
        </div>
        <p class="panel-note">ヘッダー一覧: ${escapeHtml((diagnostics.headers || []).join(", "))}</p>
        <div class="table-wrap diagnostics-table-wrap">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>列名</th>
                        <th>サンプル値</th>
                        <th>ユニーク件数</th>
                        <th>欠損件数</th>
                    </tr>
                </thead>
                <tbody>${columnRowsHtml}</tbody>
            </table>
        </div>
    `;
}


function getDiagnosticsSampleLimit() {
    const rawValue = String(
        diagnosticsSampleLimitInput?.value || DEFAULT_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT,
    ).trim();
    const numericValue = Number.parseInt(rawValue, 10);
    if (!Number.isFinite(numericValue) || numericValue <= 0) {
        return DEFAULT_LOG_DIAGNOSTIC_SAMPLE_ROW_LIMIT;
    }

    return numericValue;
}

function appendDiagnosticsSettings(formData) {
    appendMappingSettings(formData);
    formData.set("sample_row_limit", String(getDiagnosticsSampleLimit()));
}

function appendActivityEndpointFilterSettings(formData) {
    const activityEndpointFilterState = getCurrentActivityEndpointFilterState();
    ["start_activity_values", "end_activity_values"].forEach((fieldName) => {
        formData.delete(fieldName);
        const selectedValues = Array.isArray(activityEndpointFilterState[fieldName])
            ? activityEndpointFilterState[fieldName]
            : [];
        if (selectedValues.length) {
            formData.set(fieldName, selectedValues.join(","));
        }
    });
}

function triggerFileDownload(blob, fileName) {
    const downloadUrl = URL.createObjectURL(blob);
    const downloadLink = document.createElement("a");
    downloadLink.href = downloadUrl;
    downloadLink.download = fileName;
    document.body.appendChild(downloadLink);
    downloadLink.click();
    downloadLink.remove();
    URL.revokeObjectURL(downloadUrl);
}

async function fetchLogProfile(file) {
    const formData = new FormData();
    if (file) {
        formData.append("csv_file", file);
    }
    appendMappingSettings(formData);

    const response = await fetch("/api/csv-headers", {
        method: "POST",
        body: formData,
    });
    const payload = await response.json();

    if (!response.ok) {
        throw new Error(payload.error || "ヘッダー一覧の取得に失敗しました。");
    }

    return payload;
}

async function fetchLogDiagnostics(file) {
    const formData = new FormData();
    if (file) {
        formData.append("csv_file", file);
    }
    appendDiagnosticsSettings(formData);

    const response = await fetch("/api/log-diagnostics", {
        method: "POST",
        body: formData,
    });
    const payload = await response.json();

    if (!response.ok) {
        throw new Error(payload.error || "ログ診断の取得に失敗しました。");
    }

    return payload;
}

async function downloadLogDiagnosticsExcel() {
    const validationError = (
        validateFileSelectionState()
        || validateColumnSelections()
        || validateFilterColumnSelections()
    );
    if (validationError) {
        setStatus(validationError, "error");
        return;
    }

    const formData = new FormData();
    const selectedFile = csvFileInput?.files?.[0] || null;
    if (selectedFile) {
        formData.append("csv_file", selectedFile);
    }
    appendDiagnosticsSettings(formData);

    isDownloadingDiagnosticsExcel = true;
    syncSubmitState();
    setStatus("ログ診断Excelを出力しています...", "info");

    try {
        const response = await fetch("/api/log-diagnostics-excel", {
            method: "POST",
            body: formData,
        });

        if (!response.ok) {
            let errorMessage = "ログ診断Excelの出力に失敗しました。";
            try {
                const payload = await response.json();
                errorMessage = payload.error || errorMessage;
            } catch {
                // ignore
            }
            throw new Error(errorMessage);
        }

        const blob = await response.blob();
        const downloadFileName = getDownloadFileName(response, "log_diagnostics.xlsx");
        triggerFileDownload(blob, downloadFileName);
        setStatus("ログ診断Excelを出力しました。", "success");
        saveTopPageState();
    } catch (error) {
        setStatus(error.message, "error");
    } finally {
        isDownloadingDiagnosticsExcel = false;
        syncSubmitState();
    }
}

async function refreshLogProfile() {
    const selectedFile = csvFileInput?.files?.[0] || null;
    const fileSelectionError = validateFileSelectionState();
    if (fileSelectionError && !selectedFile) {
        setStatus(fileSelectionError, "error");
        return;
    }

    const currentVersion = profileRequestVersion + 1;
    profileRequestVersion = currentVersion;
    diagnosticRequestVersion += 1;
    isLoadingProfile = true;
    syncSubmitState();
    setStatus("ヘッダー候補を読み込んでいます...", "info");

    try {
        const payload = await fetchLogProfile(selectedFile);
        if (currentVersion !== profileRequestVersion) {
            return;
        }

        requiresFileReselection = false;
        renderProfilePayload(payload);
        saveTopPageState();
        setStatus("候補を更新しました。", "success");
    } catch (error) {
        if (currentVersion !== profileRequestVersion) {
            return;
        }
        setStatus(error.message, "error");
    } finally {
        if (currentVersion === profileRequestVersion) {
            isLoadingProfile = false;
            syncSubmitState();
        }
    }
}

function validateFileSelectionState() {
    if (requiresFileReselection && !csvFileInput?.files?.[0]) {
        return "前回アップロードしたファイル設定を再利用するには、CSV ファイルを選び直してください。";
    }

    return "";
}

async function runLogDiagnostics() {
    const validationError = (
        validateFileSelectionState()
        || validateColumnSelections()
        || validateFilterColumnSelections()
    );
    if (validationError) {
        setStatus(validationError, "error");
        return;
    }

    const selectedFile = csvFileInput?.files?.[0] || null;
    const currentVersion = diagnosticRequestVersion + 1;
    diagnosticRequestVersion = currentVersion;
    isRunningDiagnostics = true;
    if (diagnosticsSection) {
        diagnosticsSection.open = true;
    }
    syncSubmitState();
    diagnosticsPanel.innerHTML = '<p class="panel-note">読み込み中...</p>';
    setStatus("ログ診断を実行しています...", "info");

    try {
        const payload = await fetchLogDiagnostics(selectedFile);
        if (currentVersion !== diagnosticRequestVersion) {
            return;
        }

        requiresFileReselection = false;
        renderProfilePayload(payload);
        saveTopPageState();
        setStatus("ログ診断を表示しました。", "success");
    } catch (error) {
        if (currentVersion !== diagnosticRequestVersion) {
            return;
        }
        renderDiagnostics({ diagnostics: null });
        setStatus(error.message, "error");
    } finally {
        if (currentVersion === diagnosticRequestVersion) {
            isRunningDiagnostics = false;
            syncSubmitState();
        }
    }
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
