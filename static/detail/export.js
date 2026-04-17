/**
 * Detail page module.
 * Depends on: window.ProcessMiningShared and earlier detail scripts.
 * Exposes: script-scoped functions used by static/detail.js entrypoint.
 */
window.ProcessMiningDetail = window.ProcessMiningDetail || {};

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
        downloadBlob(exportBlob, window.ProcessMiningShared.getDownloadFileName(response, fallbackFileName));
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
