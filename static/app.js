
// State
const state = {
    metric: "NET", // HR01, HR03, NET
    measure: "count",
    geoMode: "CH", // CH, KT
    selectedCanton: null, // "ZH" etc.
    rangeMonths: 12,
    compare: false,
    months: [],
    cantons: [],
    data: []
};

// Data Indices
let indexByKantonMetric = {}; // [kanton][hr] -> array of values aligned to months
let indexByCHMetric = {};     // [hr] -> array of values aligned to months

// Initialization
document.addEventListener("DOMContentLoaded", async () => {
    updateStatus("Loading data...");

    try {
        const version = await loadStatus();
        await loadDimensions(version);
        await loadData(version);

        initControls();
        processData();
        render();

        updateStatus("Ready");
    } catch (e) {
        console.error(e);
        updateStatus("Error loading data: " + e.message);
    }
});

function updateStatus(msg) {
    const el = document.getElementById("status-display");
    if (el) el.textContent = msg;
}

async function loadStatus() {
    try {
        const resp = await fetch("/api/status");
        if (resp.ok) {
            const data = await resp.json();
            if (data.data_updated_at) {
                const date = new Date(data.data_updated_at).toLocaleString();
                updateStatus(`Last updated: ${date}`);
            }
            return data.data_version;
        }
    } catch (e) {
        console.warn("Could not load status", e);
    }
    return null;
}

async function loadDimensions(version) {
    let url = "/static/data/dimensions.json";
    if (version) url += `?v=${version}`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error("Missing dimensions.json");
    const dims = await resp.json();

    state.months = dims.months;
    state.cantons = dims.cantons;

    // Default canton
    if (state.cantons.length > 0) {
        state.selectedCanton = state.cantons[0];
    }
}

async function loadData(version) {
    let url = "/static/data/shab_monthly.json";
    if (version) url += `?v=${version}`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error("Missing shab_monthly.json");
    state.data = await resp.json();
}

function initControls() {
    // Metrics
    const metricContainer = document.getElementById("metric-controls");
    ["NET", "HR01", "HR03"].forEach(m => {
        const btn = document.createElement("button");
        btn.textContent = m;
        if (m === state.metric) btn.classList.add("active");
        btn.onclick = () => {
            state.metric = m;
            updateActive(metricContainer, btn);
            render();
        };
        metricContainer.appendChild(btn);
    });

    // View Options (Compare)
    const compareToggle = document.getElementById("compare-toggle");
    if (compareToggle) {
        compareToggle.checked = state.compare;
        compareToggle.onclick = () => {
            state.compare = compareToggle.checked;
            render();
        };
    }

    // Geo
    const geoContainer = document.getElementById("geo-controls");
    geoContainer.querySelectorAll("button").forEach(btn => {
        btn.onclick = () => {
            state.geoMode = btn.dataset.val;
            updateActive(geoContainer, btn);
            toggleCantonSelector();
            render();
        };
    });

    // Canton Selector
    const sel = document.getElementById("canton-select");
    state.cantons.forEach(kt => {
        const opt = document.createElement("option");
        opt.value = kt;
        opt.textContent = kt;
        sel.appendChild(opt);
    });
    sel.value = state.selectedCanton;
    sel.onchange = () => {
        state.selectedCanton = sel.value;
        render();
    };

    // Range
    const rangeContainer = document.getElementById("range-controls");
    rangeContainer.querySelectorAll("button").forEach(btn => {
        btn.onclick = () => {
            const val = btn.dataset.val;
            state.rangeMonths = val === "all" ? 1000 : parseInt(val);
            updateActive(rangeContainer, btn);
            render();
        };
    });
}

function updateActive(container, activeBtn) {
    container.querySelectorAll("button").forEach(b => b.classList.remove("active"));
    activeBtn.classList.add("active");
}

function toggleCantonSelector() {
    const el = document.getElementById("canton-selector-group");
    el.style.display = state.geoMode === "KT" ? "block" : "none";
}

function processData() {
    // Build fast lookup indices
    // Initialize structure
    state.cantons.forEach(kt => {
        indexByKantonMetric[kt] = {};
        ["HR01", "HR03", "NET"].forEach(hr => {
            indexByKantonMetric[kt][hr] = new Array(state.months.length).fill(0);
        });
    });

    ["HR01", "HR03", "NET"].forEach(hr => {
        indexByCHMetric[hr] = new Array(state.months.length).fill(0);
    });

    // Map month string to index
    const monthMap = new Map();
    state.months.forEach((m, i) => monthMap.set(m, i));

    // Fill indices
    state.data.forEach(row => {
        const mIdx = monthMap.get(row.month);
        if (mIdx === undefined) return;

        if (row.geo === "CH") {
            if (indexByCHMetric[row.hr]) {
                indexByCHMetric[row.hr][mIdx] = row.count;
            }
        } else if (row.geo === "KT" && row.kanton) {
            if (indexByKantonMetric[row.kanton] && indexByKantonMetric[row.kanton][row.hr]) {
                indexByKantonMetric[row.kanton][row.hr][mIdx] = row.count;
            }
        }
    });
}

function getEffectiveRange() {
    const total = state.months.length;
    const count = Math.min(total, state.rangeMonths);
    const start = total - count;

    const slicedMonths = state.months.slice(start);
    const startIdx = start;

    return { slicedMonths, startIdx };
}

function render() {
    const { slicedMonths, startIdx } = getEffectiveRange();

    // Time Series gets FULL data (all months) to support slider
    renderTimeSeries(state.months);

    // Heatmap gets SLICED data (based on sidebar range)
    renderHeatmap(slicedMonths, startIdx);
}

function renderTimeSeries(allMonths) {
    const traces = [];
    const metricsToPlot = state.compare ? ["HR01", "HR03"] : [state.metric];

    // Default range for initial view (if uirevision allows)
    // Calculate last N months based on state.rangeMonths
    const total = allMonths.length;
    const count = Math.min(total, state.rangeMonths);
    const start = total - count;
    // We need dates for range. Assumes 'months' are 'YYYY-MM-DD'
    const rangeStart = allMonths[start];
    const rangeEnd = allMonths[total - 1];

    metricsToPlot.forEach(m => {
        let data;
        if (state.geoMode === "CH") {
            data = indexByCHMetric[m];
        } else {
            // KT mode
            const kt = state.selectedCanton;
            data = indexByKantonMetric[kt][m];
        }

        traces.push({
            x: allMonths,
            y: data,
            type: 'scatter',
            mode: 'lines+markers',
            name: m,
            line: { shape: 'spline', width: 3 },
            marker: { size: 6 },
            hovertemplate: "%{x|%b %Y}<br>" + m + ": %{y:,}<extra></extra>",
        });
    });

    const titleText = state.compare
        ? `HR01 vs HR03 (${state.geoMode === 'CH' ? 'Switzerland' : state.selectedCanton})`
        : `Monthly ${state.metric} (${state.geoMode === 'CH' ? 'Switzerland' : state.selectedCanton})`;

    const layout = {
        title: { text: titleText },
        margin: { t: 40, r: 20, l: 40, b: 40 },
        hovermode: 'x unified',
        xaxis: {
            automargin: true,
            rangeslider: { visible: true },
            rangeselector: {
                buttons: [
                    { count: 6, label: "6m", step: "month", stepmode: "backward" },
                    { count: 1, label: "1y", step: "year", stepmode: "backward" },
                    { count: 3, label: "3y", step: "year", stepmode: "backward" },
                    { step: "all", label: "All" },
                ],
            },
            // Set initial range if we want to respect sidebar state initially
            // But sidebar state also slices heatmap.
            // If we set 'range' here, it might override user pan unless we are careful.
            // Using a new uirevision for range changes might work, or just relying on plotly.
            // Let's rely on Plotly's default behavior, but maybe set the range if it's a fresh render?
            // Actually, if we use 'uirevision', Plotly remembers the zoom level.
            // But if the user *changed* the range in the sidebar, we probably want to update the view.
            // The sidebar range updates state.rangeMonths, which triggers render().
            // We can set 'range' here.
             range: [rangeStart, rangeEnd]
        },
        yaxis: {
            fixedrange: true
        },
        legend: { itemclick: "toggle", itemdoubleclick: "toggleothers", orientation: "h" },
        uirevision: 'ts' // preserve state like zoom?
        // Note: if we bind 'range' above, 'uirevision' might conflict or be ignored for axis.
        // If we want sidebar changes to update the chart view, we might need to NOT use uirevision for axis,
        // or update the revision ID.
        // For now, let's try leaving uirevision. If the range doesn't update when sidebar is clicked,
        // we might need to change uirevision logic.
    };

    // If the user explicitly changes the sidebar range, we want the chart to zoom to that range.
    // If we just re-render with the same uirevision, Plotly might keep the OLD zoom.
    // So we should probably update uirevision when rangeMonths changes.
    // We remove geoMode and selectedCanton from the key so zoom is preserved when switching views.
    layout.uirevision = `ts-${state.rangeMonths}`;

    const config = { responsive: true, displayModeBar: false };

    Plotly.react('tsChart', traces, layout, config);
}

function renderHeatmap(months, startIdx) {
    // Z matrix: rows = cantons, cols = months
    const z = [];
    let globalMin = Infinity;
    let globalMax = -Infinity;

    // Sort cantons by total volume in the visible range
    // Create a list of objects { kt, row, total }
    const rowsData = [];

    state.cantons.forEach(kt => {
        const row = indexByKantonMetric[kt][state.metric].slice(startIdx);
        const sum = row.reduce((a, b) => a + b, 0);

        // Update global min/max
        row.forEach(v => {
            if (v < globalMin) globalMin = v;
            if (v > globalMax) globalMax = v;
        });

        rowsData.push({ kt, row, sum });
    });

    // Sort descending by sum
    rowsData.sort((a, b) => b.sum - a.sum);

    const sortedCantons = rowsData.map(d => d.kt);
    const sortedZ = rowsData.map(d => d.row);

    // Handle case with no data
    if (globalMin === Infinity) { globalMin = 0; globalMax = 0; }

    const trace = {
        z: sortedZ,
        x: months,
        y: sortedCantons,
        type: 'heatmap',
        colorscale: state.metric === 'NET' ? 'RdBu' : 'Blues',
        zmin: globalMin,
        zmax: globalMax,
        hovertemplate: 'Canton: %{y}<br>Month: %{x}<br>Value: %{z}<extra></extra>'
    };

    if (state.metric === 'NET') {
        trace.zmid = 0;
        // RdBu: Red (low) to Blue (high).
        // We want positive NET (growth) -> Blue, negative NET (decline) -> Red.
        // However, Plotly's RdBu often maps Low->Red, High->Blue.
        // If the output shows High=Red, we need to reverse.
        // Based on verification, we need to reverse to get Blue for positive growth.
        trace.reversescale = true;
    }

    const layout = {
        title: { text: `Heatmap by Canton (${state.metric})` },
        margin: { t: 40, r: 20, l: 40, b: 40 },
        xaxis: { automargin: true, fixedrange: true },
        yaxis: { automargin: true, fixedrange: true, dtick: 1 },
        uirevision: `hm-${state.rangeMonths}-${state.metric}`
    };

    const config = { responsive: true, displayModeBar: false };

    Plotly.react('hmChart', [trace], layout, config).then(gd => {
        gd.on('plotly_click', (data) => {
            const pt = data.points[0];
            const clickedCanton = pt.y;

            // Switch to canton mode and select this canton
            state.geoMode = "KT";
            state.selectedCanton = clickedCanton;

            // Update UI
            document.getElementById("canton-select").value = clickedCanton;
            const geoContainer = document.getElementById("geo-controls");
            // Find KT button
            const ktBtn = Array.from(geoContainer.querySelectorAll("button")).find(b => b.dataset.val === "KT");
            updateActive(geoContainer, ktBtn);
            toggleCantonSelector();

            render();
        });
    });
}
