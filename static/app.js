
// State
const state = {
    metric: "NET", // HR01, HR03, NET
    measure: "count",
    geoMode: "CH", // CH, KT
    selectedCanton: null, // "ZH" etc.
    rangeMonths: 12,
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

    renderTimeSeries(slicedMonths, startIdx);
    renderHeatmap(slicedMonths, startIdx);
}

function renderTimeSeries(months, startIdx) {
    const traces = [];

    if (state.geoMode === "CH") {
        const data = indexByCHMetric[state.metric].slice(startIdx);
        traces.push({
            x: months,
            y: data,
            type: 'scatter',
            mode: 'lines+markers',
            name: `CH - ${state.metric}`,
            line: { shape: 'spline', width: 3 },
            marker: { size: 6 }
        });
    } else {
        // KT mode
        const kt = state.selectedCanton;
        const data = indexByKantonMetric[kt][state.metric].slice(startIdx);
        traces.push({
            x: months,
            y: data,
            type: 'scatter',
            mode: 'lines+markers',
            name: `${kt} - ${state.metric}`,
            line: { shape: 'spline', width: 3 },
            marker: { size: 6 }
        });
    }

    const layout = {
        title: { text: `Monthly ${state.metric} (${state.geoMode === 'CH' ? 'Switzerland' : state.selectedCanton})` },
        margin: { t: 40, r: 20, l: 40, b: 40 },
        hovermode: 'x unified',
        xaxis: {
            automargin: true,
            fixedrange: true
        },
        yaxis: {
            fixedrange: true
        },
        uirevision: 'ts' // preserve state
    };

    const config = { responsive: true, displayModeBar: false };

    Plotly.react('tsChart', traces, layout, config);
}

function renderHeatmap(months, startIdx) {
    // Z matrix: rows = cantons, cols = months
    const z = [];

    // Sort cantons? Alphabetical is fine for now
    // Maybe we want to sort by total volume? Let's stick to alphabetical.
    const cantons = state.cantons;

    cantons.forEach(kt => {
        const row = indexByKantonMetric[kt][state.metric].slice(startIdx);
        z.push(row);
    });

    const trace = {
        z: z,
        x: months,
        y: cantons,
        type: 'heatmap',
        colorscale: 'Blues',
        hovertemplate: 'Canton: %{y}<br>Month: %{x}<br>Value: %{z}<extra></extra>'
    };

    const layout = {
        title: { text: `Heatmap by Canton (${state.metric})` },
        margin: { t: 40, r: 20, l: 40, b: 40 },
        xaxis: { automargin: true, fixedrange: true },
        yaxis: { automargin: true, fixedrange: true, dtick: 1 },
        uirevision: 'hm'
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
