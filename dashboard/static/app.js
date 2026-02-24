// GPU State Dashboard — Canvas heatmap + Chart.js count charts

const STATE_LABELS = {
    0: 'Idle, prioritized', 1: 'Idle, open capacity', 2: 'Busy, prioritized',
    3: 'Busy, open capacity', 4: 'Busy, backfill', 5: 'N/A', 6: 'Idle, backfill'
};
const STATE_COLORS = ['#ff4444', '#ff8800', '#44ff44', '#00cc99', '#4488ff', '#cccccc', '#334499'];

// State codes belonging to each category (for the heatmap category filter)
const CATEGORY_CODES = {
    prioritized:  new Set([0, 2]),
    open_capacity: new Set([1, 3]),
    backfill:     new Set([4]),
};

// Colors for the Charts tab (RGB components for rgba())
const CHART_COLORS = {
    prioritized:  { r: 46,  g: 134, b: 171 },
    open_capacity: { r: 241, g: 143, b: 1   },
    backfill:     { r: 162, g: 59,  b: 114 },
};

const CHART_LABELS = {
    prioritized:  'Prioritized GPUs',
    open_capacity: 'Open Capacity GPUs',
    backfill:     'Backfill GPUs',
};

const ROW_HEIGHT = 7;
const COL_WIDTH = 8;
const REFRESH_INTERVAL_MS = 5 * 60 * 1000;

// --- State ---

let heatmapData = null;
let countsData = null;
let filteredMachines = [];
let currentRows = [];
let activeTab = 'heatmap';
let activeCategory = 'all';
let currentParams = { bucket_minutes: 15 };
let currentRange = { type: 'preset', hours: 24 };
let chartInstances = {};
let refreshTimer = null;
let countdownTimer = null;
let nextRefreshAt = null;

// --- Fetching ---

function buildUrl(path, params) {
    const url = new URL(path, window.location.origin);
    if (params.start)          url.searchParams.set('start', params.start);
    if (params.end)            url.searchParams.set('end', params.end);
    if (params.bucket_minutes) url.searchParams.set('bucket_minutes', params.bucket_minutes);
    return url;
}

async function fetchAll(params) {
    const loading = document.getElementById('loading');
    loading.classList.remove('hidden');
    currentParams = { ...params };

    try {
        const [hRes, cRes] = await Promise.all([
            fetch(buildUrl('/api/heatmap', params)),
            fetch(buildUrl('/api/counts', params)),
        ]);
        if (!hRes.ok) throw new Error(`heatmap HTTP ${hRes.status}`);
        if (!cRes.ok) throw new Error(`counts HTTP ${cRes.status}`);

        heatmapData = await hRes.json();
        countsData  = await cRes.json();

        applyFilter();
        render();
        updateStatus();
        renderCharts();
    } catch (err) {
        console.error('Fetch error:', err);
        document.getElementById('statusText').textContent = 'Error loading data';
    } finally {
        loading.classList.add('hidden');
    }
}

// --- Status bar ---

function updateStatus() {
    if (!heatmapData || !heatmapData.time_buckets.length) {
        document.getElementById('statusText').textContent = 'No data';
        return;
    }
    const nMachines = filteredMachines.length;
    const nGpus = filteredMachines.reduce((s, m) => s + m.gpus.length, 0);
    const range = heatmapData.time_buckets[0].replace('T', ' ') + ' \u2013 ' +
                  heatmapData.time_buckets[heatmapData.time_buckets.length - 1].replace('T', ' ');
    document.getElementById('statusText').textContent = `${nMachines} machines, ${nGpus} GPUs | ${range}`;
}

// --- Filtering ---

function applyFilter() {
    if (!heatmapData) return;
    const query = document.getElementById('machineFilter').value.toLowerCase();

    let machines = heatmapData.machines;

    if (query) {
        machines = machines.filter(m => m.name.toLowerCase().includes(query));
    }

    if (activeCategory !== 'all') {
        const codes = CATEGORY_CODES[activeCategory];
        machines = machines
            .map(m => ({ ...m, gpus: m.gpus.filter(gpu => gpu.states.some(s => codes.has(s))) }))
            .filter(m => m.gpus.length > 0);
    }

    filteredMachines = machines;
    currentRows = getGpuRows(filteredMachines);
}

function getGpuRows(machines) {
    const rows = [];
    machines.forEach(machine => {
        machine.gpus.forEach((gpu, idx) => {
            rows.push({
                machine: machine.name,
                gpu_id: gpu.gpu_id,
                device_name: gpu.device_name,
                states: gpu.states,
                isFirst: idx === 0,
            });
        });
    });
    return rows;
}

// --- Heatmap rendering ---

function renderLabels() {
    const container = document.getElementById('labelsScroll');
    container.innerHTML = '';
    filteredMachines.forEach(machine => {
        const groupHeight = machine.gpus.length * ROW_HEIGHT;
        const group = document.createElement('div');
        group.className = 'machine-group';
        group.style.height = groupHeight + 'px';

        const label = document.createElement('div');
        label.className = 'machine-name';
        label.textContent = machine.name.split('.')[0];
        group.appendChild(label);
        container.appendChild(group);
    });
}

function renderTimeHeader() {
    if (!heatmapData) return;
    const canvas = document.getElementById('timeHeader');
    const buckets = heatmapData.time_buckets;
    const totalWidth = buckets.length * COL_WIDTH;
    const dpr = window.devicePixelRatio || 1;

    canvas.width = totalWidth * dpr;
    canvas.height = 30 * dpr;
    canvas.style.width = totalWidth + 'px';
    canvas.style.height = '30px';

    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    ctx.fillStyle = '#16213e';
    ctx.fillRect(0, 0, totalWidth, 30);
    ctx.fillStyle = '#aaa';
    ctx.font = '10px -apple-system, sans-serif';
    ctx.textAlign = 'center';

    buckets.forEach((t, i) => {
        const mins  = parseInt(t.slice(14, 16));
        const hours = parseInt(t.slice(11, 13));
        if (mins === 0) {
            const x = i * COL_WIDTH + COL_WIDTH / 2;
            ctx.fillStyle = '#666';
            ctx.fillRect(i * COL_WIDTH, 22, 1, 8);
            ctx.fillStyle = '#aaa';
            const day   = t.slice(5, 10);
            const label = hours === 0 ? day + ' 00:00' : hours.toString().padStart(2, '0') + ':00';
            ctx.fillText(label, x, 18);
        }
    });
}

function renderHeatmap() {
    if (!heatmapData) return;
    const canvas = document.getElementById('heatmap');
    const buckets = heatmapData.time_buckets.length;
    const totalWidth  = buckets * COL_WIDTH;
    const totalHeight = currentRows.length * ROW_HEIGHT;
    const dpr = window.devicePixelRatio || 1;

    canvas.width  = totalWidth * dpr;
    canvas.height = totalHeight * dpr;
    canvas.style.width  = totalWidth + 'px';
    canvas.style.height = totalHeight + 'px';

    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);

    currentRows.forEach((row, ri) => {
        const y = ri * ROW_HEIGHT;
        row.states.forEach((state, ci) => {
            ctx.fillStyle = STATE_COLORS[state] || '#cccccc';
            ctx.fillRect(ci * COL_WIDTH, y, COL_WIDTH - 0.5, ROW_HEIGHT - 0.5);
        });
        if (row.isFirst && ri > 0) {
            ctx.fillStyle = '#555';
            ctx.fillRect(0, y - 0.5, totalWidth, 1);
        }
    });
}

function render() {
    if (!heatmapData) return;
    applyFilter();
    renderLabels();
    renderTimeHeader();
    renderHeatmap();
    setupTooltip();
    syncScroll();
}

// --- Scroll sync ---

let scrollSynced = false;
function syncScroll() {
    if (scrollSynced) return;
    scrollSynced = true;
    const canvasScroll = document.getElementById('canvasScroll');
    const labelsScroll = document.getElementById('labelsScroll');
    canvasScroll.addEventListener('scroll', () => {
        labelsScroll.scrollTop = canvasScroll.scrollTop;
    });
}

// --- Tooltip ---

let tooltipBound = false;
function setupTooltip() {
    if (tooltipBound) return;
    tooltipBound = true;
    const canvas  = document.getElementById('heatmap');
    const tooltip = document.getElementById('tooltip');

    canvas.addEventListener('mousemove', (e) => {
        const rect = canvas.getBoundingClientRect();
        const col  = Math.floor((e.clientX - rect.left)  / COL_WIDTH);
        const row  = Math.floor((e.clientY - rect.top)   / ROW_HEIGHT);

        if (row >= 0 && row < currentRows.length && col >= 0 && heatmapData && col < heatmapData.time_buckets.length) {
            const r     = currentRows[row];
            const state = r.states[col];
            tooltip.innerHTML =
                `<div class="tt-machine">${r.machine}</div>` +
                `<div class="tt-gpu">${r.gpu_id}</div>` +
                `<div class="tt-device">${r.device_name}</div>` +
                `<div class="tt-time">${heatmapData.time_buckets[col].replace('T', ' ')}</div>` +
                `<div class="tt-state" style="color:${STATE_COLORS[state]}">${STATE_LABELS[state]}</div>`;
            tooltip.style.display = 'block';
            let tx = e.clientX + 14;
            let ty = e.clientY + 14;
            if (tx + 220 > window.innerWidth)  tx = e.clientX - 220;
            if (ty + 120 > window.innerHeight) ty = e.clientY - 120;
            tooltip.style.left = tx + 'px';
            tooltip.style.top  = ty + 'px';
        } else {
            tooltip.style.display = 'none';
        }
    });

    canvas.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
}

// --- Charts tab ---

function renderCharts() {
    if (!countsData || !countsData.buckets.length) return;

    const labels = countsData.buckets.map(b => b.replace('T', ' '));

    for (const [cat, { r, g, b }] of Object.entries(CHART_COLORS)) {
        const canvasId = `chart-${cat.replace('_', '-')}`;
        const series   = countsData.series[cat];
        if (!series) continue;

        const claimed   = series.claimed;
        const unclaimed = series.total.map((t, i) => t - claimed[i]);

        if (chartInstances[cat]) {
            chartInstances[cat].destroy();
        }

        const ctx = document.getElementById(canvasId).getContext('2d');
        chartInstances[cat] = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Claimed',
                        data: claimed,
                        fill: true,
                        backgroundColor: `rgba(${r},${g},${b},0.75)`,
                        borderColor:     `rgba(${r},${g},${b},1)`,
                        borderWidth: 1,
                        tension: 0.1,
                        pointRadius: 0,
                        stack: 'gpus',
                    },
                    {
                        label: 'Unclaimed',
                        data: unclaimed,
                        fill: true,
                        backgroundColor: `rgba(${r},${g},${b},0.2)`,
                        borderColor:     `rgba(${r},${g},${b},0.35)`,
                        borderWidth: 1,
                        tension: 0.1,
                        pointRadius: 0,
                        stack: 'gpus',
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                scales: {
                    x: {
                        ticks: { color: '#888', maxTicksLimit: 12, maxRotation: 0 },
                        grid:  { color: '#2a2a4a' },
                    },
                    y: {
                        stacked: true,
                        min: 0,
                        ticks: { color: '#888' },
                        grid:  { color: '#2a2a4a' },
                        title: { display: true, text: 'GPUs', color: '#888' },
                    },
                },
                plugins: {
                    legend: { labels: { color: '#ccc', usePointStyle: true } },
                    title: {
                        display: true,
                        text:    CHART_LABELS[cat],
                        color:   '#e0e0e0',
                        font:    { size: 13 },
                    },
                },
            },
        });
    }
}

// --- Tab management ---

function switchTab(tab) {
    activeTab = tab;
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.toggle('hidden', c.id !== `tab-${tab}`));
    document.getElementById('legend').classList.toggle('hidden', tab !== 'heatmap');
    document.getElementById('heatmapControls').classList.toggle('hidden', tab !== 'heatmap');
    if (tab === 'charts') renderCharts();
}

// --- Auto-refresh ---

function scheduleRefresh() {
    if (refreshTimer)   clearInterval(refreshTimer);
    if (countdownTimer) clearInterval(countdownTimer);

    nextRefreshAt = Date.now() + REFRESH_INTERVAL_MS;

    refreshTimer = setInterval(async () => {
        // Force re-discovery of latest data on auto-refresh
        const savedHeatmap = heatmapData;
        heatmapData = null;
        await loadPresetRange(currentRange.type === 'preset' ? currentRange.hours : 24, false);
        if (!heatmapData) heatmapData = savedHeatmap;
        nextRefreshAt = Date.now() + REFRESH_INTERVAL_MS;
    }, REFRESH_INTERVAL_MS);

    countdownTimer = setInterval(updateRefreshText, 30000);
    updateRefreshText();
}

function updateRefreshText() {
    const el = document.getElementById('refreshText');
    if (!el || !nextRefreshAt) return;
    const mins = Math.max(0, Math.ceil((nextRefreshAt - Date.now()) / 60000));
    el.textContent = `Refresh in ${mins}m`;
}

// --- Range loading ---

function bucketForRange(hours) {
    if (hours <= 6)  return 5;
    if (hours <= 24) return 15;
    return 60;
}

function setActiveRangeButton(id) {
    document.querySelectorAll('.controls button[data-range]').forEach(b => b.classList.remove('active'));
    const btn = document.getElementById(id);
    if (btn) btn.classList.add('active');
}

async function loadPresetRange(hours, resetSchedule = true) {
    currentRange = { type: 'preset', hours };
    const bucket = bucketForRange(hours);

    // Use cached end time if available, otherwise do a discovery fetch first
    let endStr = null;
    if (heatmapData && heatmapData.time_buckets.length > 0) {
        endStr = heatmapData.time_buckets[heatmapData.time_buckets.length - 1];
    }

    if (!endStr) {
        await fetchAll({ bucket_minutes: bucket });
        if (heatmapData && heatmapData.time_buckets.length > 0) {
            endStr = heatmapData.time_buckets[heatmapData.time_buckets.length - 1];
        } else {
            return;
        }
    }

    const endDate   = new Date(endStr);
    const startDate = new Date(endDate.getTime() - hours * 3600 * 1000);
    const fmt = d => d.toISOString().slice(0, 16);
    await fetchAll({ start: fmt(startDate), end: fmt(endDate), bucket_minutes: bucket });

    if (resetSchedule) scheduleRefresh();
}

// --- Event listeners ---

document.getElementById('btn6h').addEventListener('click', () => {
    setActiveRangeButton('btn6h');
    loadPresetRange(6);
});
document.getElementById('btn24h').addEventListener('click', () => {
    setActiveRangeButton('btn24h');
    loadPresetRange(24);
});
document.getElementById('btn7d').addEventListener('click', () => {
    setActiveRangeButton('btn7d');
    loadPresetRange(168);
});

document.getElementById('btnCustom').addEventListener('click', () => {
    const startVal = document.getElementById('customStart').value;
    const endVal   = document.getElementById('customEnd').value;
    if (!startVal || !endVal) return;
    currentRange = { type: 'custom' };
    document.querySelectorAll('.controls button[data-range]').forEach(b => b.classList.remove('active'));
    fetchAll({ start: startVal, end: endVal, bucket_minutes: 15 });
    scheduleRefresh();
});

document.getElementById('machineFilter').addEventListener('input', () => {
    if (!heatmapData) return;
    applyFilter();
    renderLabels();
    renderHeatmap();
    updateStatus();
});

document.querySelectorAll('.cat-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.cat-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        activeCategory = btn.dataset.cat;
        applyFilter();
        renderLabels();
        renderHeatmap();
        updateStatus();
    });
});

document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
});

// --- Initial load ---
loadPresetRange(24);
