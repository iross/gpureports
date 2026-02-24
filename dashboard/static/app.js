// GPU State Dashboard - Canvas rendering + data fetching

const STATE_LABELS = {
    0: 'Idle, prioritized', 1: 'Idle, open capacity', 2: 'Busy, prioritized',
    3: 'Busy, open capacity', 4: 'Busy, backfill', 5: 'N/A'
};
const STATE_COLORS = ['#ff4444', '#ff8800', '#44ff44', '#00cc99', '#4488ff', '#cccccc'];

const ROW_HEIGHT = 7;
const COL_WIDTH = 8;

let data = null;          // Full API response
let filteredMachines = []; // After machine filter
let currentRows = [];      // Flattened GPU rows for rendering

// --- Data fetching ---

async function fetchData(params) {
    const loading = document.getElementById('loading');
    loading.classList.remove('hidden');

    const url = new URL('/api/heatmap', window.location.origin);
    if (params.start) url.searchParams.set('start', params.start);
    if (params.end) url.searchParams.set('end', params.end);
    if (params.bucket_minutes) url.searchParams.set('bucket_minutes', params.bucket_minutes);

    try {
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        data = await resp.json();
        applyFilter();
        render();
        updateStatus();
    } catch (err) {
        console.error('Fetch error:', err);
        document.getElementById('statusText').textContent = 'Error loading data';
    } finally {
        loading.classList.add('hidden');
    }
}

function updateStatus() {
    if (!data || !data.time_buckets.length) {
        document.getElementById('statusText').textContent = 'No data';
        return;
    }
    const nMachines = filteredMachines.length;
    const nGpus = filteredMachines.reduce((s, m) => s + m.gpus.length, 0);
    const range = data.time_buckets[0].replace('T', ' ') + ' \u2013 ' +
                  data.time_buckets[data.time_buckets.length - 1].replace('T', ' ');
    document.getElementById('statusText').textContent =
        `${nMachines} machines, ${nGpus} GPUs | ${range}`;
}

// --- Filtering ---

function applyFilter() {
    if (!data) return;
    const query = document.getElementById('machineFilter').value.toLowerCase();
    if (!query) {
        filteredMachines = data.machines;
    } else {
        filteredMachines = data.machines.filter(m => m.name.toLowerCase().includes(query));
    }
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
                isFirst: idx === 0
            });
        });
    });
    return rows;
}

// --- Rendering ---

function renderLabels() {
    const container = document.getElementById('labelsScroll');
    container.innerHTML = '';
    currentRows.forEach(row => {
        const div = document.createElement('div');
        div.className = 'label-row' + (row.isFirst ? ' machine-start' : '');
        div.style.height = ROW_HEIGHT + 'px';
        div.style.lineHeight = ROW_HEIGHT + 'px';
        if (row.isFirst) {
            div.textContent = row.machine.split('.')[0];
        } else {
            div.textContent = row.gpu_id.slice(0, 12);
        }
        container.appendChild(div);
    });
}

function renderTimeHeader() {
    if (!data) return;
    const canvas = document.getElementById('timeHeader');
    const buckets = data.time_buckets;
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
        const mins = parseInt(t.slice(14, 16));
        const hours = parseInt(t.slice(11, 13));
        if (mins === 0) {
            const x = i * COL_WIDTH + COL_WIDTH / 2;
            ctx.fillStyle = '#666';
            ctx.fillRect(i * COL_WIDTH, 22, 1, 8);
            ctx.fillStyle = '#aaa';
            // Show date on first bucket of each day
            const day = t.slice(5, 10);
            const label = hours === 0 ? day + ' 00:00' : hours.toString().padStart(2, '0') + ':00';
            ctx.fillText(label, x, 18);
        }
    });
}

function renderHeatmap() {
    if (!data) return;
    const canvas = document.getElementById('heatmap');
    const buckets = data.time_buckets.length;
    const totalWidth = buckets * COL_WIDTH;
    const totalHeight = currentRows.length * ROW_HEIGHT;
    const dpr = window.devicePixelRatio || 1;

    canvas.width = totalWidth * dpr;
    canvas.height = totalHeight * dpr;
    canvas.style.width = totalWidth + 'px';
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
    if (!data) return;
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
    const canvas = document.getElementById('heatmap');
    const tooltip = document.getElementById('tooltip');

    canvas.addEventListener('mousemove', (e) => {
        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const col = Math.floor(x / COL_WIDTH);
        const row = Math.floor(y / ROW_HEIGHT);

        if (row >= 0 && row < currentRows.length && col >= 0 && data && col < data.time_buckets.length) {
            const r = currentRows[row];
            const state = r.states[col];
            tooltip.innerHTML =
                `<div class="tt-machine">${r.machine}</div>` +
                `<div class="tt-gpu">${r.gpu_id}</div>` +
                `<div class="tt-device">${r.device_name}</div>` +
                `<div class="tt-time">${data.time_buckets[col].replace('T', ' ')}</div>` +
                `<div class="tt-state" style="color:${STATE_COLORS[state]}">${STATE_LABELS[state]}</div>`;
            tooltip.style.display = 'block';
            let tx = e.clientX + 14;
            let ty = e.clientY + 14;
            if (tx + 220 > window.innerWidth) tx = e.clientX - 220;
            if (ty + 120 > window.innerHeight) ty = e.clientY - 120;
            tooltip.style.left = tx + 'px';
            tooltip.style.top = ty + 'px';
        } else {
            tooltip.style.display = 'none';
        }
    });

    canvas.addEventListener('mouseleave', () => {
        tooltip.style.display = 'none';
    });
}

// --- Controls ---

function setActiveButton(id) {
    document.querySelectorAll('.controls button[data-range]').forEach(b => b.classList.remove('active'));
    const btn = document.getElementById(id);
    if (btn) btn.classList.add('active');
}

function bucketForRange(hours) {
    if (hours <= 6) return 5;
    if (hours <= 24) return 15;
    return 60;  // 7d
}

async function loadPresetRange(hours) {
    const bucket = bucketForRange(hours);
    // First, figure out what the server's latest data point is
    // by doing a minimal fetch if we don't already have data
    let endStr = null;
    if (data && data.time_buckets.length > 0) {
        endStr = data.time_buckets[data.time_buckets.length - 1];
    }
    if (!endStr) {
        // Quick fetch to discover time range
        await fetchData({ bucket_minutes: bucket });
        if (data && data.time_buckets.length > 0) {
            endStr = data.time_buckets[data.time_buckets.length - 1];
        } else {
            return;
        }
    }
    // Compute start
    const endDate = new Date(endStr);
    const startDate = new Date(endDate.getTime() - hours * 3600 * 1000);
    const fmt = d => d.toISOString().slice(0, 16);
    await fetchData({ start: fmt(startDate), end: fmt(endDate), bucket_minutes: bucket });
}

document.getElementById('btn6h').addEventListener('click', () => {
    setActiveButton('btn6h');
    loadPresetRange(6);
});
document.getElementById('btn24h').addEventListener('click', () => {
    setActiveButton('btn24h');
    loadPresetRange(24);
});
document.getElementById('btn7d').addEventListener('click', () => {
    setActiveButton('btn7d');
    loadPresetRange(168);
});

document.getElementById('btnCustom').addEventListener('click', () => {
    const startVal = document.getElementById('customStart').value;
    const endVal = document.getElementById('customEnd').value;
    if (!startVal || !endVal) return;
    document.querySelectorAll('.controls button[data-range]').forEach(b => b.classList.remove('active'));
    fetchData({ start: startVal, end: endVal, bucket_minutes: 15 });
});

document.getElementById('machineFilter').addEventListener('input', () => {
    if (!data) return;
    applyFilter();
    renderLabels();
    renderHeatmap();
    updateStatus();
});

// --- Initial load ---
fetchData({ bucket_minutes: 15 });
