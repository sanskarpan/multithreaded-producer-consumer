// Producer-Consumer Real-Time Visualization

const INITIAL_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 30000;
const CHART_REDRAW_HZ = 10;

class PatternVisualizer {
    constructor() {
        this.ws = null;
        this.throughputChart = null;
        this.queueChart = null;
        this.isRunning = false;
        this.throughputData = [];
        this.queueData = [];
        this.maxDataPoints = 50;

        // Reconnect bookkeeping (exponential backoff, capped at 30s).
        this.reconnectAttempts = 0;
        this.reconnectTimer = null;
        this.shouldReconnect = true;
        this.redrawScheduled = false;
        this.pendingChartData = null;

        this.init();
    }

    init() {
        this.setupEventListeners();
        this.setupCharts();
        this.setupPatternInfo();
        this.connectWebSocket();
    }

    setupEventListeners() {
        document.getElementById('pattern-select').addEventListener('change', (e) => {
            this.updateConfigVisibility(e.target.value);
            this.updatePatternInfo(e.target.value);
        });

        ['buffer-size', 'worker-count', 'producer-rate', 'consumer-rate'].forEach(id => {
            const input = document.getElementById(id);
            if (input) {
                input.addEventListener('input', (e) => {
                    const display = document.getElementById(id + '-display');
                    if (display) display.textContent = e.target.value;
                });
            }
        });

        document.getElementById('item-count').addEventListener('input', (e) => {
            const display = document.getElementById('item-count-display');
            if (display) display.textContent = e.target.value;
        });

        document.getElementById('start-btn').addEventListener('click', () => this.start());
        document.getElementById('stop-btn').addEventListener('click', () => this.stop());

        this.updateConfigVisibility('buffered');
        this.updatePatternInfo('buffered');
    }

    updateConfigVisibility(pattern) {
        const bufferConfig = document.querySelector('.buffer-config');
        const workerConfig = document.querySelector('.worker-config');
        const rateConfig = document.querySelectorAll('.rate-config');

        bufferConfig.style.display = 'none';
        workerConfig.style.display = 'none';
        rateConfig.forEach(el => el.style.display = 'none');

        switch (pattern) {
            case 'buffered':
            case 'worker_pool':
            case 'fan_out_fan_in':
            case 'pipeline':
            case 'rate_limited':
                bufferConfig.style.display = 'block';
                break;
        }

        if (pattern === 'worker_pool' || pattern === 'fan_out_fan_in') {
            workerConfig.style.display = 'block';
        }

        if (pattern === 'rate_limited') {
            rateConfig.forEach(el => el.style.display = 'block');
        }
    }

    updatePatternInfo(pattern) {
        const info = {
            'basic': {
                title: 'Basic (Unbuffered Channel)',
                description: 'Direct hand-off between producers and consumers using unbuffered channels.',
                features: ['Zero buffer overhead', 'Guaranteed delivery', 'Tight coupling', 'Blocking sends/receives'],
                useCase: 'Simple synchronization where direct communication is acceptable.'
            },
            'buffered': {
                title: 'Buffered Channel',
                description: 'Decouples producers and consumers with a configurable buffer.',
                features: ['Reduces blocking', 'Smooths rate differences', 'Configurable buffer size', 'Memory overhead'],
                useCase: 'When producers and consumers have different processing rates.'
            },
            'worker_pool': {
                title: 'Worker Pool',
                description: 'Fixed number of workers process tasks from a shared queue.',
                features: ['Bounded concurrency', 'Automatic load balancing', 'Efficient resource usage', 'Worker statistics'],
                useCase: 'Need to limit concurrent processing with load balancing.'
            },
            'fan_out_fan_in': {
                title: 'Fan-Out/Fan-In',
                description: 'Multiple workers process items in parallel, results merged into single channel.',
                features: ['Parallel processing', 'Result aggregation', 'High throughput', 'Unordered results'],
                useCase: 'CPU-bound parallel processing with result collection.'
            },
            'pipeline': {
                title: 'Pipeline',
                description: 'Multi-stage processing where each stage transforms data.',
                features: ['Clear separation of concerns', 'Parallelism within stages', 'Flexible configuration', 'Multiple transformations'],
                useCase: 'Data needs multiple sequential transformations.'
            },
            'rate_limited': {
                title: 'Rate-Limited',
                description: 'Controls throughput with configurable rate limiting.',
                features: ['Precise rate control', 'Prevents overwhelming downstream', 'Configurable producer/consumer rates', 'Throughput management'],
                useCase: 'Need to comply with rate limits or control resource usage.'
            }
        };

        const patternInfo = info[pattern];
        if (!patternInfo) return;
        const infoDiv = document.getElementById('pattern-info');
        if (!infoDiv) return;
        // Build DOM safely without innerHTML for user-controllable text.
        infoDiv.replaceChildren();
        const h4 = document.createElement('h4');
        h4.style.color = '#667eea';
        h4.style.marginBottom = '10px';
        h4.textContent = patternInfo.title;
        infoDiv.appendChild(h4);

        const descP = document.createElement('p');
        descP.style.marginBottom = '15px';
        descP.textContent = patternInfo.description;
        infoDiv.appendChild(descP);

        const featP = document.createElement('p');
        featP.appendChild(document.createTextNode('Features:'));
        infoDiv.appendChild(featP);

        const ul = document.createElement('ul');
        ul.style.marginLeft = '20px';
        ul.style.marginBottom = '15px';
        patternInfo.features.forEach(f => {
            const li = document.createElement('li');
            li.textContent = f;
            ul.appendChild(li);
        });
        infoDiv.appendChild(ul);

        const useCaseP = document.createElement('p');
        const strong = document.createElement('strong');
        strong.textContent = 'Best For: ';
        useCaseP.appendChild(strong);
        useCaseP.appendChild(document.createTextNode(patternInfo.useCase));
        infoDiv.appendChild(useCaseP);
    }

    setupCharts() {
        const throughputCtx = document.getElementById('throughput-chart').getContext('2d');
        this.throughputChart = new Chart(throughputCtx, {
            type: 'line',
            data: { labels: [], datasets: [{
                label: 'Throughput (items/s)',
                data: [],
                borderColor: '#667eea',
                backgroundColor: 'rgba(102, 126, 234, 0.1)',
                tension: 0.4,
                fill: true
            }]},
            options: {
                responsive: true,
                maintainAspectRatio: true,
                animation: false,
                scales: {
                    y: { beginAtZero: true, title: { display: true, text: 'Items per Second' } },
                    x: { title: { display: true, text: 'Time (s)' } }
                }
            }
        });

        const queueCtx = document.getElementById('queue-chart').getContext('2d');
        this.queueChart = new Chart(queueCtx, {
            type: 'line',
            data: { labels: [], datasets: [
                { label: 'Queue Depth', data: [], borderColor: '#e74c3c', backgroundColor: 'rgba(231, 76, 60, 0.1)', tension: 0.4, yAxisID: 'y' },
                { label: 'Buffer Utilization (%)', data: [], borderColor: '#27ae60', backgroundColor: 'rgba(39, 174, 96, 0.1)', tension: 0.4, yAxisID: 'y1' }
            ]},
            options: {
                responsive: true,
                maintainAspectRatio: true,
                animation: false,
                scales: {
                    y:  { type: 'linear', display: true, position: 'left',  beginAtZero: true, title: { display: true, text: 'Queue Depth' } },
                    y1: { type: 'linear', display: true, position: 'right', beginAtZero: true, max: 100, title: { display: true, text: 'Buffer Utilization (%)' }, grid: { drawOnChartArea: false } },
                    x:  { title: { display: true, text: 'Time (s)' } }
                }
            }
        });
    }

    connectWebSocket() {
        if (!this.shouldReconnect) {
            return;
        }
        if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
            return;
        }

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/api/ws`;

        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
            console.log('WebSocket connected');
            this.reconnectAttempts = 0;
        };

        this.ws.onmessage = (event) => {
            try {
                const metrics = JSON.parse(event.data);
                this.updateMetrics(metrics);
            } catch (err) {
                console.error('Failed to parse metrics:', err, event.data);
            }
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };

        this.ws.onclose = () => {
            console.log('WebSocket disconnected');
            if (!this.shouldReconnect) {
                return;
            }
            // Exponential backoff with jitter: 1s, 2s, 4s, ... capped at 30s.
            const base = Math.min(MAX_RECONNECT_DELAY_MS, INITIAL_RECONNECT_DELAY_MS * Math.pow(2, this.reconnectAttempts));
            const jitter = Math.random() * 0.3 * base;
            const delay = Math.floor(base + jitter);
            this.reconnectAttempts++;
            this.reconnectTimer = setTimeout(() => this.connectWebSocket(), delay);
        };
    }

    disconnectWebSocket() {
        this.shouldReconnect = false;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
    }

    scheduleChartRedraw() {
        if (this.redrawScheduled) return;
        this.redrawScheduled = true;
        const interval = Math.floor(1000 / CHART_REDRAW_HZ);
        setTimeout(() => {
            this.redrawScheduled = false;
            if (!this.pendingChartData) return;
            const m = this.pendingChartData;
            this.pendingChartData = null;

            const time = (m.duration || 0).toFixed(1);

            if (this.throughputChart.data.labels.length >= this.maxDataPoints) {
                this.throughputChart.data.labels.shift();
                this.throughputChart.data.datasets[0].data.shift();
                this.queueChart.data.labels.shift();
                this.queueChart.data.datasets[0].data.shift();
                this.queueChart.data.datasets[1].data.shift();
            }
            this.throughputChart.data.labels.push(time);
            this.throughputChart.data.datasets[0].data.push(m.throughput || 0);
            this.queueChart.data.labels.push(time);
            this.queueChart.data.datasets[0].data.push(m.queue_depth || 0);
            this.queueChart.data.datasets[1].data.push(m.buffer_utilization || 0);
            this.throughputChart.update();
            this.queueChart.update();
        }, interval);
    }

    updateMetrics(metrics) {
        const produced = metrics.items_produced || 0;
        const consumed = metrics.items_consumed || 0;
        const throughput = (metrics.throughput || 0);
        const duration = (metrics.duration || 0);
        const queueDepth = metrics.queue_depth || 0;
        const bufferUtil = (metrics.buffer_utilization || 0);
        const status = metrics.status || 'idle';

        const set = (id, text) => {
            const el = document.getElementById(id);
            if (el) el.textContent = text;
        };
        set('produced-count', produced);
        set('consumed-count', consumed);

        const tput = document.getElementById('throughput');
        if (tput) tput.textContent = throughput.toFixed(1);
        const dur = document.getElementById('duration');
        if (dur) dur.textContent = duration.toFixed(1);
        set('queue-depth', queueDepth);
        const util = document.getElementById('buffer-util');
        if (util) util.textContent = bufferUtil.toFixed(1);

        const statusText = document.getElementById('status-text');
        if (statusText) {
            statusText.textContent = status;
            statusText.className = `status-${status}`;
        }

        this.pendingChartData = metrics;
        this.scheduleChartRedraw();
    }

    resetMetricCards() {
        const set = (id, text) => {
            const el = document.getElementById(id);
            if (el) el.textContent = text;
        };
        set('produced-count', 0);
        set('consumed-count', 0);
        set('throughput', '0.0');
        set('duration', '0.0');
        set('queue-depth', 0);
        set('buffer-util', '0.0');
    }

    async start() {
        const config = {
            pattern: document.getElementById('pattern-select').value,
            item_count: parseInt(document.getElementById('item-count').value, 10),
            buffer_size: parseInt(document.getElementById('buffer-size').value, 10),
            worker_count: parseInt(document.getElementById('worker-count').value, 10),
            producer_rate: parseInt(document.getElementById('producer-rate').value, 10),
            consumer_rate: parseInt(document.getElementById('consumer-rate').value, 10)
        };

        try {
            const response = await fetch('/api/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            });

            if (response.ok) {
                this.isRunning = true;
                document.getElementById('start-btn').disabled = true;
                document.getElementById('stop-btn').disabled = false;
                this.clearCharts();
                return;
            }

            // Surface the server's error message.
            let detail = response.statusText;
            try {
                const body = await response.json();
                if (body && body.error) {
                    detail = body.error;
                }
            } catch (_) { /* keep statusText */ }
            alert(`Failed to start pattern: ${detail}`);
        } catch (error) {
            console.error('Error starting pattern:', error);
            alert('Failed to start pattern: network error');
        }
    }

    async stop() {
        try {
            const response = await fetch('/api/stop', { method: 'POST' });
            if (response.ok) {
                this.isRunning = false;
                document.getElementById('start-btn').disabled = false;
                document.getElementById('stop-btn').disabled = true;
            } else {
                const text = await response.text();
                alert(`Failed to stop pattern: ${text || response.status}`);
            }
        } catch (error) {
            console.error('Error stopping pattern:', error);
            alert(`Failed to stop pattern: ${error.message || error}`);
        }
    }

    clearCharts() {
        this.pendingChartData = null;
        this.redrawScheduled = false;
        this.throughputChart.data.labels = [];
        this.throughputChart.data.datasets[0].data = [];
        this.queueChart.data.labels = [];
        this.queueChart.data.datasets[0].data = [];
        this.queueChart.data.datasets[1].data = [];
        this.throughputChart.update();
        this.queueChart.update();
    }

    setupPatternInfo() {
        this.updatePatternInfo('buffered');
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new PatternVisualizer();
});
