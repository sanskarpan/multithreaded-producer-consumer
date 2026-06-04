# Web UI Guide

## Overview

The Producer-Consumer Web UI provides an interactive, real-time visualization of all concurrency patterns with live metrics and charts.

## Features

### 🎮 Interactive Controls
- **Pattern Selector**: Choose from 6 different patterns
- **Dynamic Configuration**: Adjust parameters based on selected pattern
  - Items to process (10-10,000)
  - Buffer size (10-1,000)
  - Worker count (1-16)
  - Producer/Consumer rates (10-200 items/sec)
- **Start/Stop Controls**: Run patterns on demand

### 📊 Real-Time Metrics
Live updating metrics displayed in dashboard cards:
- **Items Produced**: Total items generated
- **Items Consumed**: Total items processed
- **Throughput**: Current items per second
- **Duration**: Elapsed time
- **Queue Depth**: Current buffer usage
- **Buffer Utilization**: Percentage of buffer capacity used

### 📈 Live Charts
Two interactive charts using Chart.js:
1. **Throughput Over Time**: Line chart showing processing rate
2. **Queue Depth & Buffer Utilization**: Dual-axis chart tracking buffer state

### 📖 Pattern Information
Context-sensitive information panel showing:
- Pattern description
- Key features
- Best use cases
- When to use this pattern

## Architecture

### Backend (Go)
```
web/server/
├── server.go      # HTTP server, REST API, pattern execution
└── websocket.go   # WebSocket handler for real-time updates
```

**REST API Endpoints:**
- `GET /api/patterns` - List available patterns
- `POST /api/start` - Start pattern with configuration
- `POST /api/stop` - Stop running pattern
- `GET /api/metrics` - Get current metrics snapshot
- `GET /api/ws` - WebSocket upgrade endpoint

### Frontend (HTML/CSS/JavaScript)
```
web/static/
├── index.html  # UI structure
├── style.css   # Responsive styling
└── app.js      # Interactive logic & charts
```

**Technology Stack:**
- Vanilla JavaScript (no frameworks)
- Chart.js for visualizations
- WebSocket for real-time updates
- Responsive CSS Grid layout

## How It Works

### 1. Pattern Execution Flow
```
User selects pattern → Configure parameters → Click Start
    ↓
Server creates pattern instance with metrics collection
    ↓
Pattern runs with 100ms metric snapshots
    ↓
Metrics broadcast via WebSocket to all connected clients
    ↓
Frontend updates charts and metrics in real-time
```

### 2. Metrics Collection
The `metrics.Collector` system:
- Initializes metrics when pattern starts
- Updates every 100ms with current state
- Calculates throughput, utilization, etc.
- Broadcasts to WebSocket subscribers
- Marks pattern as completed when done

### 3. WebSocket Communication
```javascript
// Frontend connects to WebSocket
ws = new WebSocket('ws://localhost:8080/api/ws');

// Receives metric updates
ws.onmessage = (event) => {
    const metrics = JSON.parse(event.data);
    updateUI(metrics);
    updateCharts(metrics);
};
```

## Running the Web UI

### Quick Start
```bash
# From project root
go run cmd/webui/main.go
```

### Custom Port
```bash
go run cmd/webui/main.go -addr :3000
```

### Build and Run
```bash
go build -o webui cmd/webui/main.go
./webui
```

Then open http://localhost:8080 in your browser.

## Usage Examples

### Example 1: Comparing Buffer Sizes
1. Select "Buffered Channel" pattern
2. Set items to 1000
3. Run with buffer size 10
4. Observe queue depth chart (likely high utilization)
5. Stop and increase buffer to 500
6. Run again - see smoother performance

### Example 2: Worker Pool Scaling
1. Select "Worker Pool" pattern
2. Set items to 1000
3. Try with 2, 4, 8, 16 workers
4. Compare throughput in each run
5. Find optimal worker count for your system

### Example 3: Rate Limiting
1. Select "Rate-Limited" pattern
2. Set producer rate to 50 items/sec
3. Set consumer rate to 100 items/sec
4. Watch throughput plateau at producer rate
5. Reverse the rates and see backpressure

## Troubleshooting

### WebSocket Connection Fails
- Ensure server is running on correct port
- Check browser console for errors
- Verify no firewall blocking WebSocket connections

### Metrics Not Updating
- Refresh the page to reconnect WebSocket
- Check server logs for errors
- Ensure pattern is actually running

### Charts Not Displaying
- Verify Chart.js CDN is accessible
- Check browser console for JavaScript errors
- Ensure modern browser (supports ES6)

## Browser Compatibility

Tested and working on:
- ✅ Chrome 90+
- ✅ Firefox 88+
- ✅ Safari 14+
- ✅ Edge 90+

Requires:
- WebSocket support
- ES6 JavaScript
- CSS Grid

## Performance Notes

- Metrics update every 100ms (10 times per second)
- Charts keep last 50 data points
- WebSocket subscriber buffer: 256 messages (drop on overflow; back-pressure to broadcaster is non-blocking)
- Chart redraws are throttled to ~4 Hz so a slow browser tab doesn't melt the CPU
- Reconnect uses exponential backoff (1s → 30s) with jitter

## Future Enhancements

Potential additions:
- [ ] Pattern comparison mode (run multiple patterns side-by-side)
- [ ] Export metrics to CSV
- [ ] Historical run comparison
- [ ] Custom pattern configuration saving
- [ ] Dark mode toggle
- [ ] Mobile-responsive improvements
