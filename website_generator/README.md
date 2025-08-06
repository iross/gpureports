# GPU Monitoring Website Generator

A containerized solution for generating and hosting GPU cluster monitoring websites with real-time timeline visualizations.

## Features

- **Multi-Host Visualization**: Generate comprehensive dashboards showing GPU utilization across multiple hosts
- **Interactive Timeline Heatmaps**: Detailed timeline views for individual hosts with hover tooltips
- **Responsive Design**: Mobile-friendly web interface with professional styling
- **Automated Updates**: Optional periodic website regeneration
- **Containerized Deployment**: Easy Docker-based deployment with docker-compose
- **Simple Web Server**: Built-in static file server for hosting generated websites

## Quick Start

### Using Docker Compose (Recommended)

1. **Build and start services**:
   ```bash
   docker-compose up -d
   ```

2. **View the website**:
   Open [http://localhost:8000](http://localhost:8000) in your browser

3. **Stop services**:
   ```bash
   docker-compose down
   ```

### Manual Generation

1. **Generate a website**:
   ```bash
   python gpu_website_generator.py \
     --db-path ../gpu_state_2025-08.db \
     --output-dir ./websites \
     --hours-back 24 \
     --title "My GPU Cluster Dashboard"
   ```

2. **Serve the website**:
   ```bash
   python server.py --directory ./websites --port 8000
   ```

3. **View the website**:
   Open [http://localhost:8000](http://localhost:8000)

## Directory Structure

```
website_generator/
├── gpu_website_generator.py    # Main website generator script
├── server.py                   # Simple HTTP server
├── Dockerfile                  # Docker image definition
├── docker-compose.yml          # Docker Compose configuration
├── README.md                   # This file
└── websites/                   # Generated websites output directory
    ├── index.html              # Main dashboard page
    └── host_*.html             # Individual host timeline pages
```

## Configuration Options

### Website Generator (`gpu_website_generator.py`)

- `--db-path`: Path to SQLite database (default: `gpu_state_2025-08.db`)
- `--output-dir`: Output directory for website (default: `gpu_website`)
- `--hours-back`: Number of hours to analyze (default: `24`)
- `--end-time`: End time for analysis (format: `YYYY-MM-DD HH:MM:SS`)
- `--min-gpus`: Minimum GPUs required for host inclusion (default: `1`)
- `--title`: Website title (default: `GPU Cluster Monitoring Dashboard`)
- `--max-hosts`: Maximum number of hosts to process (for testing)

### Web Server (`server.py`)

- `--port`, `-p`: Port to serve on (default: `8000`)
- `--host`, `-H`: Host to bind to (default: `0.0.0.0`)
- `--directory`, `-d`: Directory to serve (default: current directory)
- `--verbose`, `-v`: Enable verbose logging

## Docker Services

The `docker-compose.yml` defines two services:

### 1. gpu-website-generator (Web Server)
- **Purpose**: Serves the generated websites
- **Port**: 8000
- **Health Check**: Verifies server is responding
- **Auto-restart**: Yes

### 2. gpu-website-updater (Optional)
- **Purpose**: Automatically regenerates websites every hour
- **Schedule**: Runs every 3600 seconds (1 hour)
- **Dependencies**: Requires main web server to be running
- **Auto-restart**: Yes

## Volume Mounts

The Docker setup uses several volume mounts:

- `../:/app/data:ro` - Database directory (read-only)
- `./websites:/app/websites` - Generated websites output
- `../gpu_timeline_heatmap.py:/app/gpu_timeline_heatmap.py:ro` - Timeline generator (read-only)
- `../gpu_utils.py:/app/gpu_utils.py:ro` - Utility functions (read-only)

## Development

### Prerequisites

- Python 3.11+
- Dependencies: `pandas`, `numpy`, `typer`, `matplotlib`, `seaborn`
- SQLite database with GPU state data

### Local Development

1. **Install dependencies**:
   ```bash
   pip install pandas numpy typer matplotlib seaborn
   ```

2. **Run generator locally**:
   ```bash
   python gpu_website_generator.py --db-path /path/to/database.db
   ```

3. **Test web server**:
   ```bash
   python server.py --directory ./websites --verbose
   ```

## Troubleshooting

### Common Issues

1. **Port already in use**:
   ```bash
   # Try a different port
   docker-compose up -d -p 8080:8000
   ```

2. **Database not found**:
   - Ensure the database path in `docker-compose.yml` is correct
   - Check that the database file exists and is readable

3. **Website not updating**:
   - Check the updater service logs: `docker-compose logs gpu-website-updater`
   - Manually trigger update: `docker-compose restart gpu-website-updater`

4. **Permission issues**:
   ```bash
   # Fix website directory permissions
   sudo chown -R $(whoami):$(whoami) websites/
   ```

### Debugging

1. **View service logs**:
   ```bash
   # All services
   docker-compose logs -f
   
   # Specific service
   docker-compose logs -f gpu-website-generator
   ```

2. **Execute commands in container**:
   ```bash
   docker-compose exec gpu-website-generator bash
   ```

3. **Check service health**:
   ```bash
   docker-compose ps
   ```

## Performance Considerations

- **Memory Usage**: Each host page generation uses ~50-100MB of memory
- **Generation Time**: Scales with number of hosts and time range
- **Storage**: Generated websites are typically 1-10MB per host
- **Network**: Minimal bandwidth requirements for static hosting

## Security Notes

- The web server binds to `0.0.0.0` by default (all interfaces)
- No authentication is implemented - suitable for internal networks only
- Database is mounted read-only for security
- Consider using a reverse proxy (nginx) for production deployments

## License

This project is part of the GPU Health Monitoring system.