# GPU Website Generator Docker Image
FROM python:3.11-slim

LABEL maintainer="GPU Health Monitoring Team"
LABEL description="Containerized GPU monitoring website generator and server"

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy Python files
COPY gpu_website_generator.py .
COPY server.py .

# Install Python dependencies
# Note: The actual dependencies would need to be copied from parent directory
# For now, we'll install the basics needed
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    typer \
    matplotlib \
    seaborn \
    sqlite3

# Create directories for data and output
RUN mkdir -p /app/data /app/websites

# Expose port for web server
EXPOSE 8000

# Set environment variables
ENV PYTHONPATH="/app"
ENV WEB_PORT=8000
ENV WEB_HOST=0.0.0.0

# Default command runs the web server
CMD ["python", "server.py", "--port", "8000", "--host", "0.0.0.0", "--directory", "/app/websites"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/ || exit 1