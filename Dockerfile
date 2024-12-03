# Use the official Python slim image
FROM python:3.9.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && rm -rf /root/.cache

# Copy the application code
COPY . .

# Set default environment variables
ENV INFLUXDB_URL=http://localhost:8086 \
    INFLUXDB_TOKEN=default_token \
    INFLUXDB_ORG=default_org \
    INFLUXDB_BUCKET=default_bucket \
    LOG_LEVEL=INFO

# Expose the port your app runs on
EXPOSE 5000

# Add a health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s \
  CMD curl -f http://localhost:5000/ || exit 1

# Run the application
CMD ["python", "custom-agent.py"]
