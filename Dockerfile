# Use the official Python slim image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Expose the port your app runs on
EXPOSE 5000

# Set default environment variables
ENV INFLUXDB_URL=http://localhost:8086 \
    INFLUXDB_TOKEN=default_token \
    INFLUXDB_ORG=default_org \
    INFLUXDB_BUCKET=default_bucket

# Run the application
CMD ["python", "custom-agent.py"]
