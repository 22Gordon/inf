from datetime import datetime
import json
import os
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from flask import Flask, request

# Initialize Flask app
app = Flask(__name__)

# Get configuration from environment variables with default fallback values
token = os.getenv("INFLUXDB_TOKEN", "default_token")
org = os.getenv("INFLUXDB_ORG", "default_org")
bucket = os.getenv("INFLUXDB_BUCKET", "default_bucket")
influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")

# Initialize InfluxDB client
client = InfluxDBClient(url=influx_url, token=token)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Helper function to write data into InfluxDB (synchronous)
def write_to_influxdb(sensor_id, attribute, value):
    try:
        # Create points from the data and write to InfluxDB
        point = Point("sensor_data") \
            .tag("sensor_id", sensor_id) \
            .field(attribute, value) \
            .time(datetime.utcnow(), WritePrecision.NS)

        # Write data to InfluxDB
        response = write_api.write(bucket, org, point)
        print(f"Data written to InfluxDB: {sensor_id}, {attribute}, {value}")
        print(f"Write response: {response}")
    except Exception as e:
        print(f"Error writing data to InfluxDB: {e}")


@app.route('/notify', methods=['POST'])
def notify_handler():
    try:
        # Log the headers and incoming data
        fiware_service = request.headers.get('fiware-service')
        fiware_servicepath = request.headers.get('fiware-servicepath')
        print(f"Received notification headers:")
        print(f"fiware-service: {fiware_service}")
        print(f"fiware-servicepath: {fiware_servicepath}")

        incoming_data = request.json
        if not incoming_data:
            print("Error: No data received in the notification")
            return "Error: No data received", 400

        print(f"Received notification: {json.dumps(incoming_data, indent=4)}")

        # Process entities in the notification
        for entity in incoming_data.get("data", []):
            sensor_id = entity.get('id')
            print(f"Processing entity: {sensor_id}")

            # Iterate through attributes of the entity
            for attr, value_data in entity.items():
                # Skip 'id', 'type', 'TimeInstant', and 'device_info' attributes
                if attr in ['id', 'type', 'TimeInstant', 'device_info']:
                    continue

                # Ensure value is available
                if isinstance(value_data, dict):
                    value = value_data.get('value')
                else:
                    value = value_data

                if value is not None:
                    print(f"Writing to InfluxDB: {sensor_id}, {attr}, {value}")
                    write_to_influxdb(sensor_id, attr, value)
                else:
                    print(f"Error: No value found for attribute {attr} in entity {sensor_id}")

        return "Notification processed successfully", 200

    except Exception as e:
        print(f"Error processing notification: {e}")
        return "Error processing notification", 500


# Start the Flask app
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)
