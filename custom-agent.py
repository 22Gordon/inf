from datetime import datetime
import json
import os
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from flask import Flask, request

# Initialize Flask app
app = Flask(__name__)

# InfluxDB configuration from environment variables
token = os.getenv("INFLUXDB_TOKEN", "default-token")  
org = os.getenv("INFLUXDB_ORG", "default_org")  
bucket = os.getenv("INFLUXDB_BUCKET", "default_bucket")  
influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")  # Use default for local setup

# Initialize InfluxDB client
client = InfluxDBClient(url=influx_url, token=token)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Helper function to write data into InfluxDB
def write_to_influxdb(machine_id, attribute, value, domain="sensor"):
    try:
        # Remove 'emeter-' prefix if it exists
        if machine_id.startswith("emeter-"):
            machine_id = machine_id[7:]  
        
        # Construct the entity ID in the desired format: id_0_attribute
        entity_id = f"{machine_id}_0_{attribute}"  # Ex: "312_0_Frequency"
        
        # Only write non-dictionary field values (Phase1Voltage, Frequency ...)
        if isinstance(value, dict):
            print(f"Skipping dictionary field: {attribute}")
            return  

        # Create points with tags and fields
        point = Point("sensor_data") \
            .tag("entity_id", entity_id) \
            .tag("domain", domain) \
            .field(attribute, value) \
            .time(datetime.utcnow(), WritePrecision.NS)

        # Write data to InfluxDB
        response = write_api.write(bucket, org, point)
        print(f"Data written to InfluxDB: {entity_id}, {attribute}, {value}")
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
            machine_id = entity.get('id')  # Extract machine ID
            print(f"Processing entity: {machine_id}")

            # Iterate through attributes of the entity
            for attr, value_data in entity.items():
                # Skip attributes like 'id', 'type', 'TimeInstant', and 'device_info'
                if attr in ['id', 'type', 'TimeInstant', 'device_info']:
                    continue

                # Ensure value is available and process
                if isinstance(value_data, dict):
                    value = value_data.get('value')
                else:
                    value = value_data 

                # If valid value, write to InfluxDB
                if value is not None:
                    print(f"Writing to InfluxDB: {machine_id}, {attr}, {value}")
                    write_to_influxdb(machine_id, attr, value)
                else:
                    print(f"Error: No value found for attribute {attr} in entity {machine_id}")

        return "Notification processed successfully", 200

    except Exception as e:
        print(f"Error processing notification: {e}")
        return "Error processing notification", 500


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)  
