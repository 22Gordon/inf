from datetime import datetime
import json
import os
import logging
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from flask import Flask, request

# Initialize Flask app
app = Flask(__name__)

# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()  # Set default log level to INFO
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# InfluxDB configuration from environment variables
INFLUX_CONFIG = {
    "token": os.getenv("INFLUXDB_TOKEN", "default-token"),
    "org": os.getenv("INFLUXDB_ORG", "default_org"),
    "bucket": os.getenv("INFLUXDB_BUCKET", "default_bucket"),
    "url": os.getenv("INFLUXDB_URL", "http://localhost:8086"),
}

# Initialize InfluxDB client
client = InfluxDBClient(url=INFLUX_CONFIG["url"], token=INFLUX_CONFIG["token"])
write_api = client.write_api(write_options=SYNCHRONOUS)


def extract_machine_id(machine_id):
    """
    Extract and format the machine ID.
    Handles cases like 'emeter-312' and 'emeter-312-2'.
    """
    try:
        if machine_id.startswith(("emeter-", "dmeter-", "gmeter-")):
            parts = machine_id.split('-')
            if len(parts) == 2:
                return parts[1]
            elif len(parts) == 3:
                return f"{parts[1]}-{parts[2]}"
        return machine_id
    except Exception as e:
        logger.error(f"Error extracting machine ID: {e}")
        return None


def write_to_influxdb(machine_id, attribute, value, domain="sensor"):
    """
    Write a data point to InfluxDB.
    """
    try:
        if isinstance(value, dict):
            logger.debug(f"Skipping dictionary field: {attribute}")
            return

        entity_id = f"{machine_id}_0_{attribute}"
        point = (
            Point("sensor_data")
            .tag("entity_id", entity_id)
            .tag("domain", domain)
            .field(attribute, value)
            .time(datetime.utcnow(), WritePrecision.NS)
        )

        write_api.write(INFLUX_CONFIG["bucket"], INFLUX_CONFIG["org"], point)
        logger.info(f"Data written to InfluxDB: {entity_id}, {attribute}, {value}")
    except Exception as e:
        logger.error(f"Error writing to InfluxDB: {e}")


@app.route("/notify", methods=["POST"])
def notify_handler():
    """
    Handle incoming notifications from FIWARE.
    """
    try:
        # Log headers for debugging
        fiware_service = request.headers.get("fiware-service")
        fiware_servicepath = request.headers.get("fiware-servicepath")
        logger.debug(f"Headers - fiware-service: {fiware_service}, fiware-servicepath: {fiware_servicepath}")

        # Validate incoming data
        incoming_data = request.json
        if not incoming_data:
            logger.error("No data received in the notification.")
            return "Error: No data received", 400

        logger.debug(f"Incoming data: {json.dumps(incoming_data, indent=2)}")

        # Process each entity in the notification
        for entity in incoming_data.get("data", []):
            machine_id = entity.get("id")
            if not machine_id:
                logger.warning("Entity without ID found; skipping.")
                continue

            machine_id = extract_machine_id(machine_id)
            if not machine_id:
                logger.warning("Failed to extract valid machine ID; skipping entity.")
                continue

            logger.info(f"Processing entity: {machine_id}")

            # Process each attribute in the entity
            for attr, value_data in entity.items():
                if attr in ["id", "type", "TimeInstant", "device_info"]:
                    continue

                value = value_data.get("value") if isinstance(value_data, dict) else value_data
                if value is not None:
                    logger.debug(f"Writing attribute: {attr} with value: {value}")
                    write_to_influxdb(machine_id, attr, value)
                else:
                    logger.warning(f"No value found for attribute: {attr}")

        return "Notification processed successfully", 200

    except Exception as e:
        logger.error(f"Error processing notification: {e}")
        return "Error processing notification", 500


if __name__ == "__main__":
    DEBUG = os.getenv("DEBUG", "False").lower() == "true"
    app.run(debug=DEBUG, host="0.0.0.0", port=5000)
