import can
import cantools
import time
import yaml
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Load configuration from YAML file
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

influxdb_config = config['influxdb']
can_interface = config['can_interface']
dbc_path = config['dbc_path']  # Path to your DBC file

# Load DBC file
db = cantools.database.load_file(dbc_path)

# InfluxDB client setup
client = InfluxDBClient(url=influxdb_config['url'], token=influxdb_config['token'], org=influxdb_config['org'])
write_api = client.write_api(write_options=SYNCHRONOUS)

bus = can.interface.Bus(channel=can_interface, bustype='socketcan', buffer_size=10000)
start_time = time.time()

try:
    print(f"Listening on {can_interface} and logging to InfluxDB...")

    messages = []

    while True:
        message = bus.recv()

        if message:
            decoded_message = db.decode_message(message.arbitration_id, message.data)
            point = Point("can_message") \
                .tag("interface", can_interface) \
                .time(time.time_ns(), WritePrecision.NS)
            
            # Add each decoded field as a separate field in the point
            for key, value in decoded_message.items():
                point = point.field(key, value)

            messages.append(point)

        current_time = time.time()
        elapsed_time = current_time - start_time
        message_count = len(messages)

        print(f"{elapsed_time} has elapsed, and this is the amount of messages: {message_count}")

        if elapsed_time > 4 or message_count >= 600:
            write_api.write(influxdb_config['bucket'], influxdb_config['org'], messages)
            messages.clear()
            start_time = current_time

except KeyboardInterrupt:
    print("\nScript terminated by user.")
finally:
    client.close()
    bus.shutdown()
