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
    unknown_messages = []

    while True:
        message = bus.recv()

        if message:
            try:
                decoded_message = db.decode_message(message.arbitration_id, message.data)
                point = Point("can_message") \
                    .tag("interface", can_interface) \
                    .time(time.time_ns(), WritePrecision.NS)

                for key, value in decoded_message.items():
                    point = point.field(key, value)
                messages.append(point)
            except KeyError:
                # Log unknown message IDs to a separate measurement or tag them differently
                unknown_point = Point("unknown_can_message") \
                    .tag("interface", can_interface) \
                    .field("arbitration_id", message.arbitration_id) \
                    .field("raw_data", ' '.join(format(byte, '02X') for byte in message.data)) \
                    .time(time.time_ns(), WritePrecision.NS)
                unknown_messages.append(unknown_point)
                print(f"Logged unknown message with ID {message.arbitration_id}")

        current_time = time.time()
        elapsed_time = current_time - start_time
        message_count = len(messages) + len(unknown_messages)

        print(f"{elapsed_time} has elapsed, and this is the amount of messages: {message_count}")

        if elapsed_time > 4 or message_count >= 600:
            if messages:
                write_api.write(influxdb_config['bucket'], influxdb_config['org'], messages)
            if unknown_messages:
                write_api.write(influxdb_config['bucket'], influxdb_config['org'], unknown_messages)
            messages.clear()
            unknown_messages.clear()
            start_time = current_time

except KeyboardInterrupt:
    print("\nScript terminated by user.")
finally:
    client.close()
    bus.shutdown()
