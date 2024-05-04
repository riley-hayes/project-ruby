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
    messages = []  # Initialize the list to store message points before batching to InfluxDB
    while True:
        message = bus.recv()
        if message:
            # Decode message if possible
            try:
                decoded_message = db.decode_message(message.arbitration_id, message.data)
                decoded_point = Point("can_message") \
                    .tag("interface", can_interface) \
                    .tag("id_hex", f"{message.arbitration_id:08X}") \
                    .time(time.time_ns(), WritePrecision.NS)

                for key, value in decoded_message.items():
                    decoded_point = decoded_point.field(key, value)

                messages.append(decoded_point)
            except KeyError:
                # If message is not defined in DBC file, log raw data
                pass

            # Log raw data separately
            raw_data = ' '.join(format(byte, '02X') for byte in message.data)
            raw_point = Point("raw_can_message") \
                .tag("interface", can_interface) \
                .tag("id_hex", f"{message.arbitration_id:08X}") \
                .field("raw_payload", raw_data) \
                .time(time.time_ns(), WritePrecision.NS)

            messages.append(raw_point)

        current_time = time.time()
        elapsed_time = current_time - start_time
        message_count = len(messages)

        if elapsed_time > 4 or message_count >= 600:
            write_api.write(influxdb_config['bucket'], influxdb_config['org'], messages)
            messages.clear()
            start_time = current_time

except KeyboardInterrupt:
    print("\nScript terminated by user.")
finally:
    client.close()
    bus.shutdown()
