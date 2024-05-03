import can
import time
import yaml
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Load configuration from YAML file
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

influxdb_config = config['influxdb']
can_interface = config['can_interface']

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
            point = Point("can_message") \
                .tag("interface", can_interface) \
                .field("id_hex", message.arbitration_id) \
                .field("length", len(message.data)) \
                .field("payload", ' '.join(format(byte, '02X') for byte in message.data)) \
                .time(time.time_ns(), WritePrecision.NS)
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
