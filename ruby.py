import can
import cantools
import time
import yaml
import threading
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os
import pickle
import glob
from urllib.parse import urlparse

from threading import Lock

# Load configuration from YAML file
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Database and CAN settings
influxdb_config = config['influxdb']
dbc_path = config['dbc_path']
db = cantools.database.load_file(dbc_path)

# InfluxDB client setup
client = InfluxDBClient(url=influxdb_config['url'], token=influxdb_config['token'], org=influxdb_config['org'])
write_api = client.write_api(write_options=SYNCHRONOUS)

# Thread-safe message storage
messages = []
messages_lock = Lock()

def can_bus_listener(can_interface, control_flag, messages):
    bus = can.interface.Bus(channel=can_interface, bustype='socketcan', buffer_size=10000)
    try:
        while control_flag[can_interface]:
            message = bus.recv()
            if message:
                process_message(message, can_interface, messages)
    except Exception as e:
        print(f"Exception in listener {can_interface}: {e}")
    finally:
        bus.shutdown()

def process_message(message, can_interface, messages):
    try:
        decoded_message = db.decode_message(message.arbitration_id, message.data)
        decoded_point = Point("can_message").tag("interface", can_interface).tag("id_hex", f"{message.arbitration_id:08X}").time(time.time_ns(), WritePrecision.NS)
        for key, value in decoded_message.items():
            decoded_point = decoded_point.field(key, value)
        
        with messages_lock:
            messages.append(decoded_point)
    except KeyError:
        pass  # Optionally log raw data here

# Thread management
threads = {}
control_flags = {conf['interface_name']: conf['enabled'] for conf in config['can_interfaces']}

def toggle_can_interface(interface_name, state):
    control_flags[interface_name] = state
    if state and interface_name not in threads:
        thread = threading.Thread(target=can_bus_listener, args=(interface_name, control_flags, messages), daemon=True)
        threads[interface_name] = thread
        thread.start()
    elif not state:
        threads.pop(interface_name, None)

# Start threads based on configuration
for interface_config in config['can_interfaces']:
    if interface_config['enabled']:
        toggle_can_interface(interface_config['interface_name'], True)

try:
    while True:
        with messages_lock:
            if messages:
                write_api.write(influxdb_config['bucket'], influxdb_config['org'], messages.copy())
                messages.clear()
        time.sleep(10)  # Adjust the frequency of writes as needed
except KeyboardInterrupt:
    print("\nScript terminated by user.")
finally:
    for flag in control_flags:
        control_flags[flag] = False  # Signal all threads to stop
    for thread in threads.values():
        thread.join()  # Wait for all threads to finish
    client.close()

