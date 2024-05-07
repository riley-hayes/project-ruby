import can
import cantools
import time
import yaml
import threading
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from threading import Lock

# Load configuration from YAML file
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

# InfluxDB client setup
influxdb_config = config['influxdb']
client = InfluxDBClient(url=influxdb_config['url'], token=influxdb_config['token'], org=influxdb_config['org'])
write_api = client.write_api(write_options=SYNCHRONOUS)

# Load DBC files for each CAN interface
dbc_databases = {}
for interface_config in config['can_interfaces']:
    dbc_path = interface_config['dbc_path']
    dbc_databases[interface_config['interface_name']] = cantools.database.load_file(dbc_path)

# Thread-safe message storage
messages = []
messages_lock = Lock()

def can_bus_listener(can_interface, control_flag, messages):
    bus = can.interface.Bus(channel=can_interface, bustype='socketcan', buffer_size=10000)
    db = dbc_databases[can_interface]
    try:
        print(f"Started listening on {can_interface}")
        while control_flag[can_interface]:
            message = bus.recv()
            if message:
                process_message(message, can_interface, db, messages)
    except Exception as e:
        print(f"Exception in listener {can_interface}: {e}")
    finally:
        bus.shutdown()

def process_message(message, can_interface, db, messages):
    try:
        decoded_message = db.decode_message(message.arbitration_id, message.data)
        decoded_point = Point("can_message").tag("interface", can_interface).tag("id_hex", f"{message.arbitration_id:08X}").time(time.time_ns(), WritePrecision.NS)
        for key, value in decoded_message.items():
            # Check if the value is a NamedSignalValue and convert it
            if isinstance(value, cantools.database.namedsignalvalue.NamedSignalValue):                value = value.physical_value  # Or `str(value)` if you prefer the name rather than the number
            decoded_point = decoded_point.field(key, value)
        
        with messages_lock:
            messages.append(decoded_point)
    except KeyError:
        raw_data = ' '.join(format(byte, '02X') for byte in message.data)
        raw_point = Point("raw_can_message").tag("interface", can_interface).tag("id_hex", f"{message.arbitration_id:08X}").field("raw_payload", raw_data).time(time.time_ns(), WritePrecision.NS)
        
        with messages_lock:
            messages.append(raw_point)
        #print(f"Logged raw data for unknown message ID {message.arbitration_id:08X} on {can_interface}.")

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
