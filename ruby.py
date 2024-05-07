import can
import cantools
import time
import yaml
import threading
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from threading import Lock, Thread
from queue import Queue, Empty

# Load configuration from YAML file
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Load DBC files for each CAN interface
dbc_databases = {}
for interface_config in config['can_interfaces']:
    dbc_path = interface_config['dbc_path']
    dbc_databases[interface_config['interface_name']] = cantools.database.load_file(dbc_path)

# Thread-safe message storage
messages_lock = Lock()
write_queue = Queue()

def can_bus_listener(can_interface, control_flag, db, write_queue):
    bus = can.interface.Bus(channel=can_interface, bustype='socketcan', buffer_size=10000)
    print(f"Started logging on {can_interface}")
    try:
        while control_flag[can_interface]:
            message = bus.recv()
            if message:
                process_message(message, can_interface, db, write_queue)
    except Exception as e:
        print(f"Exception in listener {can_interface}: {e}")
    finally:
        bus.shutdown()
        print(f"Stopped logging on {can_interface}")

def process_message(message, can_interface, db, write_queue):
    try:
        decoded_message = db.decode_message(message.arbitration_id, message.data)
        points = []
        for key, value in decoded_message.items():
            # Convert NamedSignalValue to a supported data type (int or str)
            if isinstance(value, cantools.database.namedsignalvalue.NamedSignalValue):
                value = value.value  # Use .value for the numerical representation, or str(value) for the string
            point = Point("can_message").tag("interface", can_interface).tag("id_hex", f"{message.arbitration_id:08X}").field(key, value).time(time.time_ns(), WritePrecision.NS)
            points.append(point)
        write_queue.put(points)
    except KeyError:
        pass  # Optionally handle or log unknown messages

def influxdb_writer(write_queue, influxdb_config):
    client = InfluxDBClient(url=influxdb_config['url'], token=influxdb_config['token'], org=influxdb_config['org'])
    write_api = client.write_api(write_options=SYNCHRONOUS)
    try:
        while True:
            try:
                points = write_queue.get(timeout=10)
                if points is None:
                    break
                write_api.write(influxdb_config['bucket'], influxdb_config['org'], points)
                write_queue.task_done()
            except Empty:
                continue
    finally:
        client.close()

# Start threads based on configuration
threads = {}
control_flags = {conf['interface_name']: conf['enabled'] for conf in config['can_interfaces']}
for interface_config in config['can_interfaces']:
    if interface_config['enabled']:
        db = dbc_databases[interface_config['interface_name']]
        thread = threading.Thread(target=can_bus_listener, args=(interface_config['interface_name'], control_flags, db, write_queue), daemon=True)
        threads[interface_config['interface_name']] = thread
        thread.start()

# Start the InfluxDB writer thread
writer_thread = Thread(target=influxdb_writer, args=(write_queue, config['influxdb']), daemon=True)
writer_thread.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nScript terminated by user.")
finally:
    for flag in control_flags:
        control_flags[flag] = False
    for thread in threads.values():
        thread.join()
    write_queue.put(None)
    writer_thread.join()
