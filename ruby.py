import can
import cantools
import time
import yaml
import os
import pickle
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
    decoded_message = db.decode_message(message.arbitration_id, message.data)
    points = []
    for key, value in decoded_message.items():
        if isinstance(value, cantools.database.namedsignalvalue.NamedSignalValue):
            value = value.value
        point = Point("can_message").tag("interface", can_interface).tag("id_hex", f"{message.arbitration_id:08X}").field(key, value).time(time.time_ns(), WritePrecision.NS)
        points.append(point)
    write_queue.put(points)

def influxdb_writer(write_queue, influxdb_config):
    client = InfluxDBClient(url=influxdb_config['url'], token=influxdb_config['token'], org=influxdb_config['org'])
    write_api = client.write_api(write_options=SYNCHRONOUS)
    offline_storage_path = 'offline_data.pkl'

    def restore_offline_data():
        if os.path.exists(offline_storage_path):
            with open(offline_storage_path, 'rb') as file:
                while True:
                    try:
                        points = pickle.load(file)
                        write_api.write(influxdb_config['bucket'], influxdb_config['org'], points)
                        print(f"Restored {len(points)} points from offline storage.")
                    except EOFError:
                        break
            os.remove(offline_storage_path)

    def save_offline(points):
        with open(offline_storage_path, 'ab') as file:
            pickle.dump(points, file)

    restore_offline_data()

    try:
        while True:
            try:
                points = []
                while not write_queue.empty():
                    points.extend(write_queue.get_nowait())
                    write_queue.task_done()

                if points:
                    write_api.write(influxdb_config['bucket'], influxdb_config['org'], points)
                    print(f"Sent {len(points)} points to InfluxDB.")
                else:
                    time.sleep(5)
            except Exception as e:
                print(f"Error writing to InfluxDB: {e}")
                save_offline(points)
                time.sleep(60)
    finally:
        client.close()
        print("InfluxDB connection closed.")

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
