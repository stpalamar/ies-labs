from paho.mqtt import client as mqtt_client
import json
import time
from schema.aggregated_data_schema import AggregatedDataSchema
from schema.accelerometer_schema import AccelerometerSchema
from schema.gps_schema import GpsSchema
from schema.parking_schema import ParkingSchema
from file_datasource import FileDatasource
import config


def connect_mqtt(broker, port):
    """Create MQTT client"""
    print(f"CONNECT TO {broker}:{port}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT Broker ({broker}:{port})!")
        else:
            print("Failed to connect {broker}:{port}, return code %d\n", rc)
            exit(rc)  # Stop execution

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    return client


def publish(client, topic, datasource, delay):
    datasource.startReading()
    while True:
        time.sleep(delay)
        data = datasource.read()

        accelerometer_topic = f"{topic}/accelerometer"
        gps_topic = f"{topic}/gps"
        parking_topic = f"{topic}/parking"

        accelerometer_msg = AccelerometerSchema().dumps(data.accelerometer)
        gps_msg = GpsSchema().dumps(data.gps)
        parking_msg = ParkingSchema().dumps(data.parking)

        accelerometer_result = client.publish(accelerometer_topic, accelerometer_msg)
        gps_result = client.publish(gps_topic, gps_msg)
        parking_result = client.publish(parking_topic, parking_msg)

        # result: [0, 1]
        accelerometer_status = accelerometer_result[0]
        printResult(accelerometer_status, accelerometer_topic)

        gps_status = gps_result[0]
        printResult(gps_status, gps_topic)

        parking_status = parking_result[0]
        printResult(parking_status, parking_topic)

def printResult(status, topic):
    if status == 0:
        pass
        # print(f"Send `{msg}` to topic `{topic}`")
    else:
        print(f"Failed to send message to topic {topic}")

def run():
    # Prepare mqtt client
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    # Prepare datasource
    datasource = FileDatasource("data/accelerometer.csv", "data/gps.csv", "data/parking.csv")
    # Infinity publish data
    publish(client, config.MQTT_TOPIC, datasource, config.DELAY)


if __name__ == "__main__":
    run()
