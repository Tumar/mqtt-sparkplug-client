import os

import paho.mqtt.client as mqtt
import sparkplug_b as sparkplug
import datetime
import json

from google.protobuf.json_format import *
from sparkplug_b import *

# APPLICATION configuration parameters -----------------------------------------------
_config_mqtt_host = os.environ.get("MQTT_HOST", "localhost")
_config_mqtt_port = int(os.environ.get("MQTT_PORT", 1883))
_config_mqtt_topic = os.environ.get("MQTT_TOPIC", "spBv1.0/#")

def on_connect(client, userdata, flags, rc):
    """
        MQTT Callback function for connect events
    """
    if rc == 0:
        topic = _config_mqtt_topic
        client.subscribe(topic)
        print("MQTT Client connected and subscribed to topic: " + topic)
    else:
        print("MQTT Client failed to connect with result code " + str(rc))


def on_message(client, userdata, msg):
    """
        MQTT Callback function for received messages events
    """
    inboundPayload = sparkplug_b_pb2.Payload()
    inboundPayload.ParseFromString(msg.payload)
    print(datetime.datetime.utcnow().isoformat() + " " + msg.topic)
    print(MessageToJson(inboundPayload))
    

# Set up the MQTT client connection that will listen to all Sparkplug B messages
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(_config_mqtt_host, _config_mqtt_port)  # Connect to MQTT
print("Connecting to MQTT broker server - %s:%d" % (_config_mqtt_host, _config_mqtt_port))

# Loop forever
client.loop_forever()
