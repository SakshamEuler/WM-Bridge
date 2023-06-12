import mysql.connector
import paho.mqtt.client as mqtt
import ssl
from ChargePoint import ChargePoint
import websocket
import threading
import json
import asyncio



mydb = mysql.connector.connect(
host="steve-db-cms.cqry44wn7lp3.ap-south-1.rds.amazonaws.com",
user='cms_admin',
password='s2VuUS34wxWO18yQtbkz',
database="MqttWeb"
)
print("db connect inside subscriber file")
# Define Variables
MQTT_PORT = 8883
MQTT_KEEPALIVE_INTERVAL = 45
imei = "866907056709165"
# MQTT_TOPIC = "864394040833701/data"
MQTT_TOPIC = imei+"/data"
# MQTT_TOPIC = "864394040833701/commands"
MQTT_MSG = "hello MQTT"

MQTT_HOST = "a4npr11hez19b-ats.iot.ap-south-1.amazonaws.com"
CA_ROOT_CERT_FILE = "amazonRoot.pem"
THING_CERT_FILE = "cert.pem.crt"    
THING_PRIVATE_KEY = "privateKey.pem.key"


# Define on connect event function
# We shall subscribe to our Topic in this function
def on_connect(mosq, obj, rc, properties = None):
    print("on connect")
    mqttc.subscribe(MQTT_TOPIC, 0)

async def wss_connect(imei, action):
    # async with websockets.connect(
    #     "ws://13.234.76.186:8080/steve/websocket/CentralSystemService/" + imei, subprotocols=["ocpp1.6"]
    # ) as ws:
    ws = websocket.WebSocketApp("ws://13.234.76.186:8080/steve/websocket/CentralSystemService/" + imei, on_message=on_message)
    # ws.run_forever()

    cp = ChargePoint(imei, ws)

    await asyncio.gather(cp.start(imei, action), cp.send_action_notification(imei, action))
    ws.run_forever()
# Define on_message event function. 
# This function will be invoked every time,
# a new message arrives for the subscribed topic 
lock = threading.Lock()
async def on_message(mosq, obj, msg):

    lock.acquire()
    print("on message")
    mycursor = mydb.cursor()

    a = msg.payload

    print(a)            
    

    if b'"RSP"' in a:

        # message = dict.get("RSP")
        if a[8:9] == b'[':

            c = b''
            flag = False
            for byte in a:
                byte_as_int = int(byte)
                byte_as_byte = bytes([byte_as_int])
                if (byte_as_byte == b'['):
                    flag = True
                if (byte_as_byte == b']'):
                    flag = False
                    c += byte_as_byte
                if (flag):
                    c += byte_as_byte

            c = c.decode()
            print("decoded: "+c)
            message = json.loads(c)
            action = message[2]

            id = imei

            message = str(message)
            sql = "UPDATE subscriberOne SET "+ action +" = %s WHERE imei = %s"
            print("..............................................")
            print(message)
            print(type(message))
            print(action)
            print("..............................................")
            val = (message, id)
            mycursor.execute(sql, val)
            
            # cp = ChargePoint(imei, ws)
            # await asyncio.gather(cp.start(imei, action), cp.send_action_notification(imei, action))

            await asyncio.run(wss_connect(id, action))

    lock.release()
    # time.sleep(7)


def on_subscribe(mosq, obj, mid, granted_qos):
    print("Subscribed to Topic: " + 
    MQTT_MSG + " with QoS: " + str(granted_qos))

# Initiate MQTT Client
mqttc = mqtt.Client()

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe
# Configure TLS Set
mqttc.tls_set(CA_ROOT_CERT_FILE, certfile=THING_CERT_FILE, keyfile=THING_PRIVATE_KEY, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)


# Connect with MQTT Broker
mqttc.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)


# Continue monitoring the incoming messages for subscribed topic
mqttc.loop_forever()