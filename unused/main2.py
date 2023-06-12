
from ChargePoint import ChargePoint
import websockets
import asyncio
import multiple_processes
import mysql.connector

# from publisher_thor import Publisher
# from mqtt_sub import subscriber
# from subscriber_thor import subscriber
import threading
import time
import asyncio
import queue
import mysql.connector
import json
import paho.mqtt.client as mqtt
import ssl

q = queue.Queue()

def mqtt_sub(imei):
    print(f"in mqtt sub {imei}")
    # sub = subscriber()
    # sub._start(imei)   
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
    
    # Define on_message event function. 
    # This function will be invoked every time,
    # a new message arrives for the subscribed topic 
    lock = threading.Lock()
    def on_message(mosq, obj, msg):

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
                mydb.commit()
                # cp = ChargePoint(imei, ws)
                # await asyncio.gather(cp.start(imei, action), cp.send_action_notification(imei, action))

                # asyncio.run(wss_connect(id, action))
                q.put(action)
                print(f"put {action} in queue")

                time.sleep(2)

        lock.release()
        # time.sleep(7)


    def on_subscribe(mosq, obj, mid, granted_qos):
        print("Subscribed to Topic: " + 
        MQTT_MSG + " with QoS: " + str(granted_qos))

    # Initiate MQTT Client
    mqttc = mqtt.Client()

    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_subscribe = on_subscribe
    mqttc.tls_set(CA_ROOT_CERT_FILE, certfile=THING_CERT_FILE, keyfile=THING_PRIVATE_KEY, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)


    # Connect with MQTT Broker
    mqttc.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)


    # Continue monitoring the incoming messages for subscribed topic
    mqttc.loop_forever() 

async def main(imei):
    print("in the wss function")        
    async with websockets.connect(
        "ws://13.234.76.186:8080/steve/websocket/CentralSystemService/" + imei, subprotocols=["ocpp1.6"]
    ) as ws:

        # while True:
        print("wss connection established")
    
        cp = ChargePoint(imei, ws)
        print("dsjfh")
        # action = multiple_processes.q.get()
        while True:
            try:
                action = q.get(timeout=1)
                print("action:", action)
                await asyncio.gather(cp.start(imei, action), cp.send_action_notification(imei, action))
                time.sleep(1)
            except queue.Empty:
                print("Queue is empty")
        print("jidsf")
        #     # # multiple_processes.q.put("None")

        #     # while action != "None" :
        #     #     print("in while loop")
        #     #     action = multiple_processes.q.get()
        #     #     multiple_processes.q.put("1")

            # action = ""

        await asyncio.gather(cp.start(imei, action), cp.send_action_notification(imei, action))

def wss_steve(imei):
    asyncio.run(main(imei))


def start_process(imei):
    threads = []
    thread1 = threading.Thread(target=mqtt_sub, args=(imei,))
    threads.append(thread1)
    thread2 = threading.Thread(target=wss_steve, args=(imei,))
    threads.append(thread2)

    for thread in threads:
        thread.start()

if __name__ == "__main__":
    
    # imei = ["866907056709164"]
    imei=["866907053293733"]
    # processes = []

    # for i in range(len(imei)):
    #     process = multiprocessing.Process(target=start_process, args=(imei[i]))
    #     processes.append(process)

    # for process in processes:
    #     process.start()

    # q = queue.Queue()

    start_process(imei[0])
    
