
import multiprocessing
from ChargePoint import ChargePoint
import websockets
import mysql.connector

import threading
import time
import asyncio
import queue
import mysql.connector
import json
import paho.mqtt.client as mqtt
import ssl
import os
import sys

# q = queue.Queue()

def mqtt_sub(imei,q):
    print(f"in mqtt sub {imei}")

    mydb = mysql.connector.connect(
        host="steve-db-cms.cqry44wn7lp3.ap-south-1.rds.amazonaws.com",
        user='cms_admin',
        password='s2VuUS34wxWO18yQtbkz',
        database="MqttWeb"
    )
    print("db connect inside subscriber")
    # Define Variables
    MQTT_PORT = 8883
    MQTT_KEEPALIVE_INTERVAL = 45
    MQTT_TOPIC = imei+"/data"

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

        
        # print("on message")
        mycursor = mydb.cursor()

        a = msg.payload

                   
        

        try:

            if b'"RSP"' in a:
                print(a) 
                lock.acquire()

                # message = dict.get("RSP")
                if a[8:9] == b'[':

                    c = b''
                    if (b'MeterValues' in a):
                        flag = False
                        for byte in a:
                            byte_as_int = int(byte)
                            byte_as_byte = bytes([byte_as_int])
                            if (byte_as_byte == b'['):
                                flag = True
                            if (byte_as_byte == b'\\'):
                                flag = False
                                # c += byte_as_byte
                            if (flag):
                                c += byte_as_byte
                    else:
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
                    # print("..............................................")
                    # print(message)
                    # print(type(message))
                    # print(action)
                    # print("..............................................")
                    val = (message, id)
                    mycursor.execute(sql, val)
                    mydb.commit()
                    
                    q.put(action)
                    print(f"put {action} in queue")

                    time.sleep(2)
                lock.release()
        except Exception as err:
            lock.release()
            print(err)
            

        
        # time.sleep(7)


    def on_subscribe(mosq, obj, mid, granted_qos):
        print("Subscribed to Topic: " + 
        MQTT_TOPIC + " with QoS: " + str(granted_qos))

    # Initiate MQTT Client
    mqttc = mqtt.Client()

    # Assign event callbacks
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_subscribe = on_subscribe

    mqttc.tls_set(CA_ROOT_CERT_FILE, certfile=THING_CERT_FILE, keyfile=THING_PRIVATE_KEY, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)

    mqttc.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)

    mqttc.loop_forever() 

async def main(imei,q):
    try:
        print(f"in the wss function {imei}")        
        while True:
            
            a = q.get()
            if (len(a) >= 9):
                q.put(a)
                async with websockets.connect(
                    "ws://13.234.76.186:8080/steve/websocket/CentralSystemService/" + imei, subprotocols=["ocpp1.6"]
                    # "ws://127.0.0.1:8080/steve/websocket/CentralSystemService/" + imei, subprotocols=["ocpp1.6"]
                ) as ws:

                    # while True:
                    print(f"wss connection established {imei}")
                
                    cp = ChargePoint(imei, ws)

                    # action = multiple_processes.q.get()
                    startTime = time.perf_counter()
                    timeoutInterval = 60

                    while (time.perf_counter() - startTime) < timeoutInterval:
                    # for i in range(10):
                        try:
                            action = q.get(timeout=1)
                            print("action:", action)
                            await asyncio.gather(cp.start(imei, action), cp.send_action_notification(imei, action))
                            startTime = time.perf_counter()
                            time.sleep(1)
                        except queue.Empty:
                            pass

                print(f"wss connection closed: {imei}")  
    except Exception as err:
        print(f'err:  {err}')
        os.execl(sys.executable, sys.executable, *sys.argv)

def wss_steve(imei,q):
    asyncio.run(main(imei,q))

def start_process(imei,q):
    threads = []
    thread1 = threading.Thread(target=mqtt_sub, args=(imei,q,))
    threads.append(thread1)
    thread2 = threading.Thread(target=wss_steve, args=(imei,q,))
    threads.append(thread2)

    for thread in threads:
        thread.start()

if __name__ == "__main__":
        
    # multiprocessing.set_start_method('spawn', force=True)
    # imei = ["866907056709164"]
    # imei=["866907053293733","866907056709164"]
    # imei = ["864394040833702", "864394040833703"]

    mydb = mysql.connector.connect(
        host="steve-db-cms.cqry44wn7lp3.ap-south-1.rds.amazonaws.com",
        user='cms_admin',
        password='s2VuUS34wxWO18yQtbkz',
        database="MqttWeb"
    )

    cursor = mydb.cursor()
    sql = "select imei from subscriberOne"
    cursor.execute(sql)
    imei = cursor.fetchall()
    print(imei)

    processes = []

    for i in range(len(imei)):
        q = multiprocessing.Queue()
        # q = queue.Queue()
        process = multiprocessing.Process(target=start_process, args=(imei[i][0],q,))
        processes.append(process)
        # process.start()

    for process in processes:
        process.start()

