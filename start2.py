# Import package
import re
import paho.mqtt.client as mqtt
import ssl
import json
import mysql.connector
from Main import runner
import asyncio
import time
import threading

class subscriber:
    
    def _start(self, imei):   
        
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
        
        def extract_between_strings(start, end, text):
            pattern = re.escape(start) + r'(.*?)' + re.escape(end)
            match = re.search(pattern, text)
            if match:
                return start + match.group(1) + end
            else:
                return None

        # Define on_message event function. 
        # This function will be invoked every time,
        # a new message arrives for the subscribed topic 
        lock = threading.Lock()
        def on_message(mosq, obj, msg):
            # print ("Topic: " + str(msg.topic))
            # print ("QoS: " + str(msg.qos))
            # print ("Payload: " + str(msg.payload))
            #..................................................
            # mycursor = mydb.cursor()
            # n = str(msg.payload.decode())
            # id = 1
            # sql = "INSERT INTO subscriber (id, data) VALUES ('%s')"
            # # val = n
            # mycursor.execute("INSERT INTO subscriber (id, data) VALUES (%s, %s)", (id, n,))
            #..................................................

            

            lock.acquire()
            print("on message")
            mycursor = mydb.cursor()
            # print(msg.payload.decode())
            # a = str(msg.payload.decode())

            # dict = json.loads(a)

            a = msg.payload

            print(a)            
            

            if b'"RSP"' in a:

                # message = dict.get("RSP")
                if a[8:9] == b'[':

                    # n = len(a)

                    # for_rsp = -n+9-1
                    # for_ts = -6
                    # b = a[for_rsp:
                    # for_ts]
                    # c = b.decode()
                    # print("decoded: "+c)
                    # message = json.loads(b)
                    # action = message[2]

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


                    
                    # list = eval(message)
                    id = imei
                    # action = list[2]
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
                    # print("Payload: " + str(.payload.decode()))
                    run = runner()
                    
                    asyncio.run(run.main(id, action))

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