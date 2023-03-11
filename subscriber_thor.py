# Import package
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
        host="localhost",
        user='root',
        password='root',
        database="MqttWeb"
        )
        print("db connect inside subscriber file")
        # Define Variables
        MQTT_PORT = 8883
        MQTT_KEEPALIVE_INTERVAL = 45
        # MQTT_TOPIC = "864394040833701/fromtcu"
        MQTT_TOPIC = imei+"/fromtcu"
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
            a = str(msg.payload.decode())

            dict = json.loads(a)

            print(msg.payload.decode())            
            
            if (dict.get("RSP") != None ):

                message = dict.get("RSP")
                if message[0] == '[':
                    list = eval(message)
                    id = imei
                    action = list[2]
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