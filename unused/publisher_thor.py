import binascii
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import time as t
import json
import mysql.connector

# Define ENDPOINT, CLIENT_ID, PATH_TO_CERTIFICATE, PATH_TO_PRIVATE_KEY, PATH_TO_AMAZON_ROOT_CA_1, MESSAGE, TOPIC, and RANGE


# Spin up resources
class Publisher:
    ENDPOINT = "a4npr11hez19b-ats.iot.ap-south-1.amazonaws.com"
    CLIENT_ID = ""
    PATH_TO_CERTIFICATE = "cert.pem.crt"
    PATH_TO_PRIVATE_KEY = "privateKey.pem.key"
    PATH_TO_AMAZON_ROOT_CA_1 = "amazonRoot.pem"
    # TOPIC = ""
    # TOPIC = "864394040833701/data"
    RANGE = 1
    
    async def _start(self, imei, action):
        TOPIC = imei+"/commands"
        self.CLIENT_ID = imei
        conn = mysql.connector.connect(
                user='cms_admin', password='s2VuUS34wxWO18yQtbkz', host="steve-db-cms.cqry44wn7lp3.ap-south-1.rds.amazonaws.com", database='MqttWeb')

        cursor = conn.cursor()
        # cursor.execute('select * from publisher')
        # last_row = cursor.fetchall()
        # print(last_row[len(last_row)-1])
        sql = "select " + action + " from publisherOne where imei = %s"
        cursor.execute(sql, (imei,))
        record = cursor.fetchone()


        event_loop_group = io.EventLoopGroup(1)
        host_resolver = io.DefaultHostResolver(event_loop_group)
        client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
                    endpoint=self.ENDPOINT,
                    cert_filepath=self.PATH_TO_CERTIFICATE,
                    pri_key_filepath=self.PATH_TO_PRIVATE_KEY,
                    client_bootstrap=client_bootstrap,
                    ca_filepath=self.PATH_TO_AMAZON_ROOT_CA_1,
                    client_id=self.CLIENT_ID,
                    clean_session=False,
                    keep_alive_secs=6
                    )
        print("Connecting to {} with client ID '{}'...".format(
                self.ENDPOINT, self.CLIENT_ID))
        # Make the connect() call
        connect_future = mqtt_connection.connect()

        # Future.result() waits until a result is available
        connect_future.result()
        print("Connected!")
        # Publish message to server desired number of times.
        # print('Begin Publish')
        # for i in range (self.RANGE):
        # raw_message = last_row[len(last_row)-1][1]
        #####################################
        # raw_message = record[0]
        # raw_message = str(raw_message)
        # #
        # raw_message = bytes(raw_message, 'utf-8')
        # raw_message = binascii.hexlify(raw_message)
        # raw_message = str(raw_message)
        # raw_message = raw_message[2:len(raw_message)-1] + "*"
        # print(".........................................................")
        # print(raw_message)
        # # data = "{}".format(raw_message)
        # message = {"CMD": "raw_message"}

        # print(message)
        # print(json.dumps(message))
        # print("..........................................................")
        # # msg = {"CMD": "abckjhasdfpiuqherwfkjbdfiugrrkfjjqiourgf"}
        # mqtt_connection.publish(topic=TOPIC, payload=json.dumps(message), qos=mqtt.QoS.AT_LEAST_ONCE)
        # print("Published: '" + json.dumps(message) + "' to the topic: " + TOPIC)
        # t.sleep(0.1)
        # # print('Publish End')
        # disconnect_future = mqtt_connection.disconnect()
        # disconnect_future.result()
        ####################################
        flag = False
        raw_message = record[0]
        raw_message = str(raw_message)
        if 'Concur' in raw_message or 'Meter' in raw_message:
            flag = True

            print('\n\n\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ DEBUG START +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \n ')
            print(raw_message)
        raw_message = bytes(raw_message, 'utf-8')
        if flag: 
            print(raw_message)
        raw_message = binascii.hexlify(raw_message)
        if flag: 
            print(raw_message)
        raw_message = str(raw_message)
        if flag: 
            print(raw_message)
        raw_message = raw_message[2:len(raw_message)-1] + "*"
        if flag: 
            print(raw_message)
        message = {"CMD": raw_message}
        if flag: 
            print(raw_message)
            print('\n\n\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ DEBUG END +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \n ')
        mqtt_connection.publish(topic=TOPIC, payload=json.dumps(message), qos=mqtt.QoS.AT_LEAST_ONCE)
        print("Published: '" + json.dumps(message) + "' to the topic: " + TOPIC)
        t.sleep(2)
        print('Publish End')
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()