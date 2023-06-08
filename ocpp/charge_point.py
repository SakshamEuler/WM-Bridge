import asyncio
import binascii
import inspect
import logging
import re
import time
import uuid
from dataclasses import asdict
from typing import Dict, List, Union
import json
import mysql.connector

from ocpp.exceptions import NotSupportedError, OCPPError
from ocpp.messages import Call, MessageType, unpack, validate_payload
from ocpp.routing import create_route_map

from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import time as t
import json
import mysql.connector

LOGGER = logging.getLogger("ocpp")

mydb = mysql.connector.connect(
        host="localhost",
        user='root',
        password='root',
        database="MqttWeb"
        )


def camel_to_snake_case(data):
    """
    Convert all keys of all dictionaries inside the given argument from
    camelCase to snake_case.

    Inspired by: https://stackoverflow.com/a/1176023/1073222

    """
    if isinstance(data, dict):
        snake_case_dict = {}
        for key, value in data.items():
            s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", key)
            key = re.sub("([a-z0-9])([A-Z])(?=\\S)", r"\1_\2", s1).lower()

            snake_case_dict[key] = camel_to_snake_case(value)

        return snake_case_dict

    if isinstance(data, list):
        snake_case_list = []
        for value in data:
            snake_case_list.append(camel_to_snake_case(value))

        return snake_case_list

    return data


def snake_to_camel_case(data):
    """
    Convert all keys of a all dictionaries inside given argument from
    snake_case to camelCase.

    Inspired by: https://stackoverflow.com/a/19053800/1073222
    """
    if isinstance(data, dict):
        camel_case_dict = {}
        for key, value in data.items():
            key = key.replace("soc", "SoC")
            components = key.split("_")
            key = components[0] + "".join(x[:1].upper() + x[1:] for x in components[1:])
            camel_case_dict[key] = snake_to_camel_case(value)

        return camel_case_dict

    if isinstance(data, list):
        camel_case_list = []
        for value in data:
            camel_case_list.append(snake_to_camel_case(value))

        return camel_case_list

    return data


def remove_nones(data: Union[List, Dict]) -> Union[List, Dict]:
    if isinstance(data, dict):
        return {k: remove_nones(v) for k, v in data.items() if v is not None}

    elif isinstance(data, list):
        return [remove_nones(v) for v in data if v is not None]

    return data


class ChargePoint:
    """
    Base Element containing all the necessary OCPP1.6J messages for messages
    initiated and received by the Central System
    """
    imei_id = ""
    def __init__(self, id, connection, response_timeout=30):
        """

        Args:

            charger_id (str): ID of the charger.
            connection: Connection to CP.
            response_timeout (int): When no response on a request is received
                within this interval, a asyncio.TimeoutError is raised.

        """
        self.id = id

        # The maximum time in seconds it may take for a CP to respond to a
        # CALL. An asyncio.TimeoutError will be raised if this limit has been
        # exceeded.
        self._response_timeout = response_timeout

        # A connection to the client. Currently this is an instance of gh
        self._connection = connection

        # A dictionary that hooks for Actions. So if the CS receives a it will
        # look up the Action into this map and execute the corresponding hooks
        # if exists.
        self.route_map = create_route_map(self)

        self._call_lock = asyncio.Lock()

        # A queue used to pass CallResults and CallErrors from
        # the self.serve() task to the self.call() task.
        self._response_queue = asyncio.Queue()

        # Function used to generate unique ids for CALLs. By default
        # uuid.uuid4() is used, but it can be changed. This is meant primarily
        # for testing purposes to have predictable unique ids.
        self._unique_id_generator = "fksdfksdjfskj"

    async def start(self, imei, action):
        # while True:
            self.imei_id = imei
            message = await self._connection.recv()
            LOGGER.info("%s: receive %s", self.id, message)
            mycursor = mydb.cursor()
            id = imei
            # mycursor.execute("INSERT INTO publisher (id, data) VALUES (%s, %s)", (id, message))

            sql = "UPDATE publisherOne SET " + action + " = %s WHERE imei = %s"
            val = (message, id)
            mycursor.execute(sql, val)

            
            mydb.commit()

            # pub = Publisher()
            # await pub._start(imei,action)

            #############################################
            if action == "a":
                pass
            else:
                ENDPOINT = "a4npr11hez19b-ats.iot.ap-south-1.amazonaws.com"
                CLIENT_ID = "testDevice"
                PATH_TO_CERTIFICATE = "cert.pem.crt"
                PATH_TO_PRIVATE_KEY = "privateKey.pem.key"
                PATH_TO_AMAZON_ROOT_CA_1 = "amazonRoot.pem"
                TOPIC = imei+"/totcu"
                
                # conn = mysql.connector.connect(user='root', password='root', host="localhost", database='MqttWeb')

                # cursor = conn.cursor()
                # sql = "select " + action + " from publisherOne where imei = %s"
                # cursor.execute(sql, (imei,))
                # record = cursor.fetchone()

                event_loop_group = io.EventLoopGroup(1)
                host_resolver = io.DefaultHostResolver(event_loop_group)
                client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
                mqtt_connection = mqtt_connection_builder.mtls_from_path(
                            endpoint=ENDPOINT,
                            cert_filepath=PATH_TO_CERTIFICATE,
                            pri_key_filepath=PATH_TO_PRIVATE_KEY,
                            client_bootstrap=client_bootstrap,
                            ca_filepath=PATH_TO_AMAZON_ROOT_CA_1,
                            client_id=CLIENT_ID,
                            clean_session=False,
                            keep_alive_secs=6
                            )
                print("Connecting to {} with client ID '{}'...".format(
                        ENDPOINT, CLIENT_ID))
                connect_future = mqtt_connection.connect()
                connect_future.result()
                print("Connected!")
                print('Begin Publish')

                # raw_message = record[0]
                raw_message = message
                raw_message = str(raw_message)
                
                # only for start transaction
                if(raw_message.find("transactionId")!=-1):
                    raw_message = raw_message[:85] + raw_message[125:]

                raw_message = bytes(raw_message, 'utf-8')
                raw_message = binascii.hexlify(raw_message)
                raw_message = str(raw_message)
                raw_message = raw_message[2:len(raw_message)-1] + "*"
                test = "ugfiygkfjhgaskdjhfvasd"
                msg = {"CMD": raw_message}
                mqtt_connection.publish(topic=TOPIC, payload=json.dumps(msg), qos=mqtt.QoS.AT_LEAST_ONCE)
                print("Published: '" + json.dumps(msg) + "' to the topic: " + TOPIC)

                t.sleep(2)
                print('Publish End')
                disconnect_future = mqtt_connection.disconnect()
                disconnect_future.result()
            #############################################

            
            await self.route_message(message)
            # break
            print("out of while true")
            # return "return from start"

    async def route_message(self, raw_msg):
        # print("inside route_message")
        """
        Route a message received from a CP.

        If the message is a of type Call the corresponding hooks are executed.
        If the message is of type CallResult or CallError the message is passed
        to the call() function via the response_queue.
        """
        try:
            msg = unpack(raw_msg)
        except OCPPError as e:
            LOGGER.exception(
                "Unable to parse message: '%s', it doesn't seem "
                "to be valid OCPP: %s",
                raw_msg,
                e,
            )
            return

        if msg.message_type_id == MessageType.Call:
            try:
                print("messagetype=call")
                await self._handle_call(msg)
            except OCPPError as error:
                LOGGER.exception("Error while handling request '%s'", msg)
                response = msg.create_call_error(error).to_json()
                await self._send(response)

        elif msg.message_type_id in [MessageType.CallResult, MessageType.CallError]:
            # print("inside call resutl")
            self._response_queue.put_nowait(msg)

    async def _handle_call(self, msg):
        print("inside handle call start")
        """
        Execute all hooks installed for based on the Action of the message.

        First the '_on_action' hook is executed and its response is returned to
        the client. If there is no '_on_action' hook for Action in the message
        a CallError with a NotImplemtendError is returned.

        Next the '_after_action' hook is executed.

        """
        try:
            print("inside handlers = self.route_map[msg.action]")
            handlers = self.route_map[msg.action]
        except KeyError:
            raise NotSupportedError(
                details={"cause": f"No handler for {msg.action} registered."}
            )

        if not handlers.get("_skip_schema_validation", False):
            print("inside handlers.get, valideate_payload")
            validate_payload(msg, self._ocpp_version)
        # OCPP uses camelCase for the keys in the payload. It's more pythonic
        # to use snake_case for keyword arguments. Therefore the keys must be
        # 'translated'. Some examples:
        #
        # * chargePointVendor becomes charge_point_vendor
        # * firmwareVersion becomes firmwareVersion
        snake_case_payload = camel_to_snake_case(msg.payload)

        try:
            handler = handlers["_on_action"]
        except KeyError:
            raise NotSupportedError(
                details={"cause": f"No handler for {msg.action} registered."}
            )

        try:
            response = handler(**snake_case_payload)
            if inspect.isawaitable(response):
                print("inside inspect")
                response = await response
                print("inside post inspect")
        except Exception as e:
            LOGGER.exception("Error while handling request '%s'", msg)
            response = msg.create_call_error(e).to_json()
            await self._send(response)

            return

        temp_response_payload = asdict(response)

        # Remove nones ensures that we strip out optional arguments
        # which were not set and have a default value of None
        response_payload = remove_nones(temp_response_payload)

        # The response payload must be 'translated' from snake_case to
        # camelCase. So:
        #
        # * charge_point_vendor becomes chargePointVendor
        # * firmware_version becomes firmwareVersion
        camel_case_payload = snake_to_camel_case(response_payload)

        response = msg.create_call_result(camel_case_payload)

        if not handlers.get("_skip_schema_validation", False):
            print("skip_schema_validation")
            validate_payload(response, self._ocpp_version)

        await self._send(response.to_json())


        try:
            handler = handlers["_after_action"]
            # Create task to avoid blocking when making a call inside the
            # after handler
            response = handler(**snake_case_payload)
            if inspect.isawaitable(response):
                asyncio.ensure_future(response)
        except KeyError:
            # '_on_after' hooks are not required. Therefore ignore exception
            # when no '_on_after' hook is installed.
            pass
        print("end of handle call")

    def getUniqueId(self, imei, action):
        sql_querry = "select " + action + " from subscriberOne where imei = %s"
        
        cursor = mydb.cursor()
        cursor.execute(sql_querry, (imei,))
        record = cursor.fetchone()
        currData = record[0]
        list = eval(currData)
        return list[1]

    async def call(self, payload, action, suppress=True):
        print("inside call function")
        """
        Send Call message to client and return payload of response.

        The given payload is transformed into a Call object by looking at the
        type of the payload. A payload of type BootNotificationPayload will
        turn in a Call with Action BootNotification, a HeartbeatPayload will
        result in a Call with Action Heartbeat etc.

        A timeout is raised when no response has arrived before expiring of
        the configured timeout.

        When waiting for a response no other Call message can be send. So this
        function will wait before response arrives or response timeout has
        expired. This is in line the OCPP specification

        Suppress is used to maintain backwards compatibility. When set to True,
        if response is a CallError, then this call will be suppressed. When
        set to False, an exception will be raised for users to handle this
        CallError.

        """
        camel_case_payload = snake_to_camel_case(asdict(payload))
        # print(str(camel_case_payload) +'\n')
        # print(payload)
        call = Call(
            unique_id=str(self.getUniqueId(self.imei_id, action)),
            action=payload.__class__.__name__[:-7],
            payload=remove_nones(camel_case_payload),
        )

        validate_payload(call, self._ocpp_version)

        

        # Use a lock to prevent make sure that only 1 message can be send at a
        # a time.
        async with self._call_lock:
            await self._send(call.to_json())
            try:
                response = await self._get_specific_response(
                    call.unique_id, self._response_timeout
                )
            except asyncio.TimeoutError:
                raise asyncio.TimeoutError(
                    f"Waited {self._response_timeout}s for response on "
                    f"{call.to_json()}."
                )

        if response.message_type_id == MessageType.CallError:
            LOGGER.warning("Received a CALLError: %s'", response)
            if suppress:
                return
            raise response.to_exception()
        else:
            response.action = call.action
            validate_payload(response, self._ocpp_version)

        snake_case_payload = camel_to_snake_case(response.payload)
        # Create the correct Payload instance based on the received payload. If
        # this method is called with a call.BootNotificationPayload, then it
        # will create a call_result.BootNotificationPayload. If this method is
        # called with a call.HeartbeatPayload, then it will create a
        # call_result.HeartbeatPayload etc.
        cls = getattr(self._call_result, payload.__class__.__name__)  # noqa
        return cls(**snake_case_payload)

    async def _get_specific_response(self, unique_id, timeout):
        print("inside get specific response")
        """
        Return response with given unique ID or raise an asyncio.TimeoutError.
        """
        wait_until = time.time() + timeout
        try:
            # Wait for response of the Call message.
            response = await asyncio.wait_for(self._response_queue.get(), timeout)
            # print("at 346 await completed")
        except asyncio.TimeoutError:
            raise

        if response.unique_id == unique_id:
            return response

        LOGGER.error("Ignoring response with unknown unique id: %s", response)
        timeout_left = wait_until - time.time()

        if timeout_left < 0:
            raise asyncio.TimeoutError

        return await self._get_specific_response(unique_id, timeout_left)

    async def _send(self, message):
        LOGGER.info("%s: send %s", self.id, message)
        print(message + "::::::::")
        await self._connection.send(message)

    async def recv_from_steve(self):
        response = await self._connection.recv()
        print(response)
