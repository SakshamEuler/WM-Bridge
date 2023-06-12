import asyncio
import logging
import websockets
import json
from ocpp.v16 import ChargePoint as cp
from ocpp.v16 import call

import mysql.connector

logging.basicConfig(level=logging.INFO)

import mysql.connector


# dict = list[3]


class ChargePoint(cp):

    def actionDef(self, list, action):
        dict = list[3]
        print(dict)
        if (action == "BootNotification"):
            return call.BootNotificationPayload(
                charge_point_model=dict.get("chargePointModel"), charge_point_vendor=dict.get("chargePointVendor")
            )
        elif action == "Heartbeat":
            return call.HeartbeatPayload()
        elif action == "DataTransfer":
            return call.DataTransferPayload(
                vendor_id = dict.get("vendorId"), message_id = dict.get("messageId"), data = dict.get("data")
            )       
        elif action == "Authorize":
            return call.AuthorizePayload(
                id_tag = dict.get("idTag")
            )
        elif action == "StatusNotification":
            return call.StatusNotificationPayload(
                connector_id=dict.get("connectorId"), error_code=dict.get("errorCode"), status=dict.get("status")
            )
        elif action == "DiagnosticsStatusNotification":
            return call.DiagnosticsStatusNotificationPayload(
                status = dict.get("status")
                # , uploadStatus=dict.get("uploadStatus")
            )
        elif action == "FirmwareStatusNotification":
            return call.FirmwareStatusNotificationPayload(
                status = dict.get("status")
            )
        elif action == "MeterValues":
            print(dict.get("meterValue"))
            return call.MeterValuesPayload(
                connector_id = dict.get("connectorId"), meter_value = dict.get("meterValue"), transaction_id = dict.get("transsactionId")
            )
        elif action == "StartTransaction":
            return call.StartTransactionPayload(
                connector_id=dict.get("connectorId"), id_tag=dict.get("idTag"), meter_start=dict.get("meterStart"), reservation_id=dict.get("reservationId"), timestamp=dict.get("timestamp")
            )
        elif action == "StopTransaction":
            return call.StopTransactionPayload(
                id_tag=dict.get("idTag"), meter_stop=dict.get("meterStop"), timestamp=dict.get("timestamp"), transaction_id=dict.get("transactionId")
            )
        else:
            pass
            print("No appropriate action received...")

    def getDataFromDb(self, imei, action):
        conn = mysql.connector.connect(
        user='cms_admin', password='s2VuUS34wxWO18yQtbkz', host="steve-db-cms.cqry44wn7lp3.ap-south-1.rds.amazonaws.com", database='MqttWeb')

        sql_querry = "select " + action + " from subscriberOne where imei = %s"
        
        cursor = conn.cursor()
        cursor.execute(sql_querry, (imei,))
        record = cursor.fetchone()

        currData = record[0]
        list = eval(currData)
        # print("sdfkshjdfksj")
        # dict = list[3]
        # print(dict)
        return list

    ## single function for all action notifications 
    async def send_action_notification(self, imei, action):
        list = self.getDataFromDb(imei, action)
        action = list[2]
        request = self.actionDef(list, action)

        response = await self.call(request, action)
        print("response -> " + str(response))


