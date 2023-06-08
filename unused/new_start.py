import asyncio
import logging
import websockets
import time
import mysql.connector
from ChargePoint import ChargePoint

from subscriber2 import subscriber

logging.basicConfig(level=logging.INFO)

# async def run(imei):
#     sub = subscriber()
#     sub._start(imei)

# async def wss_connect(imei, action):
#     async with websockets.connect(
#         "ws://13.234.76.186:8080/steve/websocket/CentralSystemService/" + imei, subprotocols=["ocpp1.6"]
#     ) as ws:

#         cp = ChargePoint(imei, ws)

#         await asyncio.gather(cp.start(imei, action), cp.send_action_notification(imei, action))

async def main(imei):
    async with websockets.connect(
            "ws://13.234.76.186:8080/steve/websocket/CentralSystemService/" + imei, subprotocols=["ocpp1.6"]
        ) as ws:

            cp = ChargePoint(imei, ws)

            # sub = subscriber()
            # sub._start(imei, ws)
            
            action = "a"

            while action == "a":
                time.sleep(0.1)

                await asyncio.gather(cp.start(imei, action), cp.send_action_notification(imei, action))

if __name__ == "__main__":
    # sub = subscriber()
    # asyncio.run(sub._start("866907056709165"))
    asyncio.run(main("866907056709165"))

