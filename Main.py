from ChargePoint import ChargePoint
import websockets
import asyncio
# from publisher_thor import Publisher
# from mqtt_sub import subscriber
# from subscriber_thor import subscriber


class runner: 
    async def main(self, imei, action):
        
        async with websockets.connect(
            "ws://13.234.76.186:8080/steve/websocket/CentralSystemService/" + imei, subprotocols=["ocpp1.6"]
        ) as ws:

            cp = ChargePoint(imei, ws)
        
            await asyncio.gather(cp.start(imei, action), cp.send_action_notification(imei, action))
        
            

    # if __name__ == "__main__":
    #     # asyncio.run() is used when running this example with Python >= 3.7v
        
    #     asyncio.run(main())

