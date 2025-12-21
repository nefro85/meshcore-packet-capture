import logging
import asyncio
import json
#import meshcore
from meshcore import MeshCore, EventType
from kafka import KafkaConsumer



class MessageSender:
    def __init__(self):
        self.logger = logging.Logger("MessageSender")
        self.running = True
        self.channel = 0xC8
        self.topic = "meshcore-text"
        self.kafka_broker = "kafka.syg:9092"
        self.group_id = "mc-text"


    def stop(self):
        self.running = False

    def init_meshcore(self, mc: MeshCore):
        self.mc = mc

    async def handle_sending_messsages(self):
        while self.running:
            try:
                self.logger.debug("handling...")

                kcons = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.kafka_broker,
                    group_id=self.group_id
                    )
                self.logger.info("consumer running")
                for msg in kcons:
                    msg_request = json.loads(msg.value)

                    self.logger.info(f"message to send: {msg_request}")

                    result = await self.mc.commands.send_chan_msg(msg_request.channel, msg_request.text)

                    if result.type == EventType.ERROR:
                        self.logger.error(f"Error sending message: {result.payload}")
                    else:
                        self.logger.info("Message sent")
                    if not self.running:
                        break        

            except asyncio.CancelledError:
                self.logger.info("cannceled")
                break
            except Exception as e:
                self.logger.error(f"Error: {e}")
                break
            finally:
                self.logger.info("end of task")
