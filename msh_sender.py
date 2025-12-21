import logging
import asyncio
import json
#import meshcore
from meshcore import MeshCore, EventType
import threading
from kafka import KafkaConsumer
from typing import Callable, Awaitable, Optional


class AsyncKafka:
    def __init__(
        self,
        topics: list[str],
        bootstrap_servers: str | list[str],
        group_id: str,
        *,
        queue_size: int = 1000,
        enable_auto_commit: bool = False,
        auto_offset_reset: str = "earliest",
        consumer_config: Optional[dict] = None,
    ):
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue = asyncio.Queue(maxsize=queue_size)
        self._running = threading.Event()
        self._running.set()

        config = {
            "bootstrap_servers": bootstrap_servers,
            "group_id": group_id,
            "enable_auto_commit": enable_auto_commit,
            "auto_offset_reset": auto_offset_reset,
        }
        if consumer_config:
            config.update(consumer_config)

        self._consumer = KafkaConsumer(*topics, **config)
        self._thread = threading.Thread(
            target=self._consumer_loop,
            daemon=True,
        )

    async def start(self):
        self._loop = asyncio.get_running_loop()
        self._thread.start()

    async def stop(self):
        self._running.clear()
        await asyncio.to_thread(self._consumer.close)

    async def consume(self, handler: Callable[[any], Awaitable[None]]):
        """
        handler(msg) -> async
        """
        while True:
            msg = await self._queue.get()
            try:
                await handler(msg)
            finally:
                self._queue.task_done()

    async def commit(self):
        await asyncio.to_thread(self._consumer.commit)

    def _consumer_loop(self):
        """
        Runs in separate thread
        """
        assert self._loop is not None

        for msg in self._consumer:
            if not self._running.is_set():
                break

            future = asyncio.run_coroutine_threadsafe(
                self._queue.put(msg),
                self._loop,
            )

            future.result()



class MessageSender:
    def __init__(self):
        self.running = True
        self.channel = 0xC8
        self.topic = "meshcore-text"
        self.kafka_broker = "kafka.syg:9092"
        self.group_id = "mc-text"

        self.kafka = AsyncKafka(
            topics=[self.topic],
            bootstrap_servers=self.kafka_broker,
            group_id=self.group_id,
            enable_auto_commit=False,
        )
    def init_logger(self, logger):
        self.logger = logger

    def stop(self):
        self.running = False

    def init_meshcore(self, mc: MeshCore):
        self.mc = mc 

    async def handle_kafka_message(self, msg):
        #print(msg.topic, msg.partition, msg.offset, msg.value)
        msg_request = json.loads(msg.value)

        self.logger.info(f"message to send: {msg_request}")
        channel = msg_request['channel']
        text = msg_request['text']

        if channel and text:
            result = await self.mc.commands.send_chan_msg(channel, text)

            if result.type == EventType.ERROR:
                self.logger.error(f"Error sending message: {result}")
            else:
                self.logger.info("Message sent")
        else:
            self.logger.warning("invalid text message")

        await self.kafka.commit()

        await asyncio.sleep(0.1)

    async def handle_sending_messsages(self):
        await self.kafka.start()

        consumer_task = asyncio.create_task(
            self.kafka.consume(self.handle_kafka_message)
        )
        while self.running:
            try:
                self.logger.info("handling kafka...")
                await consumer_task

            except asyncio.CancelledError:
                self.logger.info("cannceled")
                break
            except Exception as e:
                self.logger.error(f"Error: {e}")
                logging.exception("oops")
                break

        await self.kafka.stop()
        self.logger.info("end of task")
