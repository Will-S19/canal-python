import datetime
import json
import logging
import time
from django.conf import settings
from redis import Redis
from django.utils import timezone
from pika.exceptions import AMQPConnectionError, AMQPChannelError, ConnectionClosedByBroker
from pika import BasicProperties
from threading import Thread

redis = Redis.from_url(settings.REDIS_URL)

from mq.mq_client import AmqpClient

logger = logging.getLogger(__name__)

pipe_logger = logging.getLogger('pipeline')

TTL_MESSAGE_MODE = 0
TTL_QUEUE_MODE = 1


def set_value_to_redis(body):
    json_msg = json.loads(body)
    print(json_msg)
    redis.hmset('book', {'name2': "小王", 'name3': '小李'})


class DelayTaskManager(AmqpClient):
    name = 'default-name'
    topic = name

    def __init__(self, host, port, user, pwd, vhost, mode=TTL_MESSAGE_MODE, **kwargs):
        super().__init__(host, port, user, pwd, vhost)
        self._name = self.name
        self.queue_name = self._name

        self.delay_time = 0
        self.msg_properties = None
        self.mode = mode
        self.init_conn()
        logger.info(f"create MQ worker: {self.topic}, delay_time: {self.delay_time}")

    @classmethod
    def from_setting(cls):
        host = settings.RABBIT_MQ_HOST
        vhost = None
        mode = TTL_QUEUE_MODE
        if host.startswith('amqp'):
            vhost = settings.RABBIT_MQ_VIRTUAL_HOST
            mode = TTL_MESSAGE_MODE
        return cls(
            host=host,
            port=settings.RABBIT_MQ_PORT,
            user=settings.RABBIT_MQ_USERNAME,
            pwd=settings.RABBIT_MQ_PASSWORD,
            vhost=vhost,
            mode=mode
        )

    @staticmethod
    def process_message(body):
        ...

    def on_message(self, ch, method, properties, body):
        logger.info(f"processor: {self.name} receive message: {body} with properties: {properties}")
        try:
            self.process_message(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f'processor: {self.name} error with message: {body} for {e}')
            ch.basic_reject(delivery_tag=method.delivery_tag)

    def run(self):
        if self.mode == TTL_QUEUE_MODE:
            self.channel.basic_consume(self.queue_name, self.on_message, auto_ack=False)
        else:
            self.channel.basic_consume(self._name, self.on_message, auto_ack=False)

        self.channel.start_consuming()

    def recovery_run(self):
        while 1:
            try:
                logger.info(f"{self.topic} Connecting...")
                self.init_conn()

                self.channel.basic_qos(prefetch_count=1)

                logger.info(f"{self.topic} connected and starting consume...")
                try:
                    self.run()
                except SystemExit:
                    self.channel.stop_consuming()
                    self.conn.close()
                    break
            except ConnectionClosedByBroker:
                # Uncomment this to make the example not attempt recovery
                # from server-initiated connection closure, including
                # when the node is stopped cleanly
                #
                # break
                continue
            # Do not recover on channel errors
            except AMQPChannelError as err:
                logger.error(f"{self.topic} Caught a channel error: {err}, stopping...")
                break
            # Recover on all other connection errors
            except AMQPConnectionError:
                logger.error(f"{self.topic} Connection was closed, retrying...")
                continue

    def as_thread(self):
        t = Thread(target=self.recovery_run)
        t.setDaemon(False)
        return t


class AutoSetValueToRedis(DelayTaskManager):
    name = 'canal'
    topic = 'canal-routing-key'

    def process_message(self, body):
        print(body)
        set_value_to_redis(body)
