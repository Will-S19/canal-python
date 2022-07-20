import logging
import pika

logger = logging.getLogger(__name__)


class AmqpClient:

    def __init__(self, host, port, username, password, vhost=None):
        self.credentials = pika.PlainCredentials(username, password)
        self.host = host
        self.port = port
        self.vhost = vhost
        self.params = pika.ConnectionParameters(host=self.host,
                                                port=self.port,
                                                # virtual_host=vhost,
                                                credentials=self.credentials)
        if vhost:
            self.params._virtual_host = vhost
            self.params.virtual_host = vhost

        self.conn = None
        self.channel = None

    def init_conn(self):
        if not self.conn or not self.conn.is_open:
            self.conn = pika.BlockingConnection(self.params)
        if not self.channel or not self.channel.is_open:
            self.channel = self.conn.channel()



    def publish_msg(self, queue_name, msg, routing_key=None, properties=None, **kwargs):
        logger.info(f"publish to {queue_name} with routing_key: {routing_key}, with properties: {properties}, msg: {msg}")
        self.channel.basic_publish(exchange=queue_name, routing_key=routing_key, body=msg, properties=properties, **kwargs)
