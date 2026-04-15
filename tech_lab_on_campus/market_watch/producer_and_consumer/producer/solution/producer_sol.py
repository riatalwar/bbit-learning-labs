import pika
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):

    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name

        self.setupRMQConnection()


    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service

        # con_params = pika.URLParameters(os.environ["AMQP_URL"]) # Maybe doesn't exist in ENV VAR (?)
        con_params = pika.URLParameters('amqp://rabbitmq?connection_attempts=5&retry_delay=5')
        self.connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel

        self.channel = self.connection.channel()

        # Create the exchange if not already present

        exchange = self.channel.exchange_declare(exchange=self.exchange_name)

    def publishOrder(self, message: str) -> None:

        # Basic Publish to Exchange

        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message,
        )

        # Close Channel
        self.channel.close()

        # Close Connection
        self.connection.close()

    