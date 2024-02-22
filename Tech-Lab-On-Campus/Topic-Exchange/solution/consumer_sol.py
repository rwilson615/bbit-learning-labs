import pika
import os

from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(
        self, exchange_name: str, queue_name: str, topics: list[str]
    ) -> None:
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.topics = topics

        self.setupRMQConnection()
        
    def setupRMQConnection(self):
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        self.channel = self.connection.channel()

        exchange = self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="topic")
        self.channel.queue_declare(queue=self.queue_name)
        for topic in self.topics:
            self.channel.queue_bind(
                queue= self.queue_name,
                routing_key= topic,
                exchange=self.exchange_name,
            )
        self.channel.basic_consume(
            self.queue_name, self.on_message_callback, auto_ack=False
        )

    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        channel.basic_ack(method_frame.delivery_tag, False)

        print(body)

        self.channel.close()
        self.connection.close()

    def startConsuming(self) -> None:
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()
        

