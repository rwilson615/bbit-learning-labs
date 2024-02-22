import pika
import os

from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
	def __init__(self, routing_key:str, exchange_name: str) -> None:
		self.routing_key = routing_key
		self.exchange_name = exchange_name
		
		self.setupRMQConnection()

	def setupRMQConnection(self) -> None:
		con_params = pika.URLParameters(os.environ["AMQP_URL"])
		self.connection = pika.BlockingConnection(parameters=con_params)

	def publishOrder(self, message: str) -> None:
		channel = self.connection.channel()
		exchange = channel.exchange_declare(exchange=self.exchange_name)
		channel.basic_publish(
 		   exchange=self.exchange_name,
    		   routing_key=self.routing_key,
    		   body=message,
		)
