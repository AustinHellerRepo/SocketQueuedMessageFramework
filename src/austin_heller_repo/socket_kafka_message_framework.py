from __future__ import annotations
from abc import ABC, abstractmethod
from austin_heller_repo.common import StringEnum, HostPointer
from austin_heller_repo.socket import ClientSocketFactory, ClientSocket, ServerSocketFactory, ServerSocket
from austin_heller_repo.kafka_manager import KafkaManagerFactory, KafkaManager, KafkaAsyncWriter
from austin_heller_repo.threading import AsyncHandle, Semaphore
from typing import List, Tuple, Dict, Type
import json
from datetime import datetime
import uuid


class ClientServerMessageTypeEnum(StringEnum):
	pass


class ClientServerMessage(ABC):

	def __init__(self):

		pass

	@abstractmethod
	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		raise NotImplementedError()

	@abstractmethod
	def to_json(self) -> Dict:
		return {
			"__type": self.get_client_server_message_type().value
		}

	@staticmethod
	@abstractmethod
	def parse_from_json(*, json_object: Dict):
		raise NotImplementedError()


class ClientMessenger():

	def __init__(self, *, client_socket_factory: ClientSocketFactory, server_host_pointer: HostPointer, client_server_message_class: Type[ClientServerMessage]):

		self.__client_socket_factory = client_socket_factory
		self.__server_host_pointer = server_host_pointer
		self.__client_server_message_class = client_server_message_class

		self.__client_socket = None  # type: ClientSocket

	def send_to_server(self, *, request_client_server_message: ClientServerMessage) -> ClientServerMessage:
		if self.__client_socket is None:
			self.__client_socket = self.__client_socket_factory.get_client_socket()
			self.__client_socket.connect_to_server(
				ip_address=self.__server_host_pointer.get_host_address(),
				port=self.__server_host_pointer.get_host_port()
			)
		self.__client_socket.write(json.dumps(request_client_server_message.to_json()))
		response_message = self.__client_server_message_class.parse_from_json(
			json_object=json.loads(self.__client_socket.read())
		)
		return response_message


class ClientMessengerFactory():

	def __init__(self, *, client_socket_factory: ClientSocketFactory, server_host_pointer: HostPointer, client_server_message_class: Type[ClientServerMessage]):

		self.__client_socket_factory = client_socket_factory
		self.__server_host_pointer = server_host_pointer
		self.__client_server_message_class = client_server_message_class

	def get_client_messenger(self) -> ClientMessenger:
		return ClientMessenger(
			client_socket_factory=self.__client_socket_factory,
			server_host_pointer=self.__server_host_pointer,
			client_server_message_class=self.__client_server_message_class
		)


class KafkaMessage():

	def __init__(self, *, source_uuid: str, client_server_message: ClientServerMessage, create_datetime: datetime, message_uuid: str):

		self.__source_uuid = source_uuid
		self.__client_server_message = client_server_message
		self.__create_datetime = create_datetime
		self.__message_uuid = message_uuid

	def get_source_uuid(self) -> str:
		return self.__source_uuid

	def get_client_server_message(self) -> ClientServerMessage:
		return self.__client_server_message

	def get_create_datetime(self) -> datetime:
		return self.__create_datetime

	def get_message_uuid(self) -> str:
		return self.__message_uuid

	def to_json(self) -> Dict:
		return {
			"source_uuid": self.get_source_uuid(),
			"message": self.get_client_server_message().to_json(),
			"create_datetime": self.get_create_datetime().strftime("%Y-%m-%d %H:%M:%S.%f"),
			"message_uuid": self.get_message_uuid()
		}

	def parse_from_json(self, *, json_object: Dict) -> KafkaMessage:
		return KafkaMessage(
			source_uuid=json_object["source_uuid"],
			client_server_message=ClientServerMessage.parse_from_json(
				json_object=json_object["message"]
			),
			create_datetime=datetime.strptime(json_object["create_datetime"], "%Y-%m-%d %H:%M:%S.%f"),
			message_uuid=self.get_message_uuid()
		)


class ServerMessenger():

	def __init__(self, *, server_socket_factory: ServerSocketFactory, kafka_manager_factory: KafkaManagerFactory, local_host_pointer: HostPointer, kafka_topic_name: str, client_server_message_class: Type[ClientServerMessage]):

		self.__server_socket_factory = server_socket_factory
		self.__kafka_manager_factory = kafka_manager_factory
		self.__local_host_pointer = local_host_pointer
		self.__kafka_topic_name = kafka_topic_name
		self.__client_server_message_class = client_server_message_class

		self.__server_socket = None  # type: ServerSocket
		self.__kafka_manager = None  # type: KafkaManager
		self.__is_receiving_from_clients = False
		self.__client_sockets = []  # type: List[ClientSocket]
		self.__client_sockets_semaphore = Semaphore()

	def __on_accepted_client_method(self, client_socket: ClientSocket):

		self.__client_sockets_semaphore.acquire()
		self.__client_sockets.append(client_socket)
		self.__client_sockets_semaphore.release()

		if self.__kafka_manager is None:
			self.__kafka_manager = self.__kafka_manager_factory.get_kafka_manager()
		source_uuid = str(uuid.uuid4())
		kafka_writer = self.__kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter
		while self.__is_receiving_from_clients:
			client_server_message = self.__client_server_message_class.parse_from_json(
				json_object=json.loads(client_socket.read())
			)
			kafka_message = KafkaMessage(
				source_uuid=source_uuid,
				client_server_message=client_server_message,
				create_datetime=datetime.utcnow(),
				message_uuid=str(uuid.uuid4())
			)
			kafka_writer.write_message(
				topic_name=self.__kafka_topic_name,
				message_bytes=json.dumps(kafka_message.to_json()).encode()
			).get_result()

	def start_receiving_from_clients(self):

		if self.__server_socket is not None:
			raise Exception(f"Must first stop receiving from clients before starting.")
		else:
			self.__is_receiving_from_clients = True
			self.__server_socket = self.__server_socket_factory.get_server_socket()
			self.__server_socket.start_accepting_clients(
				host_ip_address=self.__local_host_pointer.get_host_address(),
				host_port=self.__local_host_pointer.get_host_port(),
				on_accepted_client_method=self.__on_accepted_client_method
			)

	def stop_receiving_from_clients(self):

		if self.__server_socket is None:
			raise Exception(f"Must first start receiving from clients before stopping.")
		else:
			self.__server_socket.stop_accepting_clients()
			self.__server_socket = None
			self.__is_receiving_from_clients = False
			self.__client_sockets_semaphore.acquire()
			for client_socket in self.__client_sockets:
				client_socket.close(
					is_forced=True
				)
			self.__client_sockets.clear()
			self.__client_sockets_semaphore.release()


class ServerMessengerFactory():

	def __init__(self, *, server_socket_factory: ServerSocketFactory, kafka_manager_factory: KafkaManagerFactory, local_host_pointer: HostPointer, kafka_topic_name: str, client_server_message_class: Type[ClientServerMessage]):

		self.__server_socket_factory = server_socket_factory
		self.__kafka_manager_factory = kafka_manager_factory
		self.__local_host_pointer = local_host_pointer
		self.__kafka_topic_name = kafka_topic_name
		self.__client_server_message_class = client_server_message_class

	def get_server_messenger(self) -> ServerMessenger:
		return ServerMessenger(
			server_socket_factory=self.__server_socket_factory,
			kafka_manager_factory=self.__kafka_manager_factory,
			local_host_pointer=self.__local_host_pointer,
			kafka_topic_name=self.__kafka_topic_name,
			client_server_message_class=self.__client_server_message_class
		)
