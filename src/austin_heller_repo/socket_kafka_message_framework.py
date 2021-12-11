from __future__ import annotations
from abc import ABC, abstractmethod
from austin_heller_repo.common import StringEnum, HostPointer
from austin_heller_repo.socket import ClientSocketFactory, ClientSocket, ServerSocketFactory, ServerSocket
from austin_heller_repo.kafka_manager import KafkaManagerFactory, KafkaManager, KafkaAsyncWriter, KafkaReader
from austin_heller_repo.threading import AsyncHandle, Semaphore, ReadOnlyAsyncHandle, PreparedSemaphoreRequest, SemaphoreRequestQueue, SemaphoreRequest
from typing import List, Tuple, Dict, Type, Callable
import json
from datetime import datetime
import uuid
from transitions import MachineError, Machine
import time


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

	@staticmethod
	def remove_base_keys(*, json_object: Dict):
		json_object.pop("__type")

	@abstractmethod
	def is_directed_to_destination_uuid(self, *, destination_uuid: str) -> bool:
		raise NotImplementedError()

	@abstractmethod
	def is_structural_influence(self) -> bool:
		raise NotImplementedError()


class ClientMessenger():

	def __init__(self, *, client_socket_factory: ClientSocketFactory, server_host_pointer: HostPointer, client_server_message_class: Type[ClientServerMessage], receive_from_server_polling_seconds: float):

		self.__client_socket_factory = client_socket_factory
		self.__server_host_pointer = server_host_pointer
		self.__client_server_message_class = client_server_message_class
		self.__receive_from_server_polling_seconds = receive_from_server_polling_seconds

		self.__client_socket = None  # type: ClientSocket
		self.__receive_from_server_callback = None  # type: Callable[[ClientServerMessage], None]
		self.__receive_from_server_async_handle = None  # type: AsyncHandle

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

	def __receive_from_server(self, read_only_async_handle: ReadOnlyAsyncHandle):

		is_message_received = None  # type: bool
		receive_from_server_exception = None  # type: Exception

		def read_callback(message: str):
			nonlocal is_message_received
			nonlocal receive_from_server_exception
			try:
				self.__receive_from_server_callback(message)
			except Exception as ex:
				receive_from_server_exception = ex
			is_message_received = True

		while not read_only_async_handle.is_cancelled():
			is_message_received = False
			self.__client_socket.read_async(read_callback)
			while not is_message_received and not read_only_async_handle.is_cancelled():
				time.sleep(self.__receive_from_server_polling_seconds)
			if receive_from_server_exception is not None:
				raise receive_from_server_exception

	def receive_from_server(self, *, callback: Callable[[ClientServerMessage], None]) -> AsyncHandle:
		if self.__receive_from_server_callback is not None:
			raise Exception(f"Already receiving from server.")
		else:
			self.__receive_from_server_callback = callback

			async_handle = AsyncHandle(
				get_result_method=self.__receive_from_server
			)
			async_handle.try_wait(
				timeout_seconds=0
			)
			return async_handle


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


class SocketKafkaMessage():

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

	def parse_from_json(self, *, json_object: Dict) -> SocketKafkaMessage:
		return SocketKafkaMessage(
			source_uuid=json_object["source_uuid"],
			client_server_message=ClientServerMessage.parse_from_json(
				json_object=json_object["message"]
			),
			create_datetime=datetime.strptime(json_object["create_datetime"], "%Y-%m-%d %H:%M:%S.%f"),
			message_uuid=self.get_message_uuid()
		)


class UpdateStructureInfluence():

	def __init__(self, *, socket_kafka_message: SocketKafkaMessage):

		self.__socket_kafka_message = socket_kafka_message

		self.__response_client_server_messages = []  # type: List[ClientServerMessage]

	def get_socket_kafka_message(self) -> SocketKafkaMessage:
		return self.__socket_kafka_message

	def add_response_client_server_message(self, *, client_server_message: ClientServerMessage):
		self.__response_client_server_messages.append(client_server_message)

	def get_response_client_server_messages(self) -> Tuple[ClientServerMessage]:
		return tuple(self.__response_client_server_messages)


class StructureStateEnum(StringEnum):
	pass


class Structure(Machine, ABC):

	def __init__(self, *, states: Type[StructureStateEnum], initial_state: StructureStateEnum):
		super().__init__(
			model=self,
			states=states.get_list(),
			initial=initial_state.value
		)

		pass

	def update_structure(self, *, update_structure_influence: UpdateStructureInfluence):
		try:
			# the update_structure_influence is passed into the trigger so that responses can be appended to its internal list
			self.trigger(update_structure_influence.get_socket_kafka_message().get_client_server_message().get_client_server_message_type().value, update_structure_influence)
		except MachineError as ex:
			pass  # TODO check for specific trigger_enum in error
			print(f"update_structure: ex: {ex}")


class StructureFactory(ABC):

	@abstractmethod
	def get_structure(self) -> Structure:
		raise NotImplementedError()


class ServerMessenger():

	def __init__(self, *, server_socket_factory: ServerSocketFactory, kafka_manager_factory: KafkaManagerFactory, local_host_pointer: HostPointer, kafka_topic_name: str, client_server_message_class: Type[ClientServerMessage], structure_factory: StructureFactory):

		self.__server_socket_factory = server_socket_factory
		self.__kafka_manager_factory = kafka_manager_factory
		self.__local_host_pointer = local_host_pointer
		self.__kafka_topic_name = kafka_topic_name
		self.__client_server_message_class = client_server_message_class
		self.__structure_factory = structure_factory

		self.__server_socket = None  # type: ServerSocket
		self.__kafka_manager = self.__kafka_manager_factory.get_kafka_manager()  # type: KafkaManager
		self.__kafka_reader = self.__kafka_manager.get_reader(
			topic_name=self.__kafka_topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader
		self.__is_receiving_from_clients = False
		self.__client_sockets = []  # type: List[ClientSocket]
		self.__client_sockets_semaphore = Semaphore()
		self.__structure = self.__structure_factory.get_structure()
		self.__kafka_reader_async_handle_thread = None
		self.__kafka_reader_async_handle = None  # type: AsyncHandle
		self.__blocking_semaphore_per_source_uuid = {}  # type: Dict[str, Semaphore]
		self.__blocking_semaphore_per_source_uuid_semaphore = Semaphore()
		self.__response_client_server_message_per_source_uuid = {}  # type: Dict[str, ClientServerMessage]

	def __kafka_reader_thread_method(self, read_only_async_handle: ReadOnlyAsyncHandle):
		while self.__is_receiving_from_clients:
			read_message_async_handle = self.__kafka_reader.read_message()
			read_message_async_handle.add_parent(
				async_handle=read_only_async_handle
			)
			socket_kafka_message = read_message_async_handle.get_result()  # type: SocketKafkaMessage

			# check for message waiting to be responded to
			self.__blocking_semaphore_per_source_uuid_semaphore.acquire()
			for source_uuid in self.__blocking_semaphore_per_source_uuid.keys():
				if socket_kafka_message.get_client_server_message().is_directed_to_destination_uuid(
					destination_uuid=source_uuid
				):
					self.__response_client_server_message_per_source_uuid[source_uuid] = socket_kafka_message.get_client_server_message()
					break
			self.__blocking_semaphore_per_source_uuid_semaphore.release()

	def __on_accepted_client_method(self, client_socket: ClientSocket):

		self.__client_sockets_semaphore.acquire()
		self.__client_sockets.append(client_socket)
		self.__client_sockets_semaphore.release()

		source_uuid = str(uuid.uuid4())
		self.__blocking_semaphore_per_source_uuid_semaphore.acquire()
		self.__blocking_semaphore_per_source_uuid[source_uuid] = Semaphore()
		self.__blocking_semaphore_per_source_uuid_semaphore.release()

		kafka_writer = self.__kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter
		while self.__is_receiving_from_clients:
			client_server_message = self.__client_server_message_class.parse_from_json(
				json_object=json.loads(client_socket.read())
			)

			# setup the blocking semaphore while waiting for the kafka reader to discover the response
			self.__blocking_semaphore_per_source_uuid[source_uuid].acquire()

			socket_kafka_message = SocketKafkaMessage(
				source_uuid=source_uuid,
				client_server_message=client_server_message,
				create_datetime=datetime.utcnow(),
				message_uuid=str(uuid.uuid4())
			)
			kafka_writer.write_message(
				topic_name=self.__kafka_topic_name,
				message_bytes=json.dumps(socket_kafka_message.to_json()).encode()
			).get_result()

			# wait for the thread that's reading the kafka topic to find the response
			self.__blocking_semaphore_per_source_uuid[source_uuid].acquire()

			response_client_server_message = self.__response_client_server_message_per_source_uuid.pop(source_uuid)
			client_socket.write(json.dumps(response_client_server_message.to_json()))

			# release the reading thread to continue reading
			self.__blocking_semaphore_per_source_uuid[source_uuid].release()

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

	def __init__(self, *, server_socket_factory: ServerSocketFactory, kafka_manager_factory: KafkaManagerFactory, local_host_pointer: HostPointer, kafka_topic_name: str, client_server_message_class: Type[ClientServerMessage], structure_factory: StructureFactory):

		self.__server_socket_factory = server_socket_factory
		self.__kafka_manager_factory = kafka_manager_factory
		self.__local_host_pointer = local_host_pointer
		self.__kafka_topic_name = kafka_topic_name
		self.__client_server_message_class = client_server_message_class
		self.__structure_factory = structure_factory

	def get_server_messenger(self) -> ServerMessenger:
		return ServerMessenger(
			server_socket_factory=self.__server_socket_factory,
			kafka_manager_factory=self.__kafka_manager_factory,
			local_host_pointer=self.__local_host_pointer,
			kafka_topic_name=self.__kafka_topic_name,
			client_server_message_class=self.__client_server_message_class,
			structure_factory=self.__structure_factory
		)
