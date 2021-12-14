from __future__ import annotations
from abc import ABC, abstractmethod
from austin_heller_repo.common import StringEnum, HostPointer
from austin_heller_repo.socket import ClientSocketFactory, ClientSocket, ServerSocketFactory, ServerSocket
from austin_heller_repo.kafka_manager import KafkaManagerFactory, KafkaManager, KafkaAsyncWriter, KafkaReader, KafkaMessage
from austin_heller_repo.threading import AsyncHandle, Semaphore, ReadOnlyAsyncHandle, PreparedSemaphoreRequest, SemaphoreRequestQueue, SemaphoreRequest, start_thread, ThreadCycle, ThreadCycleCache, CyclingUnitOfWork
from typing import List, Tuple, Dict, Type, Callable
import json
from datetime import datetime
import uuid
from transitions import MachineError, Machine
import time
import multiprocessing as mp


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
	def is_response(self) -> bool:
		raise NotImplementedError()

	@abstractmethod
	def is_directed_to_destination_uuid(self, *, destination_uuid: str) -> bool:
		raise NotImplementedError()

	@abstractmethod
	def is_structural_influence(self) -> bool:
		raise NotImplementedError()


class ClientMessenger():

	def __init__(self, *, client_socket_factory: ClientSocketFactory, server_host_pointer: HostPointer, client_server_message_class: Type[ClientServerMessage], receive_from_server_polling_seconds: float, is_debug: bool = False):

		self.__client_socket_factory = client_socket_factory
		self.__server_host_pointer = server_host_pointer
		self.__client_server_message_class = client_server_message_class
		self.__receive_from_server_polling_seconds = receive_from_server_polling_seconds
		self.__is_debug = is_debug

		self.__client_socket = None  # type: ClientSocket
		self.__receive_from_server_callback = None  # type: Callable[[ClientServerMessage], None]
		self.__receive_from_server_async_handle = None  # type: AsyncHandle

	def connect_to_server(self):
		if self.__client_socket is not None:
			raise Exception(f"Already connected to the server messenger.")
		else:
			self.__client_socket = self.__client_socket_factory.get_client_socket()
			self.__client_socket.connect_to_server(
				ip_address=self.__server_host_pointer.get_host_address(),
				port=self.__server_host_pointer.get_host_port()
			)

	def send_to_server(self, *, request_client_server_message: ClientServerMessage):
		message = json.dumps(request_client_server_message.to_json())
		self.__client_socket.write(message)

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

			if self.__is_debug:
				print(f"{datetime.utcnow()}: looping")

			is_message_received = False
			self.__client_socket.read_async(read_callback)

			if self.__is_debug:
				print(f"{datetime.utcnow()}: read async")

			while not is_message_received and not read_only_async_handle.is_cancelled():
				time.sleep(self.__receive_from_server_polling_seconds)

			if self.__is_debug:
				print(f"{datetime.utcnow()}: done waiting")

			if receive_from_server_exception is not None:
				raise receive_from_server_exception

		if self.__is_debug:
			print(f"{datetime.utcnow()}: done looping")

	def receive_from_server(self, *, callback: Callable[[ClientServerMessage], None]):
		if self.__receive_from_server_callback is not None:
			raise Exception(f"Already receiving from server.")
		else:
			def get_result(read_only_async_handle: ReadOnlyAsyncHandle):
				try:
					while not read_only_async_handle.is_cancelled():

						if self.__is_debug:
							print(f"{datetime.utcnow()}: ClientMessenger: receive_from_server: read start")

						client_server_message_json_string = self.__client_socket.read()

						if self.__is_debug:
							print(f"{datetime.utcnow()}: ClientMessenger: receive_from_server: read end: {client_server_message_json_string}")

						client_server_message = self.__client_server_message_class.parse_from_json(
							json_object=json.loads(client_server_message_json_string)
						)  # type: ClientServerMessage

						if self.__is_debug:
							print(f"{datetime.utcnow()}: ClientMessenger: receive_from_server: parsed: {client_server_message.get_client_server_message_type()}")

						callback(client_server_message)

						if self.__is_debug:
							print(f"{datetime.utcnow()}: ClientMessenger: receive_from_server: callback completed")

					if self.__is_debug:
						print(f"{datetime.utcnow()}: ClientMessenger: receive_from_server: cancelled")

				except Exception as ex:
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ClientMessenger: receive_from_server: ex: {ex}")
					raise ex
			async_handle = AsyncHandle(
				get_result_method=get_result
			)
			async_handle.try_wait(
				timeout_seconds=0
			)
			self.__receive_from_server_async_handle = async_handle

	def dispose(self):

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ClientMessenger: dispose: start")

		if self.__receive_from_server_async_handle is not None:
			self.__receive_from_server_async_handle.cancel()
			self.__receive_from_server_async_handle = None
		if self.__client_socket is not None:

			if self.__is_debug:
				print(f"{datetime.utcnow()}: ClientMessenger: dispose: closing client socket")

			self.__client_socket.close()

			if self.__is_debug:
				print(f"{datetime.utcnow()}: ClientMessenger: dispose: closed client socket")

			self.__client_socket = None

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ClientMessenger: dispose: end")


class ClientMessengerFactory():

	def __init__(self, *, client_socket_factory: ClientSocketFactory, server_host_pointer: HostPointer, client_server_message_class: Type[ClientServerMessage], receive_from_server_polling_seconds: float):

		self.__client_socket_factory = client_socket_factory
		self.__server_host_pointer = server_host_pointer
		self.__client_server_message_class = client_server_message_class
		self.__receive_from_server_polling_seconds = receive_from_server_polling_seconds

	def get_client_messenger(self) -> ClientMessenger:
		return ClientMessenger(
			client_socket_factory=self.__client_socket_factory,
			server_host_pointer=self.__server_host_pointer,
			client_server_message_class=self.__client_server_message_class,
			receive_from_server_polling_seconds=self.__receive_from_server_polling_seconds
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

	@staticmethod
	def parse_from_json(*, json_object: Dict, client_server_message_class: Type[ClientServerMessage]) -> SocketKafkaMessage:
		return SocketKafkaMessage(
			source_uuid=json_object["source_uuid"],
			client_server_message=client_server_message_class.parse_from_json(
				json_object=json_object["message"]
			),
			create_datetime=datetime.strptime(json_object["create_datetime"], "%Y-%m-%d %H:%M:%S.%f"),
			message_uuid=json_object["message_uuid"]
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

	def __init__(self, *, server_socket_factory: ServerSocketFactory, kafka_manager_factory: KafkaManagerFactory, local_host_pointer: HostPointer, kafka_topic_name: str, client_server_message_class: Type[ClientServerMessage], structure_factory: StructureFactory, is_debug: bool = False):

		self.__server_socket_factory = server_socket_factory
		self.__kafka_manager_factory = kafka_manager_factory
		self.__local_host_pointer = local_host_pointer
		self.__kafka_topic_name = kafka_topic_name
		self.__client_server_message_class = client_server_message_class
		self.__structure_factory = structure_factory
		self.__is_debug = is_debug

		self.__server_messenger_uuid = str(uuid.uuid4())
		self.__server_socket = None  # type: ServerSocket
		self.__kafka_manager = None  # type: KafkaManager
		self.__is_receiving_from_clients = False
		self.__client_sockets_per_source_uuid = {}  # type: Dict[str, ClientSocket]
		self.__client_sockets_per_source_uuid_semaphore = Semaphore()
		self.__structure = self.__structure_factory.get_structure()
		self.__kafka_reader_async_handle = None  # type: AsyncHandle
		self.__kafka_writer = None  # type: KafkaAsyncWriter
		self.__kafka_writer_semaphore = None  # type: Semaphore
		self.__is_threaded = False
		self.__send_socket_kafka_message_to_client_semaphore = Semaphore()

		self.__initialize()

	def __initialize(self):

		self.__kafka_manager = self.__kafka_manager_factory.get_kafka_manager()

		self.__kafka_writer = self.__kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter
		self.__kafka_writer_semaphore = Semaphore()

	def __send_socket_kafka_message_to_client(self, socket_kafka_message: SocketKafkaMessage):

		try:
			self.__send_socket_kafka_message_to_client_semaphore.acquire()
			is_message_sent_to_source = False
			for source_uuid in self.__client_sockets_per_source_uuid.keys():
				if socket_kafka_message.get_client_server_message().is_directed_to_destination_uuid(
					destination_uuid=source_uuid
				):
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __send_socket_kafka_message_to_client: sending message to client socket")
					try:
						self.__client_sockets_per_source_uuid[source_uuid].write(json.dumps(socket_kafka_message.get_client_server_message().to_json()))
						is_message_sent_to_source = True
					except BrokenPipeError as ex:
						try:
							self.__client_sockets_per_source_uuid_semaphore.acquire()
							del self.__client_sockets_per_source_uuid[source_uuid]
						finally:
							self.__client_sockets_per_source_uuid_semaphore.release()
					break

			if self.__is_debug:
				if is_message_sent_to_source:
					print(f"{datetime.utcnow()}: ServerMessenger: __send_socket_kafka_message_to_client: message sent")
				else:
					print(f"{datetime.utcnow()}: ServerMessenger: __send_socket_kafka_message_to_client: no message sent to a client socket")
		except Exception as ex:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __send_socket_kafka_message_to_client: ex: {ex}")
		finally:
			self.__send_socket_kafka_message_to_client_semaphore.release()

	def __write_socket_kafka_message_to_kafka(self, socket_kafka_message: SocketKafkaMessage, read_only_async_handle: ReadOnlyAsyncHandle):

		if not read_only_async_handle.is_cancelled():
			self.__kafka_writer_semaphore.acquire()
			try:
				write_message_async_handle = self.__kafka_writer.write_message(
					topic_name=self.__kafka_topic_name,
					message_bytes=json.dumps(socket_kafka_message.to_json()).encode()
				)
				write_message_async_handle.add_parent(
					async_handle=read_only_async_handle
				)
				write_message_async_handle.try_wait(
					timeout_seconds=0
				)
				write_message_async_handle.get_result()
			finally:
				self.__kafka_writer_semaphore.release()

	def __on_exception_kafka_write_cycling_unit_of_work(self, exception: Exception):
		print(f"{datetime.utcnow()}: ServerMessenger: __on_exception_kafka_write_cycling_unit_of_work: ex: {exception}")

	def __kafka_reader_thread_method(self, read_only_async_handle: ReadOnlyAsyncHandle):

		read_message_async_handle = None  # type: AsyncHandle

		if self.__is_debug and False:
			def test_method():
				nonlocal read_only_async_handle
				nonlocal read_message_async_handle
				try:
					while True:
						if read_only_async_handle.is_cancelled():
							print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: read_only_async_handle: test_method: (T): {read_only_async_handle.is_cancelled()}")
							if read_message_async_handle is not None:
								print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: read_message_async_handle: {read_message_async_handle.is_cancelled()}")
							time.sleep(0.5)
							break
						else:
							print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: read_only_async_handle: test_method: (F): {read_only_async_handle.is_cancelled()}")
							if read_message_async_handle is not None:
								print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: read_message_async_handle: {read_message_async_handle.is_cancelled()}")
							time.sleep(0.5)
				except Exception as ex:
					print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: read_only_async_handle: test_method: ex: {ex}")

			start_thread(test_method)

		try:
			kafka_reader_async_handle = self.__kafka_manager.get_reader(
				topic_name=self.__kafka_topic_name,
				is_from_beginning=True
			)
			kafka_reader_async_handle.add_parent(
				async_handle=read_only_async_handle
			)
			kafka_reader = kafka_reader_async_handle.get_result()  # type: KafkaReader
			while self.__is_receiving_from_clients and not read_only_async_handle.is_cancelled():

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: reading message")

				read_message_async_handle = kafka_reader.read_message()

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: setting parent")

				read_message_async_handle.add_parent(
					async_handle=read_only_async_handle
				)

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: getting result")

				kafka_message = read_message_async_handle.get_result()  # type: KafkaMessage

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: kafka_message: {kafka_message}")

				if not read_only_async_handle.is_cancelled():
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: message bytes: {kafka_message.get_message_bytes()}")
					socket_kafka_message = SocketKafkaMessage.parse_from_json(
						json_object=json.loads(kafka_message.get_message_bytes().decode()),
						client_server_message_class=self.__client_server_message_class
					)

					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: socket_kafka_message json: {socket_kafka_message.to_json()}")

					if not read_only_async_handle.is_cancelled():

						# check for message waiting to be responded to

						if socket_kafka_message.get_client_server_message().is_response():
							if self.__is_threaded:
								start_thread(self.__send_socket_kafka_message_to_client, socket_kafka_message)
							else:
								self.__send_socket_kafka_message_to_client(
									socket_kafka_message=socket_kafka_message
								)

						# send message into the structure if it needs to be processed

						if socket_kafka_message.get_client_server_message().is_structural_influence():
							update_structure_influence = UpdateStructureInfluence(
								socket_kafka_message=socket_kafka_message
							)
							self.__structure.update_structure(
								update_structure_influence=update_structure_influence
							)

							if len(update_structure_influence.get_response_client_server_messages()) != 0:

								if self.__is_debug:
									print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: sending {len(update_structure_influence.get_response_client_server_messages())} responses")
								for response_client_server_message in update_structure_influence.get_response_client_server_messages():
									response_socket_kafka_message = SocketKafkaMessage(
										source_uuid=self.__server_messenger_uuid,
										client_server_message=response_client_server_message,
										create_datetime=datetime.utcnow(),
										message_uuid=str(uuid.uuid4())
									)

									if self.__is_threaded:
										start_thread(self.__write_socket_kafka_message_to_kafka, response_socket_kafka_message, read_only_async_handle)
									else:
										self.__write_socket_kafka_message_to_kafka(
											socket_kafka_message=response_socket_kafka_message,
											read_only_async_handle=read_only_async_handle
										)

		except Exception as ex:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: ex: {ex}")
			raise ex

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: end")

	def __on_accepted_client_method(self, client_socket: ClientSocket):

		source_uuid = str(uuid.uuid4())

		self.__client_sockets_per_source_uuid_semaphore.acquire()
		self.__client_sockets_per_source_uuid[source_uuid] = client_socket
		self.__client_sockets_per_source_uuid_semaphore.release()

		process_read_async_handles = []  # type: List[AsyncHandle]

		try:
			while self.__is_receiving_from_clients:
				client_server_message_json_string = client_socket.read()

				def process_read_method(read_only_async_handle: ReadOnlyAsyncHandle):
					if client_server_message_json_string is None:
						if self.__is_debug:
							print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: accepted socket sent empty message")
					else:
						# TODO process the message in a separate thread so that the next read can occur
						client_server_message = self.__client_server_message_class.parse_from_json(
							json_object=json.loads(client_server_message_json_string)
						)

						socket_kafka_message = SocketKafkaMessage(
							source_uuid=source_uuid,
							client_server_message=client_server_message,
							create_datetime=datetime.utcnow(),
							message_uuid=str(uuid.uuid4())
						)

						if self.__is_debug:
							print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: writing message: {socket_kafka_message.to_json()}")

						self.__write_socket_kafka_message_to_kafka(
							socket_kafka_message=socket_kafka_message,
							read_only_async_handle=read_only_async_handle
						)

						if self.__is_debug:
							print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: wrote message: {socket_kafka_message.to_json()}")
				process_read_async_handle = AsyncHandle(
					get_result_method=process_read_method
				)
				process_read_async_handle.try_wait(
					timeout_seconds=0
				)
				process_read_async_handles.append(process_read_async_handle)

		except Exception as ex:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: ex: {ex}")
			if self.__is_receiving_from_clients:
				raise ex
		finally:
			for process_read_async_handle in process_read_async_handles:
				process_read_async_handle.cancel()

	def start_receiving_from_clients(self):

		if self.__server_socket is not None:
			raise Exception(f"Must first stop receiving from clients before starting.")
		else:
			self.__is_receiving_from_clients = True

			self.__kafka_reader_async_handle = AsyncHandle(
				get_result_method=self.__kafka_reader_thread_method
			)
			self.__kafka_reader_async_handle.try_wait(
				timeout_seconds=0
			)

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
			self.__server_socket.close()
			self.__server_socket = None  # type: ServerSocket
			self.__is_receiving_from_clients = False
			self.__kafka_reader_async_handle.cancel()
			self.__client_sockets_per_source_uuid_semaphore.acquire()
			for source_uuid in self.__client_sockets_per_source_uuid.keys():
				client_socket = self.__client_sockets_per_source_uuid[source_uuid]
				client_socket.close()
			self.__client_sockets_per_source_uuid.clear()
			self.__client_sockets_per_source_uuid_semaphore.release()


class ServerMessengerFactory():

	def __init__(self, *, server_socket_factory: ServerSocketFactory, kafka_manager_factory: KafkaManagerFactory, local_host_pointer: HostPointer, kafka_topic_name: str, client_server_message_class: Type[ClientServerMessage], structure_factory: StructureFactory, is_debug: bool = False):

		self.__server_socket_factory = server_socket_factory
		self.__kafka_manager_factory = kafka_manager_factory
		self.__local_host_pointer = local_host_pointer
		self.__kafka_topic_name = kafka_topic_name
		self.__client_server_message_class = client_server_message_class
		self.__structure_factory = structure_factory
		self.__is_debug = is_debug

	def get_server_messenger(self) -> ServerMessenger:
		return ServerMessenger(
			server_socket_factory=self.__server_socket_factory,
			kafka_manager_factory=self.__kafka_manager_factory,
			local_host_pointer=self.__local_host_pointer,
			kafka_topic_name=self.__kafka_topic_name,
			client_server_message_class=self.__client_server_message_class,
			structure_factory=self.__structure_factory,
			is_debug=self.__is_debug
		)
