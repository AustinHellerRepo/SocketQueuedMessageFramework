from __future__ import annotations
from abc import ABC, abstractmethod
from austin_heller_repo.common import StringEnum, HostPointer
from austin_heller_repo.socket import ClientSocketFactory, ClientSocket, ServerSocketFactory, ServerSocket, ReadWriteSocketClosedException
from austin_heller_repo.threading import AsyncHandle, Semaphore, ReadOnlyAsyncHandle, PreparedSemaphoreRequest, SemaphoreRequestQueue, SemaphoreRequest, start_thread, ThreadCycle, ThreadCycleCache, CyclingUnitOfWork, SequentialQueue, SequentialQueueReader, SequentialQueueWriter, SequentialQueueFactory, ConstantAsyncHandle
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
	def get_destination_uuid(self) -> str:
		raise NotImplementedError()

	@abstractmethod
	def is_structural_influence(self) -> bool:
		raise NotImplementedError()

	@abstractmethod
	def is_ordered(self) -> bool:
		raise NotImplementedError()

	@abstractmethod
	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		raise NotImplementedError()


class ClientMessenger():

	def __init__(self, *, client_socket_factory: ClientSocketFactory, server_host_pointer: HostPointer, client_server_message_class: Type[ClientServerMessage], is_debug: bool = False):

		self.__client_socket_factory = client_socket_factory
		self.__server_host_pointer = server_host_pointer
		self.__client_server_message_class = client_server_message_class
		self.__is_debug = is_debug

		self.__client_socket = None  # type: ClientSocket
		self.__receive_from_server_callback = None  # type: Callable[[ClientServerMessage], None]
		self.__receive_from_server_async_handle = None  # type: AsyncHandle
		self.__is_closing = False

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

	def __receive_from_server(self, read_only_async_handle: ReadOnlyAsyncHandle, callback: Callable[[ClientServerMessage], None], on_exception: Callable[[Exception], None]):

		if not read_only_async_handle.is_cancelled():
			try:
				while not read_only_async_handle.is_cancelled():

					if self.__is_debug:
						print(f"{datetime.utcnow()}: ClientMessenger: receive_from_server: read start")

					client_server_message_json_string = self.__client_socket.read()

					if self.__is_debug:
						print(f"{datetime.utcnow()}: ClientMessenger: receive_from_server: read end: {client_server_message_json_string}")

					if not self.__is_closing:

						client_server_message = self.__client_server_message_class.parse_from_json(
							json_object=json.loads(client_server_message_json_string)
						)  # type: ClientServerMessage

						if self.__is_debug:
							print(
								f"{datetime.utcnow()}: ClientMessenger: receive_from_server: parsed: {client_server_message.get_client_server_message_type()}")

						callback(client_server_message)

						if self.__is_debug:
							print(f"{datetime.utcnow()}: ClientMessenger: receive_from_server: callback completed")

					else:
						if not read_only_async_handle.is_cancelled():
							raise Exception(f"Unexpected closing while read_only_async_handle is not cancelled.")

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ClientMessenger: receive_from_server: cancelled")

			except Exception as ex:
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ClientMessenger: receive_from_server: ex: {ex}")
				if self.__is_closing and isinstance(ex, ReadWriteSocketClosedException):
					pass  # permit this exception to be ignored because this would be expected when disposing this object
				else:
					on_exception(ex)

	def receive_from_server(self, *, callback: Callable[[ClientServerMessage], None], on_exception: Callable[[Exception], None]):
		if self.__receive_from_server_callback is not None:
			raise Exception(f"Already receiving from server.")
		elif self.__client_socket is None:
			raise Exception(f"Must connect to the server before starting to receive.")
		else:
			async_handle = AsyncHandle(
				get_result_method=self.__receive_from_server,
				callback=callback,
				on_exception=on_exception
			)
			async_handle.try_wait(
				timeout_seconds=0
			)
			self.__receive_from_server_async_handle = async_handle

	def dispose(self):

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ClientMessenger: dispose: start")

		self.__is_closing = True

		if self.__receive_from_server_async_handle is not None:
			self.__receive_from_server_async_handle.cancel()

		if self.__client_socket is not None:

			if self.__is_debug:
				print(f"{datetime.utcnow()}: ClientMessenger: dispose: closing client socket")

			self.__client_socket.close()

			if self.__is_debug:
				print(f"{datetime.utcnow()}: ClientMessenger: dispose: closed client socket")

		if self.__receive_from_server_async_handle is not None:
			self.__receive_from_server_async_handle.get_result()

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ClientMessenger: dispose: end")


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


class StructureTriggerInvalidForStateException(Exception):

	def __init__(self, update_structure_influence: UpdateStructureInfluence, inner_exception: Exception, *args):
		super().__init__(*args)

		self.__update_structure_influence = update_structure_influence
		self.__inner_exception = inner_exception

	def get_update_structure_influence(self) -> UpdateStructureInfluence:
		return self.__update_structure_influence

	def get_inner_exception(self) -> Exception:
		return self.__inner_exception


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
			print(f"update_structure: ex: {ex}")
			raise StructureTriggerInvalidForStateException(
				update_structure_influence=update_structure_influence,
				inner_exception=ex
			)


class StructureFactory(ABC):

	@abstractmethod
	def get_structure(self) -> Structure:
		raise NotImplementedError()


class ServerMessenger():

	def __init__(self, *, server_socket_factory: ServerSocketFactory, sequential_queue_factory: SequentialQueueFactory, local_host_pointer: HostPointer, client_server_message_class: Type[ClientServerMessage], structure_factory: StructureFactory, is_debug: bool = False):

		self.__server_socket_factory = server_socket_factory
		self.__sequential_queue_factory = sequential_queue_factory
		self.__local_host_pointer = local_host_pointer
		self.__client_server_message_class = client_server_message_class
		self.__structure_factory = structure_factory
		self.__is_debug = is_debug

		self.__server_messenger_uuid = str(uuid.uuid4())
		self.__server_socket = None  # type: ServerSocket
		self.__sequential_queue = None  # type: SequentialQueue
		self.__sequential_queue_writer = None  # type: SequentialQueueWriter
		self.__sequential_queue_reader = None  # type: SequentialQueueReader
		self.__is_receiving_from_clients = False
		self.__client_sockets_per_source_uuid = {}  # type: Dict[str, ClientSocket]
		self.__client_sockets_per_source_uuid_semaphore = Semaphore()
		self.__structure = self.__structure_factory.get_structure()
		self.__kafka_reader_async_handle = None  # type: AsyncHandle
		self.__on_accepted_client_method_exception = None  # type: Exception
		self.__on_accepted_client_method_exception_semaphore = Semaphore()
		self.__process_read_async_handle_per_source_uuid = {}  # type: Dict[str, AsyncHandle]
		self.__process_read_async_handle_per_source_uuid_semaphore = Semaphore()

		self.__initialize()

	def __initialize(self):

		self.__sequential_queue = self.__sequential_queue_factory.get_sequential_queue()
		self.__sequential_queue_writer = self.__sequential_queue.get_writer().get_result()
		self.__sequential_queue_reader = self.__sequential_queue.get_reader().get_result()

	def __send_socket_kafka_message_to_client(self, socket_kafka_message: SocketKafkaMessage):

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ServerMessenger: __send_socket_kafka_message_to_client: sending message to client socket")

		try:
			is_message_sent_to_source = False

			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __send_socket_kafka_message_to_client: searching in keys: {self.__client_sockets_per_source_uuid.keys()}")

			destination_uuid = socket_kafka_message.get_client_server_message().get_destination_uuid()

			if destination_uuid is None:
				raise Exception(f"Unexpected response client server message without destination uuid. Message: {socket_kafka_message.to_json()}")
			else:
				self.__client_sockets_per_source_uuid_semaphore.acquire()
				try:
					if destination_uuid in self.__client_sockets_per_source_uuid:
						self.__client_sockets_per_source_uuid[destination_uuid].write(json.dumps(socket_kafka_message.get_client_server_message().to_json()))
						is_message_sent_to_source = True
				except ReadWriteSocketClosedException as ex:
					self.__client_sockets_per_source_uuid[destination_uuid].close()
					del self.__client_sockets_per_source_uuid[destination_uuid]
				finally:
					self.__client_sockets_per_source_uuid_semaphore.release()

			if self.__is_debug:
				if is_message_sent_to_source:
					print(f"{datetime.utcnow()}: ServerMessenger: __send_socket_kafka_message_to_client: message sent")
				else:
					print(f"{datetime.utcnow()}: ServerMessenger: __send_socket_kafka_message_to_client: no message sent to a client socket")
		except Exception as ex:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __send_socket_kafka_message_to_client: ex: {ex}")
			raise ex
		finally:
			pass

	def __write_socket_kafka_message_to_kafka(self, read_only_async_handle: ReadOnlyAsyncHandle, socket_kafka_message: SocketKafkaMessage):

		if not read_only_async_handle.is_cancelled():

			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __write_socket_kafka_message_to_kafka: writing message to sequential queue")

			write_bytes_async_handle = self.__sequential_queue_writer.write_bytes(
				message_bytes=json.dumps(socket_kafka_message.to_json()).encode()
			)
			write_bytes_async_handle.add_parent(
				async_handle=read_only_async_handle
			)
			write_bytes_async_handle.get_result()
		else:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __write_socket_kafka_message_to_kafka: cancelled, so not writing message to sequential queue")

	def __process_socket_kafka_message(self, read_only_async_handle: ReadOnlyAsyncHandle, socket_kafka_message: SocketKafkaMessage, is_from_queue: bool):

		if not read_only_async_handle.is_cancelled():
			if socket_kafka_message.get_client_server_message().is_ordered() and not is_from_queue:
				self.__write_socket_kafka_message_to_kafka(
					read_only_async_handle=read_only_async_handle,
					socket_kafka_message=socket_kafka_message
				)
			else:
				if socket_kafka_message.get_client_server_message().is_response():
					self.__send_socket_kafka_message_to_client(
						socket_kafka_message=socket_kafka_message
					)

				# send message into the structure if it needs to be processed

				if socket_kafka_message.get_client_server_message().is_structural_influence():
					update_structure_influence = UpdateStructureInfluence(
						socket_kafka_message=socket_kafka_message
					)
					response_socket_kafka_messages = []  # type: List[SocketKafkaMessage]
					try:
						self.__structure.update_structure(
							update_structure_influence=update_structure_influence
						)
					except StructureTriggerInvalidForStateException as ex:
						if self.__is_debug:
							print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: structure ex: {ex}")
						response_client_server_message = socket_kafka_message.get_client_server_message().get_structural_error_client_server_message_response(
							destination_uuid=socket_kafka_message.get_source_uuid()
						)
						if response_client_server_message is not None:
							response_socket_kafka_messages.append(SocketKafkaMessage(
								source_uuid=self.__server_messenger_uuid,
								client_server_message=response_client_server_message,
								create_datetime=datetime.utcnow(),
								message_uuid=str(uuid.uuid4())
							))
					else:
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
								response_socket_kafka_messages.append(response_socket_kafka_message)

					if len(response_socket_kafka_messages) != 0:
						if self.__is_debug:
							print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: sending {len(response_socket_kafka_messages)} responses")
						for response_socket_kafka_message in response_socket_kafka_messages:
							self.__process_socket_kafka_message(
								read_only_async_handle=read_only_async_handle,
								socket_kafka_message=response_socket_kafka_message,
								is_from_queue=False
							)

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
			while self.__is_receiving_from_clients and not read_only_async_handle.is_cancelled():

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: reading message")

				read_bytes_async_handle = self.__sequential_queue_reader.read_bytes()

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: setting parent")

				read_bytes_async_handle.add_parent(
					async_handle=read_only_async_handle
				)

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: getting result")

				message_bytes = read_bytes_async_handle.get_result()  # type: bytes

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: kafka_message: {message_bytes}")

				if self.__is_receiving_from_clients and not read_only_async_handle.is_cancelled():
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: message bytes: {message_bytes}")
					socket_kafka_message = SocketKafkaMessage.parse_from_json(
						json_object=json.loads(message_bytes.decode()),
						client_server_message_class=self.__client_server_message_class
					)

					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: socket_kafka_message json: {socket_kafka_message.to_json()}")

					if not read_only_async_handle.is_cancelled():

						# check for message waiting to be responded to

						async_handle = AsyncHandle(
							get_result_method=self.__process_socket_kafka_message,
							socket_kafka_message=socket_kafka_message,
							is_from_queue=True
						)
						async_handle.add_parent(
							async_handle=read_only_async_handle
						)
						async_handle.get_result()

				else:
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: cancelled")

		except Exception as ex:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: ex: {ex}")
			raise ex

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ServerMessenger: __kafka_reader_thread_method: end")

	def __process_read_message_from_client_socket(self, read_only_async_handle: ReadOnlyAsyncHandle, source_uuid: str, message: str):

		if message is None:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __process_read_message_from_client_socket: accepted socket sent empty message")
		else:
			client_server_message = self.__client_server_message_class.parse_from_json(
				json_object=json.loads(message)
			)

			socket_kafka_message = SocketKafkaMessage(
				source_uuid=source_uuid,
				client_server_message=client_server_message,
				create_datetime=datetime.utcnow(),
				message_uuid=str(uuid.uuid4())
			)

			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: writing message: {socket_kafka_message.to_json()}")

			self.__process_socket_kafka_message(
				read_only_async_handle=read_only_async_handle,
				socket_kafka_message=socket_kafka_message,
				is_from_queue=False
			)

			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: wrote message: {socket_kafka_message.to_json()}")

	def __on_accepted_client_method(self, client_socket: ClientSocket):

		source_uuid = str(uuid.uuid4())

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: start: {source_uuid}")

		self.__client_sockets_per_source_uuid_semaphore.acquire()
		self.__client_sockets_per_source_uuid[source_uuid] = client_socket
		self.__client_sockets_per_source_uuid_semaphore.release()

		try:
			while self.__is_receiving_from_clients:

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: reading from client socket")

				try:
					client_server_message_json_string = client_socket.read()
				except ReadWriteSocketClosedException as ex:
					break

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: read from client socket: {client_server_message_json_string}")

				if self.__on_accepted_client_method_exception is None:

					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: starting __process_read_message_from_client_socket")

					self.__process_read_async_handle_per_source_uuid_semaphore.acquire()
					constant_async_handle = ConstantAsyncHandle(
						result=None
					)
					self.__process_read_async_handle_per_source_uuid[source_uuid] = constant_async_handle
					self.__process_read_async_handle_per_source_uuid_semaphore.release()

					self.__process_read_message_from_client_socket(
						read_only_async_handle=constant_async_handle,
						source_uuid=source_uuid,
						message=client_server_message_json_string
					)

					self.__process_read_async_handle_per_source_uuid_semaphore.acquire()
					if source_uuid in self.__process_read_async_handle_per_source_uuid:
						del self.__process_read_async_handle_per_source_uuid[source_uuid]
					self.__process_read_async_handle_per_source_uuid_semaphore.release()

					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: ended __process_read_message_from_client_socket")

		except Exception as ex:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: ex: {ex}")
			self.__on_accepted_client_method_exception_semaphore.acquire()
			if self.__on_accepted_client_method_exception is None:
				self.__on_accepted_client_method_exception = ex
			self.__on_accepted_client_method_exception_semaphore.release()
		finally:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: cancelling process_read_async_handles: {len(self.__process_read_async_handle_per_source_uuid.keys())}")

			self.__process_read_async_handle_per_source_uuid_semaphore.acquire()
			if source_uuid in self.__process_read_async_handle_per_source_uuid:
				self.__process_read_async_handle_per_source_uuid[source_uuid].cancel()
			self.__process_read_async_handle_per_source_uuid_semaphore.release()

			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: cancelled process_read_async_handles")

			self.__client_sockets_per_source_uuid_semaphore.acquire()
			try:
				if source_uuid in self.__client_sockets_per_source_uuid:
					del self.__client_sockets_per_source_uuid[source_uuid]
			finally:
				self.__client_sockets_per_source_uuid_semaphore.release()
			client_socket.close()

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: end: {source_uuid}")

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
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__server_socket.stop_accepting_clients()")
			self.__server_socket.stop_accepting_clients()
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__server_socket.close()")
			self.__server_socket.close()
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__server_socket = None")
			self.__server_socket = None  # type: ServerSocket
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__is_receiving_from_clients = False")
			self.__is_receiving_from_clients = False
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__sequential_queue.dispose()")
			self.__sequential_queue.dispose()
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__sequential_queue_writer.dispose()")
			self.__sequential_queue_writer.dispose()
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__sequential_queue_reader.dispose()")
			self.__sequential_queue_reader.dispose()
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__kafka_reader_async_handle.cancel()")
			self.__kafka_reader_async_handle.cancel()
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__kafka_reader_async_handle.get_result()")
			self.__kafka_reader_async_handle.get_result()
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__client_sockets_per_source_uuid_semaphore.acquire()")
			self.__process_read_async_handle_per_source_uuid_semaphore.acquire()
			for source_uuid in self.__process_read_async_handle_per_source_uuid:
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__process_read_async_handle_per_source_uuid[source_uuid].cancel()")
				self.__process_read_async_handle_per_source_uuid[source_uuid].cancel()
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__process_read_async_handle_per_source_uuid[source_uuid].get_result()")
				self.__process_read_async_handle_per_source_uuid[source_uuid].get_result()
			self.__process_read_async_handle_per_source_uuid_semaphore.release()
			self.__client_sockets_per_source_uuid_semaphore.acquire()
			for source_uuid in self.__client_sockets_per_source_uuid:
				client_socket = self.__client_sockets_per_source_uuid[source_uuid]
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: client_socket.close() start")
				client_socket.close()
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: client_socket.close() end")
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__client_sockets_per_source_uuid.clear()")
			self.__client_sockets_per_source_uuid.clear()
			self.__client_sockets_per_source_uuid_semaphore.release()

		self.__on_accepted_client_method_exception_semaphore.acquire()
		exception = self.__on_accepted_client_method_exception
		self.__on_accepted_client_method_exception = None
		self.__on_accepted_client_method_exception_semaphore.release()

		if exception is not None:
			raise exception


class ServerMessengerFactory():

	def __init__(self, *, server_socket_factory: ServerSocketFactory, sequential_queue_factory: SequentialQueueFactory, local_host_pointer: HostPointer, client_server_message_class: Type[ClientServerMessage], structure_factory: StructureFactory, is_debug: bool = False):

		self.__server_socket_factory = server_socket_factory
		self.__sequential_queue_factory = sequential_queue_factory
		self.__local_host_pointer = local_host_pointer
		self.__client_server_message_class = client_server_message_class
		self.__structure_factory = structure_factory
		self.__is_debug = is_debug

	def get_server_messenger(self) -> ServerMessenger:
		return ServerMessenger(
			server_socket_factory=self.__server_socket_factory,
			sequential_queue_factory=self.__sequential_queue_factory,
			local_host_pointer=self.__local_host_pointer,
			client_server_message_class=self.__client_server_message_class,
			structure_factory=self.__structure_factory,
			is_debug=self.__is_debug
		)
