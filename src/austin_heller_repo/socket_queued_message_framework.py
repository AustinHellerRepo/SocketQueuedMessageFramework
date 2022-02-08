from __future__ import annotations
from abc import ABC, abstractmethod
from austin_heller_repo.common import StringEnum, HostPointer, static_init, ElapsedTime
from austin_heller_repo.socket import ClientSocketFactory, ClientSocket, ServerSocketFactory, ServerSocket, ReadWriteSocketClosedException
from austin_heller_repo.threading import AsyncHandle, Semaphore, ReadOnlyAsyncHandle, PreparedSemaphoreRequest, SemaphoreRequestQueue, SemaphoreRequest, start_thread, ThreadCycle, ThreadCycleCache, CyclingUnitOfWork, SequentialQueue, SequentialQueueReader, SequentialQueueWriter, SequentialQueueFactory, ConstantAsyncHandle
from typing import List, Tuple, Dict, Type, Callable
import json
from datetime import datetime
import uuid
import time
import multiprocessing as mp


class ClientServerMessageTypeEnum(StringEnum):
	pass


class ClientServerMessage(ABC):

	__client_server_message_class_per_client_server_message_type_per_parent_class = {}  # type: Dict[Type[ClientServerMessage], Dict[ClientServerMessageTypeEnum, Type[ClientServerMessage]]]

	def __init__(self, *, destination_uuid: str):

		self.__destination_uuid = destination_uuid

	@classmethod
	def __map_client_server_message_class_to_client_server_message_type(cls):
		subclasses = list(cls.__subclasses__())  # type: List[Type[ClientServerMessage]]
		if not subclasses:
			raise Exception(f"Failed to find subclasses. Main class: {cls}.")
		ClientServerMessage.__client_server_message_class_per_client_server_message_type_per_parent_class[cls] = {}
		for subclass in subclasses:
			client_server_message_type = subclass.get_client_server_message_type()
			if client_server_message_type in ClientServerMessage.__client_server_message_class_per_client_server_message_type_per_parent_class[cls]:
				raise Exception(f"Duplicate ClientServerMessageType found for both {ClientServerMessage.__client_server_message_class_per_client_server_message_type_per_parent_class[cls][client_server_message_type].__name__} and {subclass.__name__}")
			else:
				ClientServerMessage.__client_server_message_class_per_client_server_message_type_per_parent_class[cls][client_server_message_type] = subclass

	@classmethod
	def get_client_server_message_class(cls, client_server_message_type: ClientServerMessageTypeEnum):
		if cls not in ClientServerMessage.__client_server_message_class_per_client_server_message_type_per_parent_class:
			cls.__map_client_server_message_class_to_client_server_message_type()
		return ClientServerMessage.__client_server_message_class_per_client_server_message_type_per_parent_class[cls][client_server_message_type]

	@classmethod
	def parse_from_json(cls, *, json_object: Dict):
		client_server_message_type_class = cls.get_client_server_message_type_class()
		client_server_message_type = client_server_message_type_class(json_object["__type"])
		client_server_message_class = cls.get_client_server_message_class(
			client_server_message_type=client_server_message_type
		)
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if json_object:
			client_server_message = client_server_message_class(
				**json_object
			)
		else:
			client_server_message = client_server_message_class()
		return client_server_message

	@staticmethod
	def remove_base_keys(*, json_object: Dict):
		json_object.pop("__type")

	@classmethod
	@abstractmethod
	def get_client_server_message_type_class(cls) -> Type[ClientServerMessageTypeEnum]:
		raise NotImplementedError()

	@classmethod
	@abstractmethod
	def get_client_server_message_type(cls) -> ClientServerMessageTypeEnum:
		pass

	@abstractmethod
	def to_json(self) -> Dict:
		return {
			"__type": self.__class__.get_client_server_message_type().value,
			"destination_uuid": self.__destination_uuid
		}

	def get_destination_uuid(self) -> str:
		return self.__destination_uuid

	@abstractmethod
	def get_structural_error_client_server_message_response(self, *, structure_transition_exception: StructureTransitionException, destination_uuid: str) -> ClientServerMessage:
		raise NotImplementedError()


class ClientMessenger():

	def __init__(self, *, client_socket_factory: ClientSocketFactory, server_host_pointer: HostPointer, client_server_message_class: Type[ClientServerMessage], is_debug: bool = False):

		self.__client_socket_factory = client_socket_factory
		self.__server_host_pointer = server_host_pointer
		self.__client_server_message_class = client_server_message_class
		self.__is_debug = is_debug

		self.__debug_uuid = str(uuid.uuid4())
		self.__client_socket = None  # type: ClientSocket
		self.__receive_from_server_callback = None  # type: Callable[[ClientServerMessage], None]
		self.__receive_from_server_async_handle = None  # type: AsyncHandle
		self.__is_closing = False

	def set_debug(self, *, is_debug: bool):
		self.__is_debug = is_debug

	def connect_to_server(self):
		if self.__is_debug:
			print(f"{datetime.utcnow()}: ClientMessenger: connect_to_server: start")
		try:
			if self.__client_socket is not None:
				raise Exception(f"Already connected to the server messenger.")
			else:
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ClientMessenger: connect_to_server: self.__client_socket_factory.get_client_socket(): start")
				self.__client_socket = self.__client_socket_factory.get_client_socket()
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ClientMessenger: connect_to_server: self.__client_socket_factory.get_client_socket(): end")
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ClientMessenger: connect_to_server: connect_to_server: start: host_address: {self.__server_host_pointer.get_host_address()}. host_port: {self.__server_host_pointer.get_host_port()}.")
				self.__client_socket.connect_to_server(
					ip_address=self.__server_host_pointer.get_host_address(),
					port=self.__server_host_pointer.get_host_port()
				)
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ClientMessenger: connect_to_server: connect_to_server: end")
		finally:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ClientMessenger: connect_to_server: end")

	def send_to_server(self, *, client_server_message: ClientServerMessage):
		message = json.dumps(client_server_message.to_json())
		self.__client_socket.write(message)

	def __receive_from_server(self, read_only_async_handle: ReadOnlyAsyncHandle, callback: Callable[[ClientServerMessage], None], on_exception: Callable[[Exception], None]):

		if not read_only_async_handle.is_cancelled():
			try:
				while not read_only_async_handle.is_cancelled():

					if self.__is_debug:
						print(f"{datetime.utcnow()}: ClientMessenger: {self.__debug_uuid}: receive_from_server: read start")

					client_server_message_json_string = self.__client_socket.read()

					if self.__is_debug:
						print(f"{datetime.utcnow()}: ClientMessenger: {self.__debug_uuid}: receive_from_server: read end: {client_server_message_json_string}")

					if not self.__is_closing:

						client_server_message = self.__client_server_message_class.parse_from_json(
							json_object=json.loads(client_server_message_json_string),

						)  # type: ClientServerMessage

						if self.__is_debug:
							print(f"{datetime.utcnow()}: ClientMessenger: {self.__debug_uuid}: receive_from_server: parsed: {client_server_message.__class__.get_client_server_message_type()}")

						callback(client_server_message)

						if self.__is_debug:
							print(f"{datetime.utcnow()}: ClientMessenger: {self.__debug_uuid}: receive_from_server: callback completed")

					else:
						if not read_only_async_handle.is_cancelled():
							raise Exception(f"Unexpected closing while read_only_async_handle is not cancelled.")

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ClientMessenger: {self.__debug_uuid}: receive_from_server: cancelled")

			except Exception as ex:
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ClientMessenger: {self.__debug_uuid}: receive_from_server: ex: {ex}")
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
			print(f"{datetime.utcnow()}: ClientMessenger: {self.__debug_uuid}: dispose: start")

		self.__is_closing = True

		if self.__receive_from_server_async_handle is not None:
			self.__receive_from_server_async_handle.cancel()

		if self.__client_socket is not None:

			if self.__is_debug:
				print(f"{datetime.utcnow()}: ClientMessenger: {self.__debug_uuid}: dispose: closing client socket")

			self.__client_socket.close()

			if self.__is_debug:
				print(f"{datetime.utcnow()}: ClientMessenger: {self.__debug_uuid}: dispose: closed client socket")

		if self.__receive_from_server_async_handle is not None:
			self.__receive_from_server_async_handle.get_result()

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ClientMessenger: {self.__debug_uuid}: dispose: end")


class ClientMessengerFactory():

	def __init__(self, *, client_socket_factory: ClientSocketFactory, server_host_pointer: HostPointer, client_server_message_class: Type[ClientServerMessage], is_debug: bool = False):

		self.__client_socket_factory = client_socket_factory
		self.__server_host_pointer = server_host_pointer
		self.__client_server_message_class = client_server_message_class
		self.__is_debug = is_debug

	def get_client_messenger(self) -> ClientMessenger:
		return ClientMessenger(
			client_socket_factory=self.__client_socket_factory,
			server_host_pointer=self.__server_host_pointer,
			client_server_message_class=self.__client_server_message_class,
			is_debug=self.__is_debug
		)


class SourceTypeEnum(StringEnum):
	pass


class SocketQueuedMessage():

	def __init__(self, *, source_uuid: str, source_type: SourceTypeEnum, client_server_message: ClientServerMessage, create_datetime: datetime, message_uuid: str):

		self.__source_uuid = source_uuid
		self.__source_type = source_type
		self.__client_server_message = client_server_message
		self.__create_datetime = create_datetime
		self.__message_uuid = message_uuid

	def get_source_uuid(self) -> str:
		return self.__source_uuid

	def get_source_type(self) -> SourceTypeEnum:
		return self.__source_type

	def get_client_server_message(self) -> ClientServerMessage:
		return self.__client_server_message

	def get_create_datetime(self) -> datetime:
		return self.__create_datetime

	def get_message_uuid(self) -> str:
		return self.__message_uuid

	def to_json(self) -> Dict:
		return {
			"source_uuid": self.get_source_uuid(),
			"source_type": self.get_source_type().value if self.get_source_type() is not None else None,
			"message": self.get_client_server_message().to_json(),
			"create_datetime": self.get_create_datetime().strftime("%Y-%m-%d %H:%M:%S.%f"),
			"message_uuid": self.get_message_uuid()
		}

	@staticmethod
	def parse_from_json(*, json_object: Dict, client_server_message_class: Type[ClientServerMessage], source_type_enum_class: Type[SourceTypeEnum]) -> SocketQueuedMessage:
		source_type = json_object["source_type"]
		return SocketQueuedMessage(
			source_uuid=json_object["source_uuid"],
			source_type=source_type_enum_class(source_type) if source_type is not None else None,
			client_server_message=client_server_message_class.parse_from_json(
				json_object=json_object["message"]
			),
			create_datetime=datetime.strptime(json_object["create_datetime"], "%Y-%m-%d %H:%M:%S.%f"),
			message_uuid=json_object["message_uuid"]
		)


class StructureStateEnum(StringEnum):
	pass


class StructureInfluence():

	def __init__(self, *, client_server_message: ClientServerMessage, source_uuid: str, source_type: SourceTypeEnum):

		self.__client_server_message = client_server_message
		self.__source_uuid = source_uuid
		self.__source_type = source_type

	def get_client_server_message(self) -> ClientServerMessage:
		return self.__client_server_message

	def get_source_uuid(self) -> str:
		return self.__source_uuid

	def get_source_type(self) -> SourceTypeEnum:
		return self.__source_type


class StructureTransition():

	def __init__(self, *, destination_structure_state: StructureStateEnum, on_transition: Callable[[StructureInfluence], None]):

		self.__destination_structure_state = destination_structure_state
		self.__on_transition = on_transition

	def get_destination_structure_state(self) -> StructureStateEnum:
		return self.__destination_structure_state

	def get_on_transition(self) -> Callable[[StructureInfluence], None]:
		return self.__on_transition


class StructureTransitionException(Exception, ABC):

	def __init__(self, *, structure_influence: StructureInfluence, structure_state: StructureStateEnum):
		super().__init__()

		self.__structure_influence = structure_influence
		self.__structure_state = structure_state

	def get_structure_influence(self) -> StructureInfluence:
		return self.__structure_influence

	def get_structure_state(self) -> StructureStateEnum:
		return self.__structure_state

	def __str__(self):
		return str(type(self))


class StructureTransitionMissingTriggerException(StructureTransitionException):

	def __init__(self, *, structure_influence: StructureInfluence, structure_state: StructureStateEnum):
		super().__init__(
			structure_influence=structure_influence,
			structure_state=structure_state
		)

	def __str__(self):
		return str(type(self))


class StructureTransitionMissingSourceTypeException(StructureTransitionException):

	def __init__(self, *, structure_influence: StructureInfluence, structure_state: StructureStateEnum):
		super().__init__(
			structure_influence=structure_influence,
			structure_state=structure_state
		)

	def __str__(self):
		return str(type(self))


class StructureTransitionMissingStartStructureStateException(StructureTransitionException):

	def __init__(self, *, structure_influence: StructureInfluence, structure_state: StructureStateEnum):
		super().__init__(
			structure_influence=structure_influence,
			structure_state=structure_state
		)

	def __str__(self):
		return str(type(self))


class Structure(ABC):

	def __init__(self, *, states: Type[StructureStateEnum], initial_state: StructureStateEnum):

		self.__states = states
		self.__initial_state = initial_state

		self.__found_exception = None  # type: Exception
		self.__current_state = None  # type: StructureStateEnum
		self.__on_response = None  # type: Callable[[ClientServerMessage], None]
		self.__registered_child_structures = []  # type: List[Structure]
		self.__registered_child_structures_semaphore = Semaphore()
		self.__next_state = None  # type: StructureStateEnum
		self.__transitions = {}  # type: Dict[ClientServerMessageTypeEnum, Dict[SourceTypeEnum, Dict[StructureStateEnum, StructureTransition]]]
		self.__bound_client_messenger_per_source_uuid = {}  # type: Dict[str, ClientMessenger]
		self.__bound_client_messenger_per_source_uuid_semaphore = Semaphore()
		self.__update_structure_semaphore = Semaphore()

		self.__initialize()

	def __initialize(self):

		self.__current_state = self.__initial_state

	def get_state(self) -> StructureStateEnum:
		return self.__current_state

	def set_state(self, *, structure_state: StructureStateEnum):
		self.__current_state = structure_state

	def set_on_response(self, *, on_response: Callable[[ClientServerMessage], None]):
		self.__on_response = on_response

	def __child_structure_on_response(self, client_server_message: ClientServerMessage):
		self.send_client_server_message(
			client_server_message=client_server_message
		)

	def register_child_structure(self, *, structure: Structure):
		self.__registered_child_structures_semaphore.acquire()
		try:
			structure.set_on_response(
				on_response=self.__child_structure_on_response
			)
			self.__registered_child_structures.append(structure)
		finally:
			self.__registered_child_structures_semaphore.release()

	@abstractmethod
	def on_client_connected(self, *, source_uuid: str, source_type: SourceTypeEnum):
		raise NotImplementedError()

	def connect_to_outbound_messenger(self, *, client_messenger_factory: ClientMessengerFactory, source_type: SourceTypeEnum):

		source_uuid = str(uuid.uuid4())

		def callback(client_server_message: ClientServerMessage):
			nonlocal source_uuid
			nonlocal source_type
			structure_influence = StructureInfluence(
				client_server_message=client_server_message,
				source_uuid=source_uuid,
				source_type=source_type
			)
			self.update_structure(
				structure_influence=structure_influence
			)

		def on_exception(exception: Exception):
			if self.__found_exception is None:
				self.__found_exception = exception

		client_messenger = client_messenger_factory.get_client_messenger()

		self.__bound_client_messenger_per_source_uuid_semaphore.acquire()
		self.__bound_client_messenger_per_source_uuid[source_uuid] = client_messenger
		self.__bound_client_messenger_per_source_uuid_semaphore.release()

		client_messenger.connect_to_server()
		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		self.on_client_connected(
			source_uuid=source_uuid,
			source_type=source_type
		)

	def update_structure(self, *, structure_influence: StructureInfluence):
		self.__update_structure_semaphore.acquire()
		try:
			client_server_message_type = structure_influence.get_client_server_message().__class__.get_client_server_message_type()
			if client_server_message_type not in self.__transitions:
				raise StructureTransitionMissingTriggerException(
					structure_influence=structure_influence,
					structure_state=self.__current_state
				)
			source_type = structure_influence.get_source_type()
			if source_type not in self.__transitions[client_server_message_type]:
				raise StructureTransitionMissingSourceTypeException(
					structure_influence=structure_influence,
					structure_state=self.__current_state
				)
			structure_transition_per_start_structure_state = self.__transitions[client_server_message_type][source_type]
			if self.__current_state not in structure_transition_per_start_structure_state:
				raise StructureTransitionMissingStartStructureStateException(
					structure_influence=structure_influence,
					structure_state=self.__current_state
				)
			structure_transition = structure_transition_per_start_structure_state[self.__current_state]  # type: StructureTransition
			self.__current_state = structure_transition.get_destination_structure_state()
			structure_transition.get_on_transition()(structure_influence)
		except StructureTransitionException as ex:
			print(f"update_structure: ex: StructureTransitionException: state: {ex.get_structure_state()}")
			print(f"update_structure: ex: StructureTransitionException: type: {ex.get_structure_influence().get_client_server_message().__class__.get_client_server_message_type()}")
			print(f"update_structure: ex: StructureTransitionException: source: {ex.get_structure_influence().get_source_uuid()}")
			raise
		except Exception as ex:
			print(f"update_structure: ex: {ex}")
			raise
		finally:
			self.__update_structure_semaphore.release()

	def send_client_server_message(self, *, client_server_message: ClientServerMessage):
		bound_client_messenger = self.__bound_client_messenger_per_source_uuid.get(client_server_message.get_destination_uuid(), None)
		if bound_client_messenger is None:
			if self.__on_response is None:
				raise Exception(f"Must first set on_response before expecting responses to be processed by this structure or ensure that the source_uuid is correct for the bound client. Type: {type(self)}. ClientServerMessage: {type(client_server_message)}. destination_uuid: {client_server_message.get_destination_uuid()}.")
			self.__on_response(client_server_message)
		else:
			bound_client_messenger.send_to_server(
				client_server_message=client_server_message
			)

	def add_transition(self, *, client_server_message_type: ClientServerMessageTypeEnum, from_source_type: SourceTypeEnum, start_structure_state: StructureStateEnum, end_structure_state: StructureStateEnum, on_transition: Callable[[StructureInfluence], None]):
		if client_server_message_type not in self.__transitions:
			self.__transitions[client_server_message_type] = {}
		if from_source_type not in self.__transitions[client_server_message_type]:
			self.__transitions[client_server_message_type][from_source_type] = {}
		if start_structure_state in self.__transitions[client_server_message_type][from_source_type]:
			raise Exception(f"Unexpected duplicate trigger/state for transition. Trigger: {client_server_message_type}. Source: {from_source_type}. State: {start_structure_state}.")
		self.__transitions[client_server_message_type][from_source_type][start_structure_state] = StructureTransition(
			destination_structure_state=end_structure_state,
			on_transition=on_transition
		)

	def dispose(self):
		for client_messenger in self.__bound_client_messenger_per_source_uuid.values():
			client_messenger.dispose()
		for structure in self.__registered_child_structures:
			structure.dispose()


class StructureFactory(ABC):

	@abstractmethod
	def get_structure(self) -> Structure:
		raise NotImplementedError()


class ServerMessenger():

	def __init__(self, *, server_socket_factory_and_local_host_pointer_per_source_type: Dict[SourceTypeEnum, Tuple[ServerSocketFactory, HostPointer]], client_server_message_class: Type[ClientServerMessage], source_type_enum_class: Type[SourceTypeEnum], server_messenger_source_type: SourceTypeEnum, structure_factory: StructureFactory, is_debug: bool = False):

		self.__server_socket_factory_and_local_host_pointer_per_source_type = server_socket_factory_and_local_host_pointer_per_source_type
		self.__client_server_message_class = client_server_message_class
		self.__source_type_enum_class = source_type_enum_class
		self.__server_messenger_source_type = server_messenger_source_type
		self.__structure_factory = structure_factory
		self.__is_debug = is_debug

		self.__server_messenger_uuid = str(uuid.uuid4())
		self.__server_sockets = []  # type: List[ServerSocket]
		self.__is_receiving_from_clients = False
		self.__client_sockets_per_source_uuid = {}  # type: Dict[str, ClientSocket]
		self.__client_sockets_per_source_uuid_semaphore = Semaphore()
		self.__structure = self.__structure_factory.get_structure()
		self.__on_accepted_client_method_exception = None  # type: Exception
		self.__on_accepted_client_method_exception_semaphore = Semaphore()
		self.__process_read_async_handle_per_source_uuid = {}  # type: Dict[str, AsyncHandle]
		self.__process_read_async_handle_per_source_uuid_semaphore = Semaphore()
		self.__on_response_from_structure_async_handle = None  # type: ConstantAsyncHandle

		self.__initialize()

	def __initialize(self):

		self.__on_response_from_structure_async_handle = ConstantAsyncHandle(
			result=None
		)
		self.__structure.set_on_response(
			on_response=self.__on_response_from_structure
		)

	def __send_client_server_message_to_destination(self, client_server_message: ClientServerMessage):

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ServerMessenger: __send_client_server_message_to_destination: sending message to client socket")

		try:
			is_message_sent_to_source = False

			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __send_client_server_message_to_destination: searching in keys: {self.__client_sockets_per_source_uuid.keys()}")

			destination_uuid = client_server_message.get_destination_uuid()

			if destination_uuid is None:
				raise Exception(f"Unexpected response client server message without destination uuid. Message: {client_server_message.to_json()}")
			else:
				# TODO stop every client from having to wait for a response here
				self.__client_sockets_per_source_uuid_semaphore.acquire()
				try:
					if destination_uuid in self.__client_sockets_per_source_uuid:
						self.__client_sockets_per_source_uuid[destination_uuid].write(json.dumps(client_server_message.to_json()))
						is_message_sent_to_source = True
				except ReadWriteSocketClosedException as ex:
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __send_client_server_message_to_destination: ReadWriteSocketClosedException ex: {ex}")
					self.__client_sockets_per_source_uuid[destination_uuid].close()
					del self.__client_sockets_per_source_uuid[destination_uuid]
				except Exception as ex:
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __send_client_server_message_to_destination: ex: {ex}")
					raise
				finally:
					self.__client_sockets_per_source_uuid_semaphore.release()

			if self.__is_debug:
				if is_message_sent_to_source:
					print(f"{datetime.utcnow()}: ServerMessenger: __send_client_server_message_to_destination: message sent")
				else:
					print(f"{datetime.utcnow()}: ServerMessenger: __send_client_server_message_to_destination: no message sent to a client socket")
		except Exception as ex:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __send_client_server_message_to_destination: ex: {ex}")
			raise
		finally:
			pass

	def __on_response_from_structure(self, client_server_message: ClientServerMessage):

		self.__process_client_server_message(
			read_only_async_handle=self.__on_response_from_structure_async_handle,
			client_server_message=client_server_message,
			source_uuid=self.__server_messenger_uuid,
			source_type=self.__server_messenger_source_type
		)

	def __process_client_server_message(self, read_only_async_handle: ReadOnlyAsyncHandle, client_server_message: ClientServerMessage, source_uuid: str, source_type: SourceTypeEnum):

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ServerMessenger: __process_client_server_message: start")

		try:
			if not read_only_async_handle.is_cancelled():
				if client_server_message.get_destination_uuid() in self.__client_sockets_per_source_uuid:
					# send message if this isn't meant to be processed on this structure
					self.__send_client_server_message_to_destination(
						client_server_message=client_server_message
					)
				else:
					# send message into the structure if it needs to be processed
					try:
						# NOTE tested calling self.__structure.send_response directly here and found no real performance increase
						self.__structure.update_structure(
							structure_influence=StructureInfluence(
								client_server_message=client_server_message,
								source_uuid=source_uuid,
								source_type=source_type
							)
						)
					except StructureTransitionException as ex:
						if self.__is_debug:
							print(f"{datetime.utcnow()}: ServerMessenger: __process_client_server_message: structure: ex: {ex}")
						response_client_server_message = client_server_message.get_structural_error_client_server_message_response(
							structure_transition_exception=ex,
							destination_uuid=source_uuid
						)
						if response_client_server_message is not None:
							if self.__is_debug:
								print(f"{datetime.utcnow()}: ServerMessenger: __process_client_server_message: structure: sending response")
							self.__process_client_server_message(
								read_only_async_handle=read_only_async_handle,
								client_server_message=response_client_server_message,
								source_uuid=self.__server_messenger_uuid,
								source_type=self.__server_messenger_source_type
							)
						else:
							if self.__is_debug:
								print(f"{datetime.utcnow()}: ServerMessenger: __process_client_server_message: structure: not sending response")
		finally:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __process_client_server_message: end")

	def __process_read_message_from_client_socket(self, read_only_async_handle: ReadOnlyAsyncHandle, source_uuid: str, message: str, source_type: SourceTypeEnum):

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ServerMessenger: __process_read_message_from_client_socket: start")

		try:
			if message is None:
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __process_read_message_from_client_socket: accepted socket sent empty message")
			else:
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __process_read_message_from_client_socket: process message: parsing: {message}")

				client_server_message = self.__client_server_message_class.parse_from_json(
					json_object=json.loads(message)
				)

				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: __process_read_message_from_client_socket: process message: parsed: {client_server_message.to_json()}")

				self.__process_client_server_message(
					read_only_async_handle=read_only_async_handle,
					client_server_message=client_server_message,
					source_uuid=source_uuid,
					source_type=source_type
				)
		except Exception as ex:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __process_read_message_from_client_socket: ex: {ex}")
			print(f"{datetime.utcnow()}: ServerMessenger: __process_read_message_from_client_socket: ex: {ex}. Message: {message}")
			raise ex
		finally:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: __process_read_message_from_client_socket: end")

	def __on_accepted_client_method(self, client_socket: ClientSocket, source_type: SourceTypeEnum):

		source_uuid = str(uuid.uuid4())

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: start: {source_uuid}")

		self.__client_sockets_per_source_uuid_semaphore.acquire()
		self.__client_sockets_per_source_uuid[source_uuid] = client_socket
		self.__client_sockets_per_source_uuid_semaphore.release()

		try:
			self.__structure.on_client_connected(
				source_uuid=source_uuid,
				source_type=source_type
			)

			while self.__is_receiving_from_clients and source_uuid in self.__client_sockets_per_source_uuid:

				try:
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: reading from client socket: start")

					client_server_message_json_string = client_socket.read()

					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: reading from client socket: successful: {client_server_message_json_string}")

				except ReadWriteSocketClosedException as ex:
					break
				finally:
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: __on_accepted_client_method: reading from client socket: end")

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
						message=client_server_message_json_string,
						source_type=source_type
					)

					self.__process_read_async_handle_per_source_uuid_semaphore.acquire()
					if source_uuid in self.__process_read_async_handle_per_source_uuid:
						del self.__process_read_async_handle_per_source_uuid[source_uuid]
					self.__process_read_async_handle_per_source_uuid_semaphore.release()

					# NOTE added a hard delay per client means that a burst of data will always take longer to process
					#time.sleep(0.002)

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

		if self.__server_sockets:
			raise Exception(f"Must first stop receiving from clients before starting.")
		else:
			self.__is_receiving_from_clients = True

			def get_on_accept_client_method(*, source_type: SourceTypeEnum):
				def on_accept_client_method(client_socket: ClientSocket):
					self.__on_accepted_client_method(client_socket, source_type)
				return on_accept_client_method

			for source_type, (server_socket_factory, local_host_pointer) in self.__server_socket_factory_and_local_host_pointer_per_source_type.items():
				server_socket = server_socket_factory.get_server_socket()
				server_socket.start_accepting_clients(
					host_ip_address=local_host_pointer.get_host_address(),
					host_port=local_host_pointer.get_host_port(),
					on_accepted_client_method=get_on_accept_client_method(
						source_type=source_type
					)
				)
				self.__server_sockets.append(server_socket)

	def stop_receiving_from_clients(self):

		if self.__is_debug:
			print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: start")
		try:
			if not self.__server_sockets:
				raise Exception(f"Must first start receiving from clients before stopping.")
			else:
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: for server_socket in self.__server_sockets")
				for server_socket in self.__server_sockets:
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: server_socket.stop_accepting_clients()")
					server_socket.stop_accepting_clients()
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: server_socket.close()")
					server_socket.close()
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__server_sockets.clear()")
				self.__server_sockets.clear()
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__is_receiving_from_clients = False")
				self.__is_receiving_from_clients = False
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__on_response_from_structure_async_handle.cancel()")
				self.__on_response_from_structure_async_handle.cancel()
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__process_read_async_handle_per_source_uuid_semaphore.acquire()")
				self.__process_read_async_handle_per_source_uuid_semaphore.acquire()
				for source_uuid in self.__process_read_async_handle_per_source_uuid:
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__process_read_async_handle_per_source_uuid[source_uuid].cancel()")
					self.__process_read_async_handle_per_source_uuid[source_uuid].cancel()
					if self.__is_debug:
						print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__process_read_async_handle_per_source_uuid[source_uuid].get_result()")
					self.__process_read_async_handle_per_source_uuid[source_uuid].get_result()
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__process_read_async_handle_per_source_uuid_semaphore.release()")
				self.__process_read_async_handle_per_source_uuid_semaphore.release()
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__client_sockets_per_source_uuid_semaphore.acquire()")
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
				if self.__is_debug:
					print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: self.__client_sockets_per_source_uuid_semaphore.release()")
				self.__client_sockets_per_source_uuid_semaphore.release()

			self.__on_accepted_client_method_exception_semaphore.acquire()
			exception = self.__on_accepted_client_method_exception
			self.__on_accepted_client_method_exception = None
			self.__on_accepted_client_method_exception_semaphore.release()

			if exception is not None:
				raise exception
		finally:
			if self.__is_debug:
				print(f"{datetime.utcnow()}: ServerMessenger: stop_receiving_from_clients: end")

	def dispose(self):
		self.__structure.dispose()


class ServerMessengerFactory():

	def __init__(self, *, server_socket_factory_and_local_host_pointer_per_source_type: Dict[SourceTypeEnum, Tuple[ServerSocketFactory, HostPointer]], client_server_message_class: Type[ClientServerMessage], source_type_enum_class: Type[SourceTypeEnum], server_messenger_source_type: SourceTypeEnum, structure_factory: StructureFactory, is_debug: bool = False):

		self.__server_socket_factory_and_local_host_pointer_per_source_type = server_socket_factory_and_local_host_pointer_per_source_type
		self.__client_server_message_class = client_server_message_class
		self.__source_type_enum_class = source_type_enum_class
		self.__server_messenger_source_type = server_messenger_source_type
		self.__structure_factory = structure_factory
		self.__is_debug = is_debug

	def get_server_messenger(self) -> ServerMessenger:
		return ServerMessenger(
			server_socket_factory_and_local_host_pointer_per_source_type=self.__server_socket_factory_and_local_host_pointer_per_source_type,
			client_server_message_class=self.__client_server_message_class,
			source_type_enum_class=self.__source_type_enum_class,
			server_messenger_source_type=self.__server_messenger_source_type,
			structure_factory=self.__structure_factory,
			is_debug=self.__is_debug
		)
