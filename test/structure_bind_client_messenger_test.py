from __future__ import annotations
import unittest
from typing import List, Tuple, Dict, Type, Callable, Deque
from abc import ABC, abstractmethod
import uuid
from collections import deque
import time
from datetime import datetime
from src.austin_heller_repo.socket_queued_message_framework import Structure, StructureFactory, ClientServerMessage, SourceTypeEnum, ClientServerMessageTypeEnum, StructureStateEnum, ClientMessengerFactory, ClientMessenger, StructureTransitionException, StructureInfluence, ServerMessenger, ServerMessengerFactory, ServerSocketFactory, HostPointer, ClientSocketFactory
from austin_heller_repo.threading import SingletonMemorySequentialQueueFactory, Semaphore
from austin_heller_repo.common import StringEnum, SingleDependentDependencyManager, AggregateDependentDependencyManager


is_debug = False


def get_default_message_manager_client_structure_factory() -> MessageManagerClientStructureFactory:
	return MessageManagerClientStructureFactory(
		message_manager_client_messenger_factory=ClientMessengerFactory(
			client_socket_factory=ClientSocketFactory(
				to_server_packet_bytes_length=4096
			),
			server_host_pointer=HostPointer(
				host_address="localhost",
				host_port=37440
			),
			client_server_message_class=MessageManagerClientServerMessage,
			is_debug=is_debug
		)
	)


def get_default_message_manager_server_messenger_factory() -> ServerMessengerFactory:
	return ServerMessengerFactory(
		server_socket_factory_and_local_host_pointer_per_source_type={
			MessageManagerSourceTypeEnum.Client: (
				ServerSocketFactory(
					to_client_packet_bytes_length=4096,
					listening_limit_total=10,
					accept_timeout_seconds=1.0
				),
				HostPointer(
					host_address="localhost",
					host_port=37440
				)
			)
		},
		sequential_queue_factory=SingletonMemorySequentialQueueFactory(),
		client_server_message_class=MessageManagerClientServerMessage,
		source_type_enum_class=MessageManagerSourceTypeEnum,
		server_messenger_source_type=MessageManagerSourceTypeEnum.MessageManager,
		structure_factory=MessageManagerStructureFactory(
			uppercase_manager_client_messenger_factory=ClientMessengerFactory(
				client_socket_factory=ClientSocketFactory(
					to_server_packet_bytes_length=4096
				),
				server_host_pointer=HostPointer(
					host_address="localhost",
					host_port=37441
				),
				client_server_message_class=UppercaseManagerClientServerMessage
			),
			reverse_manager_client_messenger_factory=ClientMessengerFactory(
				client_socket_factory=ClientSocketFactory(
					to_server_packet_bytes_length=4096
				),
				server_host_pointer=HostPointer(
					host_address="localhost",
					host_port=37442
				),
				client_server_message_class=ReverseManagerClientServerMessage
			)
		),
		is_debug=is_debug
	)


def get_default_uppercase_manager_server_messenger_factory() -> ServerMessengerFactory:
	return ServerMessengerFactory(
		server_socket_factory_and_local_host_pointer_per_source_type={
			UppercaseManagerSourceTypeEnum.MessageManager: (
				ServerSocketFactory(
					to_client_packet_bytes_length=4096,
					listening_limit_total=10,
					accept_timeout_seconds=1.0
				),
				HostPointer(
					host_address="localhost",
					host_port=37441
				)
			)
		},
		sequential_queue_factory=SingletonMemorySequentialQueueFactory(),
		client_server_message_class=UppercaseManagerClientServerMessage,
		source_type_enum_class=UppercaseManagerSourceTypeEnum,
		server_messenger_source_type=UppercaseManagerSourceTypeEnum.UppercaseManager,
		structure_factory=UppercaseManagerStructureFactory(),
		is_debug=is_debug
	)


def get_default_reverse_manager_server_messenger_factory() -> ServerMessengerFactory:
	return ServerMessengerFactory(
		server_socket_factory_and_local_host_pointer_per_source_type={
			ReverseManagerSourceTypeEnum.MessageManager: (
				ServerSocketFactory(
					to_client_packet_bytes_length=4096,
					listening_limit_total=10,
					accept_timeout_seconds=1.0
				),
				HostPointer(
					host_address="localhost",
					host_port=37442
				)
			)
		},
		sequential_queue_factory=SingletonMemorySequentialQueueFactory(),
		client_server_message_class=ReverseManagerClientServerMessage,
		source_type_enum_class=ReverseManagerSourceTypeEnum,
		server_messenger_source_type=ReverseManagerSourceTypeEnum.ReverseManager,
		structure_factory=ReverseManagerStructureFactory(),
		is_debug=is_debug
	)


class MessageManagerSourceTypeEnum(SourceTypeEnum):
	MessageManager = "message_manager"
	Client = "client"
	UppercaseManager = "uppercase_manager"
	ReverseManager = "reverse_manager"


class MessageManagerClientServerMessageTypeEnum(ClientServerMessageTypeEnum):
	FormatMessageRequest = "format_message_request"
	FormatMessageResponse = "format_message_response"


class MessageManagerStructureStateEnum(StructureStateEnum):
	Active = "active"


class FormatTypeEnum(StringEnum):
	Uppercase = "uppercase"
	Reverse = "reverse"


class MessageManagerClientServerMessage(ClientServerMessage, ABC):

	@classmethod
	def get_client_server_message_type_class(cls) -> Type[ClientServerMessageTypeEnum]:
		return MessageManagerClientServerMessageTypeEnum


class FormatMessageRequestMessageManagerClientServerMessage(MessageManagerClientServerMessage):

	def __init__(self, *, message: str, format_types: List[str], destination_uuid: str):
		super().__init__(
			destination_uuid=destination_uuid
		)

		self.__message = message
		self.__format_types = [FormatTypeEnum(x) for x in format_types]

	def get_message(self) -> str:
		return self.__message

	def get_format_types(self) -> List[FormatTypeEnum]:
		return self.__format_types

	@classmethod
	def get_client_server_message_type(cls) -> ClientServerMessageTypeEnum:
		return MessageManagerClientServerMessageTypeEnum.FormatMessageRequest

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["message"] = self.__message
		json_object["format_types"] = [x.value for x in self.__format_types]
		return json_object

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, structure_transition_exception: StructureTransitionException, destination_uuid: str) -> ClientServerMessage:
		return None


class FormatMessageResponseMessageManagerClientServerMessage(MessageManagerClientServerMessage):

	def __init__(self, *, message: str, destination_uuid: str,):
		super().__init__(
			destination_uuid=destination_uuid
		)

		self.__message = message

	def get_message(self) -> str:
		return self.__message

	@classmethod
	def get_client_server_message_type(cls) -> ClientServerMessageTypeEnum:
		return MessageManagerClientServerMessageTypeEnum.FormatMessageResponse

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["message"] = self.__message
		return json_object

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, structure_transition_exception: StructureTransitionException, destination_uuid: str) -> ClientServerMessage:
		return None


# uppercase
class UppercaseManagerSourceTypeEnum(SourceTypeEnum):
	UppercaseManager = "uppercase_manager"
	MessageManager = "message_manager"


class UppercaseManagerClientServerMessageTypeEnum(ClientServerMessageTypeEnum):
	FormatMessageRequest = "format_message_request"
	FormatMessageResponse = "format_message_response"


class UppercaseManagerStructureStateEnum(StructureStateEnum):
	Active = "active"


class UppercaseManagerClientServerMessage(ClientServerMessage, ABC):

	@classmethod
	def get_client_server_message_type_class(cls) -> Type[ClientServerMessageTypeEnum]:
		return UppercaseManagerClientServerMessageTypeEnum


class FormatMessageRequestUppercaseManagerClientServerMessage(UppercaseManagerClientServerMessage):

	def __init__(self, *, message: str, message_uuid: str, destination_uuid: str):
		super().__init__(
			destination_uuid=destination_uuid
		)

		self.__message = message
		self.__message_uuid = message_uuid

	def get_message(self) -> str:
		return self.__message

	def get_message_uuid(self) -> str:
		return self.__message_uuid

	@classmethod
	def get_client_server_message_type(cls) -> ClientServerMessageTypeEnum:
		return UppercaseManagerClientServerMessageTypeEnum.FormatMessageRequest

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["message"] = self.__message
		json_object["message_uuid"] = self.__message_uuid
		return json_object

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, structure_transition_exception: StructureTransitionException, destination_uuid: str) -> ClientServerMessage:
		return None


class FormatMessageResponseUppercaseManagerClientServerMessage(UppercaseManagerClientServerMessage):

	def __init__(self, *, message: str, message_uuid: str, destination_uuid: str):
		super().__init__(
			destination_uuid=destination_uuid
		)

		self.__message = message
		self.__message_uuid = message_uuid

	def get_message(self) -> str:
		return self.__message

	def get_message_uuid(self) -> str:
		return self.__message_uuid

	@classmethod
	def get_client_server_message_type(cls) -> ClientServerMessageTypeEnum:
		return UppercaseManagerClientServerMessageTypeEnum.FormatMessageResponse

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["message"] = self.__message
		json_object["message_uuid"] = self.__message_uuid
		return json_object

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, structure_transition_exception: StructureTransitionException, destination_uuid: str) -> ClientServerMessage:
		return None


# reverse
class ReverseManagerSourceTypeEnum(SourceTypeEnum):
	ReverseManager = "reverse_manager"
	MessageManager = "message_manager"


class ReverseManagerClientServerMessageTypeEnum(ClientServerMessageTypeEnum):
	FormatMessageRequest = "format_message_request"
	FormatMessageResponse = "format_message_response"


class ReverseManagerStructureStateEnum(StructureStateEnum):
	Active = "active"


class ReverseManagerClientServerMessage(ClientServerMessage, ABC):

	@classmethod
	def get_client_server_message_type_class(cls) -> Type[ClientServerMessageTypeEnum]:
		return ReverseManagerClientServerMessageTypeEnum


class FormatMessageRequestReverseManagerClientServerMessage(ReverseManagerClientServerMessage):

	def __init__(self, *, message: str, message_uuid: str, destination_uuid: str):
		super().__init__(
			destination_uuid=destination_uuid
		)

		self.__message = message
		self.__message_uuid = message_uuid

	def get_message(self) -> str:
		return self.__message

	def get_message_uuid(self) -> str:
		return self.__message_uuid

	@classmethod
	def get_client_server_message_type(cls) -> ClientServerMessageTypeEnum:
		return ReverseManagerClientServerMessageTypeEnum.FormatMessageRequest

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["message"] = self.__message
		json_object["message_uuid"] = self.__message_uuid
		return json_object

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, structure_transition_exception: StructureTransitionException, destination_uuid: str) -> ClientServerMessage:
		return None


class FormatMessageResponseReverseManagerClientServerMessage(ReverseManagerClientServerMessage):

	def __init__(self, *, message: str, message_uuid: str, destination_uuid: str):
		super().__init__(
			destination_uuid=destination_uuid
		)

		self.__message = message
		self.__message_uuid = message_uuid

	def get_message(self) -> str:
		return self.__message

	def get_message_uuid(self) -> str:
		return self.__message_uuid

	@classmethod
	def get_client_server_message_type(cls) -> ClientServerMessageTypeEnum:
		return ReverseManagerClientServerMessageTypeEnum.FormatMessageResponse

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["message"] = self.__message
		json_object["message_uuid"] = self.__message_uuid
		return json_object

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, structure_transition_exception: StructureTransitionException, destination_uuid: str) -> ClientServerMessage:
		return None


# structures
class MessageManagerStructure(Structure):

	def __init__(self, *, uppercase_manager_client_messenger_factory: ClientMessengerFactory, reverse_manager_client_messenger_factory: ClientMessengerFactory):
		super().__init__(
			states=MessageManagerStructureStateEnum,
			initial_state=MessageManagerStructureStateEnum.Active
		)

		self.__uppercase_manager_client_messenger_factory = uppercase_manager_client_messenger_factory
		self.__reverse_manager_client_messenger_factory = reverse_manager_client_messenger_factory

		self.__uppercase_manager_client_messenger_source_uuid = None  # type: str
		self.__reverse_manager_client_messenger_source_uuid = None  # type: str
		self.__message_per_message_uuid = {}  # type: Dict[str, str]
		self.__format_types_per_message_uuid = {}  # type: Dict[str, Deque[FormatTypeEnum]]
		self.__source_uuid_per_message_uuid = {}  # type: Dict[str, str]

		self.add_transition(
			client_server_message_type=MessageManagerClientServerMessageTypeEnum.FormatMessageRequest,
			from_source_type=MessageManagerSourceTypeEnum.Client,
			start_structure_state=MessageManagerStructureStateEnum.Active,
			end_structure_state=MessageManagerStructureStateEnum.Active,
			on_transition=self.__format_message_request_transition
		)

		self.add_transition(
			client_server_message_type=UppercaseManagerClientServerMessageTypeEnum.FormatMessageResponse,
			from_source_type=MessageManagerSourceTypeEnum.UppercaseManager,
			start_structure_state=MessageManagerStructureStateEnum.Active,
			end_structure_state=MessageManagerStructureStateEnum.Active,
			on_transition=self.__format_message_response_transition
		)

		self.add_transition(
			client_server_message_type=ReverseManagerClientServerMessageTypeEnum.FormatMessageResponse,
			from_source_type=MessageManagerSourceTypeEnum.ReverseManager,
			start_structure_state=MessageManagerStructureStateEnum.Active,
			end_structure_state=MessageManagerStructureStateEnum.Active,
			on_transition=self.__format_message_response_transition
		)

		self.__initialize()

	def __initialize(self):

		self.__uppercase_manager_client_messenger_source_uuid = self.bind_client_messenger(
			client_messenger_factory=self.__uppercase_manager_client_messenger_factory,
			source_type=MessageManagerSourceTypeEnum.UppercaseManager
		)

		self.__reverse_manager_client_messenger_source_uuid = self.bind_client_messenger(
			client_messenger_factory=self.__reverse_manager_client_messenger_factory,
			source_type=MessageManagerSourceTypeEnum.ReverseManager
		)

	def __process_next_format_type_for_message_uuid(self, *, message_uuid: str):

		message = self.__message_per_message_uuid[message_uuid]
		if message_uuid not in self.__format_types_per_message_uuid:
			del self.__message_per_message_uuid[message_uuid]
			source_uuid = self.__source_uuid_per_message_uuid[message_uuid]
			del self.__source_uuid_per_message_uuid[message_uuid]
			self.send_client_server_message(
				client_server_message=FormatMessageResponseMessageManagerClientServerMessage(
					destination_uuid=source_uuid,
					message=message
				)
			)
		else:
			format_type = self.__format_types_per_message_uuid[message_uuid].popleft()
			if not self.__format_types_per_message_uuid[message_uuid]:
				del self.__format_types_per_message_uuid[message_uuid]
			if format_type == FormatTypeEnum.Uppercase:
				self.send_client_server_message(
					client_server_message=FormatMessageRequestUppercaseManagerClientServerMessage(
						message=message,
						message_uuid=message_uuid,
						destination_uuid=self.__uppercase_manager_client_messenger_source_uuid
					)
				)
			elif format_type == FormatTypeEnum.Reverse:
				self.send_client_server_message(
					client_server_message=FormatMessageRequestReverseManagerClientServerMessage(
						message=message,
						message_uuid=message_uuid,
						destination_uuid=self.__reverse_manager_client_messenger_source_uuid
					)
				)
			else:
				raise NotImplementedError()

	def __format_message_request_transition(self, structure_influence: StructureInfluence):

		client_server_message = structure_influence.get_client_server_message()
		if isinstance(client_server_message, FormatMessageRequestMessageManagerClientServerMessage):
			message = client_server_message.get_message()
			format_types = client_server_message.get_format_types()
			source_uuid = structure_influence.get_source_uuid()
			message_uuid = str(uuid.uuid4())
			self.__message_per_message_uuid[message_uuid] = message
			self.__format_types_per_message_uuid[message_uuid] = deque(format_types)
			self.__source_uuid_per_message_uuid[message_uuid] = source_uuid
			self.__process_next_format_type_for_message_uuid(
				message_uuid=message_uuid
			)
		else:
			raise NotImplementedError()

	def __format_message_response_transition(self, structure_influence: StructureInfluence):

		client_server_message = structure_influence.get_client_server_message()
		if isinstance(client_server_message, FormatMessageResponseUppercaseManagerClientServerMessage):
			message = client_server_message.get_message()
			message_uuid = client_server_message.get_message_uuid()
			self.__message_per_message_uuid[message_uuid] = message
			self.__process_next_format_type_for_message_uuid(
				message_uuid=message_uuid
			)
		elif isinstance(client_server_message, FormatMessageResponseReverseManagerClientServerMessage):
			message = client_server_message.get_message()
			message_uuid = client_server_message.get_message_uuid()
			self.__message_per_message_uuid[message_uuid] = message
			self.__process_next_format_type_for_message_uuid(
				message_uuid=message_uuid
			)
		else:
			raise NotImplementedError()


class MessageManagerStructureFactory(StructureFactory):

	def __init__(self, *, uppercase_manager_client_messenger_factory: ClientMessengerFactory, reverse_manager_client_messenger_factory: ClientMessengerFactory):

		self.__uppercase_manager_client_messenger_factory = uppercase_manager_client_messenger_factory
		self.__reverse_manager_client_messenger_factory = reverse_manager_client_messenger_factory

	def get_structure(self) -> Structure:
		return MessageManagerStructure(
			uppercase_manager_client_messenger_factory=self.__uppercase_manager_client_messenger_factory,
			reverse_manager_client_messenger_factory=self.__reverse_manager_client_messenger_factory
		)


class UppercaseManagerStructure(Structure):

	def __init__(self):
		super().__init__(
			states=UppercaseManagerStructureStateEnum,
			initial_state=UppercaseManagerStructureStateEnum.Active
		)

		self.add_transition(
			client_server_message_type=UppercaseManagerClientServerMessageTypeEnum.FormatMessageRequest,
			from_source_type=UppercaseManagerSourceTypeEnum.MessageManager,
			start_structure_state=UppercaseManagerStructureStateEnum.Active,
			end_structure_state=UppercaseManagerStructureStateEnum.Active,
			on_transition=self.__format_message_request_transition
		)

	def __format_message_request_transition(self, structure_influence: StructureInfluence):

		client_server_message = structure_influence.get_client_server_message()
		if isinstance(client_server_message, FormatMessageRequestUppercaseManagerClientServerMessage):
			print(f"{datetime.utcnow()}: UppercaseManagerStructure: received uppercase format request")
			message = client_server_message.get_message()
			message_uuid = client_server_message.get_message_uuid()
			formatted_message = message.upper()
			self.send_client_server_message(
				client_server_message=FormatMessageResponseUppercaseManagerClientServerMessage(
					destination_uuid=structure_influence.get_source_uuid(),
					message=formatted_message,
					message_uuid=message_uuid
				)
			)
		else:
			raise NotImplementedError()


class UppercaseManagerStructureFactory(StructureFactory):

	def get_structure(self) -> Structure:
		return UppercaseManagerStructure()


class ReverseManagerStructure(Structure):

	def __init__(self):
		super().__init__(
			states=ReverseManagerStructureStateEnum,
			initial_state=ReverseManagerStructureStateEnum.Active
		)

		self.add_transition(
			client_server_message_type=ReverseManagerClientServerMessageTypeEnum.FormatMessageRequest,
			from_source_type=ReverseManagerSourceTypeEnum.MessageManager,
			start_structure_state=ReverseManagerStructureStateEnum.Active,
			end_structure_state=ReverseManagerStructureStateEnum.Active,
			on_transition=self.__format_message_request_transition
		)

	def __format_message_request_transition(self, structure_influence: StructureInfluence):

		client_server_message = structure_influence.get_client_server_message()
		if isinstance(client_server_message, FormatMessageRequestReverseManagerClientServerMessage):
			message = client_server_message.get_message()
			message_uuid = client_server_message.get_message_uuid()
			formatted_message = message[::-1]
			self.send_client_server_message(
				client_server_message=FormatMessageResponseReverseManagerClientServerMessage(
					destination_uuid=structure_influence.get_source_uuid(),
					message=formatted_message,
					message_uuid=message_uuid
				)
			)
		else:
			raise NotImplementedError()


class ReverseManagerStructureFactory(StructureFactory):

	def get_structure(self) -> Structure:
		return ReverseManagerStructure()


class MessageManagerClientStructureStateEnum(StructureStateEnum):
	Active = "active"


class MessageManagerClientStructure(Structure):

	def __init__(self, *, message_manager_client_messenger_factory: ClientMessengerFactory):
		super().__init__(
			states=MessageManagerClientStructureStateEnum,
			initial_state=MessageManagerClientStructureStateEnum.Active
		)

		self.__message_manager_client_messenger_factory = message_manager_client_messenger_factory

		self.__message_manager_source_uuid = None  # type: str
		self.__method_semaphore = Semaphore()
		self.__blocking_semaphore = Semaphore()
		self.__previous_client_server_message = None  # type: ClientServerMessage

		self.add_transition(
			client_server_message_type=MessageManagerClientServerMessageTypeEnum.FormatMessageResponse,
			from_source_type=MessageManagerSourceTypeEnum.MessageManager,
			start_structure_state=MessageManagerClientStructureStateEnum.Active,
			end_structure_state=MessageManagerClientStructureStateEnum.Active,
			on_transition=self.__message_manager_client_structure_on_transition
		)

		self.__initialize()

	def __initialize(self):

		self.__blocking_semaphore.acquire()

		self.__message_manager_source_uuid = self.bind_client_messenger(
			client_messenger_factory=self.__message_manager_client_messenger_factory,
			source_type=MessageManagerSourceTypeEnum.MessageManager
		)

	def __message_manager_client_structure_on_transition(self, structure_influence: StructureInfluence):

		client_server_message = structure_influence.get_client_server_message()
		self.__previous_client_server_message = client_server_message
		self.__blocking_semaphore.release()

	def format_message(self, *, message: str, format_types: List[FormatTypeEnum]) -> str:
		self.__method_semaphore.acquire()
		try:
			self.send_client_server_message(
				client_server_message=FormatMessageRequestMessageManagerClientServerMessage(
					message=message,
					format_types=[x.value for x in format_types],
					destination_uuid=self.__message_manager_source_uuid
				)
			)
			self.__blocking_semaphore.acquire()
			if not isinstance(self.__previous_client_server_message, FormatMessageResponseMessageManagerClientServerMessage):
				raise Exception(f"Unexpected message: {self.__previous_client_server_message}")
			else:
				message = self.__previous_client_server_message.get_message()
		finally:
			self.__method_semaphore.release()
		return message


class MessageManagerClientStructureFactory(StructureFactory):

	def __init__(self, *, message_manager_client_messenger_factory: ClientMessengerFactory):

		self.__message_manager_client_messenger_factory = message_manager_client_messenger_factory

	def get_structure(self) -> MessageManagerClientStructure:
		return MessageManagerClientStructure(
			message_manager_client_messenger_factory=self.__message_manager_client_messenger_factory
		)


class StructureBindClientMessengerTest(unittest.TestCase):

	def test_initialize(self):

		uppercase_manager_server_messenger = get_default_uppercase_manager_server_messenger_factory().get_server_messenger()

		self.assertIsNotNone(uppercase_manager_server_messenger)

		uppercase_manager_server_messenger.start_receiving_from_clients()

		reverse_manager_server_messenger = get_default_reverse_manager_server_messenger_factory().get_server_messenger()

		self.assertIsNotNone(reverse_manager_server_messenger)

		reverse_manager_server_messenger.start_receiving_from_clients()

		message_manager_server_messenger = get_default_message_manager_server_messenger_factory().get_server_messenger()

		self.assertIsNotNone(message_manager_server_messenger)

		message_manager_server_messenger.start_receiving_from_clients()

		time.sleep(0.1)

		message_manager_server_messenger.stop_receiving_from_clients()

		message_manager_server_messenger.dispose()

		reverse_manager_server_messenger.stop_receiving_from_clients()

		reverse_manager_server_messenger.dispose()

		uppercase_manager_server_messenger.stop_receiving_from_clients()

		uppercase_manager_server_messenger.dispose()

	def test_uppercase(self):

		uppercase_manager_server_messenger = get_default_uppercase_manager_server_messenger_factory().get_server_messenger()

		uppercase_manager_server_messenger.start_receiving_from_clients()

		reverse_manager_server_messenger = get_default_reverse_manager_server_messenger_factory().get_server_messenger()

		reverse_manager_server_messenger.start_receiving_from_clients()

		message_manager_server_messenger = get_default_message_manager_server_messenger_factory().get_server_messenger()

		message_manager_server_messenger.start_receiving_from_clients()

		time.sleep(0.1)

		# send message

		message_manager_client_structure = get_default_message_manager_client_structure_factory().get_structure()

		formatted_message = message_manager_client_structure.format_message(
			message="test message",
			format_types=[
				FormatTypeEnum.Uppercase
			]
		)

		# waiting for messages

		time.sleep(1.0)

		message_manager_client_structure.dispose()

		message_manager_server_messenger.stop_receiving_from_clients()

		message_manager_server_messenger.dispose()

		reverse_manager_server_messenger.stop_receiving_from_clients()

		reverse_manager_server_messenger.dispose()

		uppercase_manager_server_messenger.stop_receiving_from_clients()

		uppercase_manager_server_messenger.dispose()

		self.assertEqual("TEST MESSAGE", formatted_message)

	def test_reverse(self):

		uppercase_manager_server_messenger = get_default_uppercase_manager_server_messenger_factory().get_server_messenger()

		uppercase_manager_server_messenger.start_receiving_from_clients()

		reverse_manager_server_messenger = get_default_reverse_manager_server_messenger_factory().get_server_messenger()

		reverse_manager_server_messenger.start_receiving_from_clients()

		message_manager_server_messenger = get_default_message_manager_server_messenger_factory().get_server_messenger()

		message_manager_server_messenger.start_receiving_from_clients()

		time.sleep(0.1)

		# send message

		message_manager_client_structure = get_default_message_manager_client_structure_factory().get_structure()

		for index in range(3):
			start_datetime = datetime.utcnow()
			formatted_message = message_manager_client_structure.format_message(
				message="test message",
				format_types=[
					FormatTypeEnum.Reverse
				]
			)
			end_datetime = datetime.utcnow()

			print(f"{datetime.utcnow()}: test: elapsed time: {(end_datetime - start_datetime).total_seconds()}")

		# waiting for messages

		time.sleep(1.0)

		message_manager_client_structure.dispose()

		message_manager_server_messenger.stop_receiving_from_clients()

		message_manager_server_messenger.dispose()

		reverse_manager_server_messenger.stop_receiving_from_clients()

		reverse_manager_server_messenger.dispose()

		uppercase_manager_server_messenger.stop_receiving_from_clients()

		uppercase_manager_server_messenger.dispose()

		self.assertEqual("egassem tset", formatted_message)

	def test_uppercase_reverse(self):

		uppercase_manager_server_messenger = get_default_uppercase_manager_server_messenger_factory().get_server_messenger()

		uppercase_manager_server_messenger.start_receiving_from_clients()

		reverse_manager_server_messenger = get_default_reverse_manager_server_messenger_factory().get_server_messenger()

		reverse_manager_server_messenger.start_receiving_from_clients()

		message_manager_server_messenger = get_default_message_manager_server_messenger_factory().get_server_messenger()

		message_manager_server_messenger.start_receiving_from_clients()

		time.sleep(0.1)

		# send message

		message_manager_client_structure = get_default_message_manager_client_structure_factory().get_structure()

		formatted_message = message_manager_client_structure.format_message(
			message="test message",
			format_types=[
				FormatTypeEnum.Uppercase,
				FormatTypeEnum.Reverse
			]
		)

		time.sleep(1.0)

		message_manager_client_structure.dispose()

		message_manager_server_messenger.stop_receiving_from_clients()

		message_manager_server_messenger.dispose()

		reverse_manager_server_messenger.stop_receiving_from_clients()

		reverse_manager_server_messenger.dispose()

		uppercase_manager_server_messenger.stop_receiving_from_clients()

		uppercase_manager_server_messenger.dispose()

		self.assertEqual("EGASSEM TSET", formatted_message)

	def test_reverse_reverse(self):

		uppercase_manager_server_messenger = get_default_uppercase_manager_server_messenger_factory().get_server_messenger()

		uppercase_manager_server_messenger.start_receiving_from_clients()

		reverse_manager_server_messenger = get_default_reverse_manager_server_messenger_factory().get_server_messenger()

		reverse_manager_server_messenger.start_receiving_from_clients()

		message_manager_server_messenger = get_default_message_manager_server_messenger_factory().get_server_messenger()

		message_manager_server_messenger.start_receiving_from_clients()

		time.sleep(0.1)

		# send message

		message_manager_client_structure = get_default_message_manager_client_structure_factory().get_structure()

		formatted_message = message_manager_client_structure.format_message(
			message="test message",
			format_types=[
				FormatTypeEnum.Reverse,
				FormatTypeEnum.Reverse
			]
		)

		time.sleep(1.0)

		message_manager_client_structure.dispose()

		message_manager_server_messenger.stop_receiving_from_clients()

		message_manager_server_messenger.dispose()

		reverse_manager_server_messenger.stop_receiving_from_clients()

		reverse_manager_server_messenger.dispose()

		uppercase_manager_server_messenger.stop_receiving_from_clients()

		uppercase_manager_server_messenger.dispose()

		self.assertEqual("test message", formatted_message)

	def test_reverse_reverse_reverse(self):

		uppercase_manager_server_messenger = get_default_uppercase_manager_server_messenger_factory().get_server_messenger()

		uppercase_manager_server_messenger.start_receiving_from_clients()

		reverse_manager_server_messenger = get_default_reverse_manager_server_messenger_factory().get_server_messenger()

		reverse_manager_server_messenger.start_receiving_from_clients()

		message_manager_server_messenger = get_default_message_manager_server_messenger_factory().get_server_messenger()

		message_manager_server_messenger.start_receiving_from_clients()

		time.sleep(0.1)

		# send message

		message_manager_client_structure = get_default_message_manager_client_structure_factory().get_structure()

		formatted_message = message_manager_client_structure.format_message(
			message="test message",
			format_types=[
				FormatTypeEnum.Reverse,
				FormatTypeEnum.Reverse,
				FormatTypeEnum.Reverse
			]
		)

		time.sleep(1.0)

		message_manager_client_structure.dispose()

		message_manager_server_messenger.stop_receiving_from_clients()

		message_manager_server_messenger.dispose()

		reverse_manager_server_messenger.stop_receiving_from_clients()

		reverse_manager_server_messenger.dispose()

		uppercase_manager_server_messenger.stop_receiving_from_clients()

		uppercase_manager_server_messenger.dispose()

		self.assertEqual("egassem tset", formatted_message)

	def test_reverse_many_times(self):

		uppercase_manager_server_messenger = get_default_uppercase_manager_server_messenger_factory().get_server_messenger()

		uppercase_manager_server_messenger.start_receiving_from_clients()

		reverse_manager_server_messenger = get_default_reverse_manager_server_messenger_factory().get_server_messenger()

		reverse_manager_server_messenger.start_receiving_from_clients()

		message_manager_server_messenger = get_default_message_manager_server_messenger_factory().get_server_messenger()

		message_manager_server_messenger.start_receiving_from_clients()

		time.sleep(0.1)

		# send message

		message_manager_client_structure = get_default_message_manager_client_structure_factory().get_structure()

		for index in range(100):
			start_datetime = datetime.utcnow()
			formatted_message = message_manager_client_structure.format_message(
				message="test message",
				format_types=[
					FormatTypeEnum.Reverse
				]
			)
			end_datetime = datetime.utcnow()

			print(f"{datetime.utcnow()}: test: elapsed time: {(end_datetime - start_datetime).total_seconds()}")

		# waiting for messages

		time.sleep(1.0)

		message_manager_client_structure.dispose()

		message_manager_server_messenger.stop_receiving_from_clients()

		message_manager_server_messenger.dispose()

		reverse_manager_server_messenger.stop_receiving_from_clients()

		reverse_manager_server_messenger.dispose()

		uppercase_manager_server_messenger.stop_receiving_from_clients()

		uppercase_manager_server_messenger.dispose()

		self.assertEqual("egassem tset", formatted_message)

	def test_reverse_many_times_recursively(self):

		uppercase_manager_server_messenger = get_default_uppercase_manager_server_messenger_factory().get_server_messenger()

		uppercase_manager_server_messenger.start_receiving_from_clients()

		reverse_manager_server_messenger = get_default_reverse_manager_server_messenger_factory().get_server_messenger()

		reverse_manager_server_messenger.start_receiving_from_clients()

		message_manager_server_messenger = get_default_message_manager_server_messenger_factory().get_server_messenger()

		message_manager_server_messenger.start_receiving_from_clients()

		time.sleep(0.1)

		# send message

		message_manager_client_structure = get_default_message_manager_client_structure_factory().get_structure()

		avg_total = 4
		for index in range(25):
			avg_sum = 0
			for avg_index in range(avg_total):
				start_datetime = datetime.utcnow()
				formatted_message = message_manager_client_structure.format_message(
					message="test message",
					format_types=[
						FormatTypeEnum.Reverse
					]*(index + 1)
				)
				end_datetime = datetime.utcnow()
				avg_sum += (end_datetime - start_datetime).total_seconds()
			print(f"{datetime.utcnow()}: test: elapsed time: {index}: {avg_sum / avg_total}")

		# waiting for messages

		time.sleep(1.0)

		message_manager_client_structure.dispose()

		message_manager_server_messenger.stop_receiving_from_clients()

		message_manager_server_messenger.dispose()

		reverse_manager_server_messenger.stop_receiving_from_clients()

		reverse_manager_server_messenger.dispose()

		uppercase_manager_server_messenger.stop_receiving_from_clients()

		uppercase_manager_server_messenger.dispose()

		self.assertEqual("egassem tset", formatted_message)
