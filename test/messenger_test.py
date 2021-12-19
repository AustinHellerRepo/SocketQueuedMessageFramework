from __future__ import annotations
import unittest
from src.austin_heller_repo.socket_kafka_message_framework import ClientMessenger, ServerMessenger, ClientServerMessage, ClientServerMessageTypeEnum, Structure, StructureStateEnum, StructureFactory
from austin_heller_repo.socket import ClientSocketFactory, ServerSocketFactory, ReadWriteSocketClosedException
from austin_heller_repo.common import HostPointer
from austin_heller_repo.kafka_manager import KafkaSequentialQueueFactory, KafkaManager, KafkaWrapper, KafkaManagerFactory
from austin_heller_repo.threading import start_thread, Semaphore, SingletonMemorySequentialQueueFactory
from typing import List, Tuple, Dict, Callable, Type
import uuid
import time
from datetime import datetime
from abc import ABC, abstractmethod
import multiprocessing as mp
import matplotlib.pyplot as plt
import math


is_socket_debug_active = False
is_client_messenger_debug_active = False
is_server_messenger_debug_active = False
is_kafka_debug_active = False
is_kafka_sequential_queue = False


def get_default_local_host_pointer() -> HostPointer:
	return HostPointer(
		host_address="0.0.0.0",
		host_port=36429
	)


def get_default_kafka_host_pointer() -> HostPointer:
	return HostPointer(
		host_address="0.0.0.0",
		host_port=9092
	)


def get_default_kafka_manager_factory() -> KafkaManagerFactory:
	return KafkaManagerFactory(
		kafka_wrapper=KafkaWrapper(
			host_pointer=get_default_kafka_host_pointer()
		),
		read_polling_seconds=0,
		is_cancelled_polling_seconds=0.01,
		new_topic_partitions_total=1,
		new_topic_replication_factor=1,
		remove_topic_cluster_propagation_blocking_timeout_seconds=30,
		is_debug=is_kafka_debug_active
	)


def get_default_client_messenger() -> ClientMessenger:
	return ClientMessenger(
		client_socket_factory=ClientSocketFactory(
			to_server_packet_bytes_length=4096,
			server_read_failed_delay_seconds=0.1,
			is_ssl=False,
			is_debug=is_socket_debug_active
		),
		server_host_pointer=get_default_local_host_pointer(),
		client_server_message_class=BaseClientServerMessage,
		is_debug=is_client_messenger_debug_active
	)


def get_default_server_messenger() -> ServerMessenger:

	if is_kafka_sequential_queue:

		kafka_topic_name = str(uuid.uuid4())

		kafka_manager = get_default_kafka_manager_factory().get_kafka_manager()

		kafka_manager.add_topic(
			topic_name=kafka_topic_name
		).get_result()

		sequential_queue_factory = KafkaSequentialQueueFactory(
			kafka_manager=kafka_manager,
			kafka_topic_name=kafka_topic_name
		)
	else:
		sequential_queue_factory = SingletonMemorySequentialQueueFactory()

	return ServerMessenger(
		server_socket_factory=ServerSocketFactory(
			to_client_packet_bytes_length=4096,
			listening_limit_total=10,
			accept_timeout_seconds=10.0,
			client_read_failed_delay_seconds=0.1,
			is_ssl=False,
			is_debug=is_socket_debug_active
		),
		sequential_queue_factory=sequential_queue_factory,
		local_host_pointer=get_default_local_host_pointer(),
		client_server_message_class=BaseClientServerMessage,
		structure_factory=ButtonStructureFactory(),
		is_debug=is_server_messenger_debug_active
	)


class BaseClientServerMessage(ClientServerMessage, ABC):
	pass

	@staticmethod
	def parse_from_json(*, json_object: Dict):
		base_client_server_message_type = BaseClientServerMessageTypeEnum(json_object["__type"])
		if base_client_server_message_type == BaseClientServerMessageTypeEnum.Announce:
			return AnnounceBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		elif base_client_server_message_type == BaseClientServerMessageTypeEnum.AnnounceFailed:
			return AnnounceFailedBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		elif base_client_server_message_type == BaseClientServerMessageTypeEnum.HelloWorld:
			return HelloWorldBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		elif base_client_server_message_type == BaseClientServerMessageTypeEnum.PressButton:
			return PressButtonBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		elif base_client_server_message_type == BaseClientServerMessageTypeEnum.ResetButton:
			return ResetButtonBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		elif base_client_server_message_type == BaseClientServerMessageTypeEnum.ResetTransmission:
			return ResetTransmissionBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		elif base_client_server_message_type == BaseClientServerMessageTypeEnum.ThreePressesTransmission:
			return ThreePressesTransmissionBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		elif base_client_server_message_type == BaseClientServerMessageTypeEnum.PingRequest:
			return PingRequestBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		elif base_client_server_message_type == BaseClientServerMessageTypeEnum.PingResponse:
			return PingResponseBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		elif base_client_server_message_type == BaseClientServerMessageTypeEnum.EchoRequest:
			return EchoRequestBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		elif base_client_server_message_type == BaseClientServerMessageTypeEnum.EchoResponse:
			return EchoResponseBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		elif base_client_server_message_type == BaseClientServerMessageTypeEnum.ErrorRequest:
			return ErrorRequestBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		elif base_client_server_message_type == BaseClientServerMessageTypeEnum.ErrorResponse:
			return ErrorResponseBaseClientServerMessage.parse_from_json(
				json_object=json_object
			)
		else:
			raise Exception(f"Unexpected BaseClientServerMessageTypeEnum: {base_client_server_message_type}")


class BaseClientServerMessageTypeEnum(ClientServerMessageTypeEnum):
	HelloWorld = "hello_world"  # basic test
	Announce = "announce"  # announces name to structure
	AnnounceFailed = "announce_failed"  # announce failed to apply to structure
	PressButton = "press_button"  # structural influence, three presses cause broadcast of transmission to users
	ResetButton = "reset_button"  # structural_influence, resets number of presses and informs button pressers that it was reset
	ResetTransmission = "reset_transmission"  # directed to specific users that pressed the button
	ThreePressesTransmission = "three_presses_transmission"  # broadcasts to all users that the button was pressed three times and then resets the button
	PingRequest = "ping_request"  # pings the server and gets a response
	PingResponse = "ping_response"  # the response from the ping request
	EchoRequest = "echo_request"  # records the messages that should be echoed back
	EchoResponse = "echo_response"  # the response containing the echo message
	ErrorOnGetClientServerMessageType = "error_on_get_client_server_message_type"
	ErrorRequest = "error_request"  # a request that throws an exception as defined in the constructor
	ErrorResponse = "error_response"  # the response that will throw a predefined exception
	PowerButton = "power_button"  # increments a child structure by the number of presses processed by the parent structure
	PowerOverloadTransmission = "power_overload_transmission"  # if the power button is pressed three times at any stage of normal button presses an overload transmission is sent out to all clients involved


class HelloWorldBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self):
		super().__init__()

		pass

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.HelloWorld

	def to_json(self) -> Dict:
		json_object = super().to_json()
		# nothing to add
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> HelloWorldBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 0:
			raise Exception(f"Unexpected properties sent to be parsed into {HelloWorldBaseClientServerMessage.__name__}")
		return HelloWorldBaseClientServerMessage()

	def is_response(self) -> bool:
		return False

	def get_destination_uuid(self) -> str:
		return None

	def is_structural_influence(self) -> bool:
		return False

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		return None


class AnnounceBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self, *, name: str):
		super().__init__()

		self.__name = name

	def get_name(self) -> str:
		return self.__name

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.Announce

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["name"] = self.__name
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> AnnounceBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 1:
			raise Exception(f"Unexpected properties sent to be parsed into {AnnounceBaseClientServerMessage.__name__}")
		return AnnounceBaseClientServerMessage(
			**json_object
		)

	def is_response(self) -> bool:
		return False

	def get_destination_uuid(self) -> str:
		return None

	def is_structural_influence(self) -> bool:
		return True

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		return AnnounceFailedBaseClientServerMessage(
			client_uuid=destination_uuid
		)


class AnnounceFailedBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self, *, client_uuid: str):
		super().__init__()

		self.__client_uuid = client_uuid

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.AnnounceFailed

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["client_uuid"] = self.__client_uuid
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> AnnounceFailedBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 1:
			raise Exception(f"Unexpected properties sent to be parsed into {AnnounceFailedBaseClientServerMessage.__name__}")
		return AnnounceFailedBaseClientServerMessage(
			**json_object
		)

	def is_response(self) -> bool:
		return True

	def get_destination_uuid(self) -> str:
		return self.__client_uuid

	def is_structural_influence(self) -> bool:
		return False

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		return None


class PressButtonBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self):
		super().__init__()

		pass

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.PressButton

	def to_json(self) -> Dict:
		json_object = super().to_json()
		# nothing to add
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> PressButtonBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 0:
			raise Exception(f"Unexpected properties sent to be parsed into {PressButtonBaseClientServerMessage.__name__}")
		return PressButtonBaseClientServerMessage()

	def is_response(self) -> bool:
		return False

	def get_destination_uuid(self) -> str:
		return None

	def is_structural_influence(self) -> bool:
		return True

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		return None


class ResetButtonBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self):
		super().__init__()

		pass

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.ResetButton

	def to_json(self) -> Dict:
		json_object = super().to_json()
		# nothing to add
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> ResetButtonBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 0:
			raise Exception(f"Unexpected properties sent to be parsed into {ResetButtonBaseClientServerMessage.__name__}")
		return ResetButtonBaseClientServerMessage()

	def is_response(self) -> bool:
		return False

	def get_destination_uuid(self) -> str:
		return None

	def is_structural_influence(self) -> bool:
		return True

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		return None


class ResetTransmissionBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self, *, client_uuid: str):
		super().__init__()

		self.__client_uuid = client_uuid

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.ResetTransmission

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["client_uuid"] = self.__client_uuid
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> ResetTransmissionBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 1:
			raise Exception(f"Unexpected properties sent to be parsed into {ResetTransmissionBaseClientServerMessage.__name__}")
		return ResetTransmissionBaseClientServerMessage(
			**json_object
		)

	def is_response(self) -> bool:
		return True

	def get_destination_uuid(self) -> str:
		return self.__client_uuid

	def is_structural_influence(self) -> bool:
		return False

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		return None


class ThreePressesTransmissionBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self, *, client_uuid: str, power: int):
		super().__init__()

		self.__client_uuid = client_uuid
		self.__power = power

	def get_power(self) -> int:
		return self.__power

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.ThreePressesTransmission

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["client_uuid"] = self.__client_uuid
		json_object["power"] = self.__power
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> ThreePressesTransmissionBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 2:
			raise Exception(f"Unexpected properties sent to be parsed into {ThreePressesTransmissionBaseClientServerMessage.__name__}")
		return ThreePressesTransmissionBaseClientServerMessage(
			**json_object
		)

	def is_response(self) -> bool:
		return True

	def get_destination_uuid(self) -> str:
		return self.__client_uuid

	def is_structural_influence(self) -> bool:
		return True

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		return None


class PingRequestBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self):
		super().__init__()

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.PingRequest

	def to_json(self) -> Dict:
		json_object = super().to_json()
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> PingRequestBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 0:
			raise Exception(f"Unexpected properties sent to be parsed into {PingRequestBaseClientServerMessage.__name__}")
		return PingRequestBaseClientServerMessage()

	def is_response(self) -> bool:
		return False

	def get_destination_uuid(self) -> str:
		return None

	def is_structural_influence(self) -> bool:
		return True

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		return None


class PingResponseBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self, *, client_uuid: str, ping_index: int):
		super().__init__()

		self.__client_uuid = client_uuid
		self.__ping_index = ping_index

	def get_ping_index(self) -> int:
		return self.__ping_index

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.PingResponse

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["client_uuid"] = self.__client_uuid
		json_object["ping_index"] = self.__ping_index
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> PingResponseBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 2:
			raise Exception(f"Unexpected properties sent to be parsed into {PingResponseBaseClientServerMessage.__name__}")
		return PingResponseBaseClientServerMessage(
			**json_object
		)

	def is_response(self) -> bool:
		return True

	def get_destination_uuid(self) -> str:
		return self.__client_uuid

	def is_structural_influence(self) -> bool:
		return False

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		return None


class EchoRequestBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self, *, message: str, is_ordered: bool):
		super().__init__()

		self.__message = message
		self.__is_ordered = is_ordered

	def get_message(self) -> str:
		return self.__message

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.EchoRequest

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["message"] = self.__message
		json_object["is_ordered"] = self.__is_ordered
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> EchoRequestBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 2:
			raise Exception(f"Unexpected properties sent to be parsed into {EchoRequestBaseClientServerMessage.__name__}")
		return EchoRequestBaseClientServerMessage(
			**json_object
		)

	def is_response(self) -> bool:
		return False

	def get_destination_uuid(self) -> str:
		return None

	def is_structural_influence(self) -> bool:
		return True

	def is_ordered(self) -> bool:
		return self.__is_ordered

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		return None


class EchoResponseBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self, *, message: str, client_uuid: str):
		super().__init__()

		self.__message = message
		self.__client_uuid = client_uuid

	def get_message(self) -> str:
		return self.__message

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.EchoResponse

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["message"] = self.__message
		json_object["client_uuid"] = self.__client_uuid
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> EchoResponseBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 2:
			raise Exception(f"Unexpected properties sent to be parsed into {EchoResponseBaseClientServerMessage.__name__}")
		return EchoResponseBaseClientServerMessage(
			**json_object
		)

	def is_response(self) -> bool:
		return True

	def get_destination_uuid(self) -> str:
		return self.__client_uuid

	def is_structural_influence(self) -> bool:
		return False

	def is_ordered(self) -> bool:
		return True

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		return None


class ErrorRequestBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self, *, is_constructor_exception_to_set: str = None, constructor_exception: str = None, get_client_server_message_type_exception: str = None, to_json_exception: str = None, is_response_exception: str = None, get_destination_uuid_exception: str = None, is_structural_influence_exception: str = None, is_ordered_exception: str = None, get_structural_error_client_server_message_response_exception: str = None, response_constructor_arguments: Dict = None):
		super().__init__()

		self.__is_constructor_exception_to_set = is_constructor_exception_to_set
		self.__constructor_exception = constructor_exception
		self.__get_client_server_message_type_exception = get_client_server_message_type_exception
		self.__to_json_exception = to_json_exception
		self.__is_response_exception = is_response_exception
		self.__get_destination_uuid_exception = get_destination_uuid_exception
		self.__is_structural_influence_exception = is_structural_influence_exception
		self.__is_ordered_exception = is_ordered_exception
		self.__get_structural_error_client_server_message_response_exception = get_structural_error_client_server_message_response_exception
		self.__response_constructor_arguments = response_constructor_arguments

		if self.__constructor_exception is not None:
			raise Exception(self.__constructor_exception)

		if self.__is_constructor_exception_to_set is not None:
			self.__constructor_exception = self.__is_constructor_exception_to_set
			self.__is_constructor_exception_to_set = None

	def get_response_constructor_arguments(self) -> Dict:
		return self.__response_constructor_arguments

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:

		if self.__get_client_server_message_type_exception is not None:
			raise Exception(self.__get_client_server_message_type_exception)

		return BaseClientServerMessageTypeEnum.ErrorRequest

	def to_json(self) -> Dict:

		if self.__to_json_exception is not None:
			raise Exception(self.__to_json_exception)

		json_object = super().to_json()
		json_object["is_constructor_exception_to_set"] = self.__is_constructor_exception_to_set
		json_object["constructor_exception"] = self.__constructor_exception
		json_object["get_client_server_message_type_exception"] = self.__get_client_server_message_type_exception
		json_object["to_json_exception"] = self.__to_json_exception
		json_object["is_response_exception"] = self.__is_response_exception
		json_object["get_destination_uuid_exception"] = self.__get_destination_uuid_exception
		json_object["is_structural_influence_exception"] = self.__is_structural_influence_exception
		json_object["is_ordered_exception"] = self.__is_ordered_exception
		json_object["get_structural_error_client_server_message_response_exception"] = self.__get_structural_error_client_server_message_response_exception
		json_object["response_constructor_arguments"] = self.__response_constructor_arguments
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> ErrorRequestBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 10:
			raise Exception(f"Unexpected properties sent to be parsed into {ErrorRequestBaseClientServerMessage.__name__}")
		return ErrorRequestBaseClientServerMessage(
			**json_object
		)

	def is_response(self) -> bool:

		if self.__is_response_exception is not None:
			raise Exception(self.__is_response_exception)

		return False

	def get_destination_uuid(self) -> str:

		if self.__get_destination_uuid_exception is not None:
			raise Exception(self.__get_destination_uuid_exception)

		return None

	def is_structural_influence(self) -> bool:

		if self.__is_structural_influence_exception is not None:
			raise Exception(self.__is_structural_influence_exception)

		return True

	def is_ordered(self) -> bool:

		if self.__is_ordered_exception is not None:
			raise Exception(self.__is_ordered_exception)

		return True

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:

		if self.__get_structural_error_client_server_message_response_exception is not None:
			raise Exception(self.__get_structural_error_client_server_message_response_exception)

		return None


class ErrorResponseBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self, *, client_uuid: str, is_constructor_exception_to_set: str = None, constructor_exception: str = None, get_client_server_message_type_exception: str = None, to_json_exception: str = None, is_response_exception: str = None, get_destination_uuid_exception: str = None, is_structural_influence_exception: str = None, is_ordered_exception: str = None, get_structural_error_client_server_message_response_exception: str = None):
		super().__init__()

		self.__client_uuid = client_uuid
		self.__is_constructor_exception_to_set = is_constructor_exception_to_set
		self.__constructor_exception = constructor_exception
		self.__get_client_server_message_type_exception = get_client_server_message_type_exception
		self.__to_json_exception = to_json_exception
		self.__is_response_exception = is_response_exception
		self.__get_destination_uuid_exception = get_destination_uuid_exception
		self.__is_structural_influence_exception = is_structural_influence_exception
		self.__is_ordered_exception = is_ordered_exception
		self.__get_structural_error_client_server_message_response_exception = get_structural_error_client_server_message_response_exception

		if self.__constructor_exception is not None:
			raise Exception(self.__constructor_exception)

		if self.__is_constructor_exception_to_set is not None:
			self.__constructor_exception = self.__is_constructor_exception_to_set
			self.__is_constructor_exception_to_set = None

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:

		if self.__get_client_server_message_type_exception is not None:
			raise Exception(self.__get_client_server_message_type_exception)

		return BaseClientServerMessageTypeEnum.ErrorResponse

	def to_json(self) -> Dict:

		if self.__to_json_exception is not None:
			raise Exception(self.__to_json_exception)

		json_object = super().to_json()
		json_object["client_uuid"] = self.__client_uuid
		json_object["is_constructor_exception_to_set"] = self.__is_constructor_exception_to_set
		json_object["constructor_exception"] = self.__constructor_exception
		json_object["get_client_server_message_type_exception"] = self.__get_client_server_message_type_exception
		json_object["to_json_exception"] = self.__to_json_exception
		json_object["is_response_exception"] = self.__is_response_exception
		json_object["get_destination_uuid_exception"] = self.__get_destination_uuid_exception
		json_object["is_structural_influence_exception"] = self.__is_structural_influence_exception
		json_object["is_ordered_exception"] = self.__is_ordered_exception
		json_object["get_structural_error_client_server_message_response_exception"] = self.__get_structural_error_client_server_message_response_exception
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> ErrorResponseBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 10:
			raise Exception(f"Unexpected properties sent to be parsed into {ErrorResponseBaseClientServerMessage.__name__}")
		return ErrorResponseBaseClientServerMessage(
			**json_object
		)

	def is_response(self) -> bool:

		if self.__is_response_exception is not None:
			raise Exception(self.__is_response_exception)

		return True

	def get_destination_uuid(self) -> str:

		if self.__get_destination_uuid_exception is not None:
			raise Exception(self.__get_destination_uuid_exception)

		return self.__client_uuid

	def is_structural_influence(self) -> bool:

		if self.__is_structural_influence_exception is not None:
			raise Exception(self.__is_structural_influence_exception)

		return False

	def is_ordered(self) -> bool:

		if self.__is_ordered_exception is not None:
			raise Exception(self.__is_ordered_exception)

		return True

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:

		if self.__get_structural_error_client_server_message_response_exception is not None:
			raise Exception(self.__get_structural_error_client_server_message_response_exception)

		return None


class PowerButtonBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self, *, is_anonymous: bool):
		super().__init__()

		self.__is_anonymous = is_anonymous  # if an overload should not be sent back to them due to this message

	def is_anonymous(self) -> bool:
		return self.__is_anonymous

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.PowerButton

	def to_json(self) -> Dict:
		json_object = super().to_json()
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> PowerButtonBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 0:
			raise Exception(f"Unexpected properties sent to be parsed into {PowerButtonBaseClientServerMessage.__name__}")
		return PowerButtonBaseClientServerMessage()

	def is_response(self) -> bool:
		return False

	def get_destination_uuid(self) -> str:
		return None

	def is_structural_influence(self) -> bool:
		return True

	def is_ordered(self) -> bool:
		return False

	def get_structural_error_client_server_message_response(self, destination_uuid: str) -> ClientServerMessage:
		return None


class PowerStructureStateEnum(StructureStateEnum):
	ZeroPower = "zero_power"
	OnePower = "one_power"
	TwoPower = "two_power"
	ThreePower = "three_power"


class PowerStructure(Structure):

	def __init__(self):
		super().__init__(
			states=PowerStructureStateEnum,
			initial_state=PowerStructureStateEnum.ZeroPower
		)

		self.__power_total = 0

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.PowerButton.value,
			source=PowerStructureStateEnum.ZeroPower.value,
			dest=PowerStructureStateEnum.OnePower.value,
			before=f"_{PowerStructure.__name__}{PowerStructure.__power_button_pressed.__name__}"
		)

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.PowerButton.value,
			source=PowerStructureStateEnum.OnePower.value,
			dest=PowerStructureStateEnum.TwoPower.value,
			before=f"_{PowerStructure.__name__}{PowerStructure.__power_button_pressed.__name__}"
		)

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.PowerButton.value,
			source=PowerStructureStateEnum.TwoPower.value,
			dest=PowerStructureStateEnum.ThreePower.value,
			before=f"_{PowerStructure.__name__}{PowerStructure.__power_button_pressed.__name__}"
		)

	def get_power(self) -> int:
		return self.__power_total

	def __power_button_pressed(self, power_button: PowerButtonBaseClientServerMessage, source_uuid: str):

		self.__power_total += 1
		if self.__power_total >= 3:
			pass

		raise NotImplementedError()


class ButtonStructureStateEnum(StructureStateEnum):
	ZeroPresses = "zero_presses"
	OnePress = "one_press"
	TwoPresses = "two_presses"
	ThreePresses = "three_presses"


class ButtonStructure(Structure):

	def __init__(self):
		super().__init__(
			states=ButtonStructureStateEnum,
			initial_state=ButtonStructureStateEnum.ZeroPresses
		)

		self.__pressed_button_client_uuids = []  # type: List[str]
		self.__name_per_client_uuid = {}  # type: Dict[str, str]
		self.__presses_total = 0
		self.__pings_total = 0

		self.__power_structure = PowerStructure()

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.Announce.value,
			source=ButtonStructureStateEnum.ZeroPresses.value,
			dest=ButtonStructureStateEnum.ZeroPresses.value,
			before=f"_{ButtonStructure.__name__}{ButtonStructure.__name_announced.__name__}"
		)

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.PressButton.value,
			source=ButtonStructureStateEnum.ZeroPresses.value,
			dest=ButtonStructureStateEnum.OnePress.value,
			before=f"_{ButtonStructure.__name__}{ButtonStructure.__button_pressed.__name__}"
		)

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.PressButton.value,
			source=ButtonStructureStateEnum.OnePress.value,
			dest=ButtonStructureStateEnum.TwoPresses.value,
			before=f"_{ButtonStructure.__name__}{ButtonStructure.__button_pressed.__name__}"
		)

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.PressButton.value,
			source=ButtonStructureStateEnum.TwoPresses.value,
			dest=ButtonStructureStateEnum.ThreePresses.value,
			before=f"_{ButtonStructure.__name__}{ButtonStructure.__button_pressed.__name__}"
		)

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.ResetButton.value,
			source=ButtonStructureStateEnum.ZeroPresses.value,
			dest=ButtonStructureStateEnum.ZeroPresses.value,
			before=f"_{ButtonStructure.__name__}{ButtonStructure.__button_reset.__name__}"
		)

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.ResetButton.value,
			source=ButtonStructureStateEnum.OnePress.value,
			dest=ButtonStructureStateEnum.ZeroPresses.value,
			before=f"_{ButtonStructure.__name__}{ButtonStructure.__button_reset.__name__}"
		)

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.ResetButton.value,
			source=ButtonStructureStateEnum.TwoPresses.value,
			dest=ButtonStructureStateEnum.ZeroPresses.value,
			before=f"_{ButtonStructure.__name__}{ButtonStructure.__button_reset.__name__}"
		)

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.ThreePressesTransmission.value,
			source=ButtonStructureStateEnum.ThreePresses.value,
			dest=ButtonStructureStateEnum.ZeroPresses.value,
			before=f"_{ButtonStructure.__name__}{ButtonStructure.__three_presses_transmission_sent.__name__}"
		)

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.PingRequest.value,
			source=ButtonStructureStateEnum.ZeroPresses.value,
			dest=ButtonStructureStateEnum.ZeroPresses.value,
			before=f"_{ButtonStructure.__name__}{ButtonStructure.__ping_requested.__name__}"
		)

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.EchoRequest.value,
			source=ButtonStructureStateEnum.ZeroPresses.value,
			dest=ButtonStructureStateEnum.ZeroPresses.value,
			before=f"_{ButtonStructure.__name__}{ButtonStructure.__echo_requested.__name__}"
		)

		self.add_transition(
			trigger=BaseClientServerMessageTypeEnum.ErrorRequest.value,
			source=ButtonStructureStateEnum.ZeroPresses.value,
			dest=ButtonStructureStateEnum.ZeroPresses.value,
			before=f"_{ButtonStructure.__name__}{ButtonStructure.__error_requested.__name__}"
		)

	def __name_announced(self, announce: AnnounceBaseClientServerMessage, source_uuid: str):
		self.__name_per_client_uuid[source_uuid] = announce.get_name()

	def __button_pressed(self, press_button: PressButtonBaseClientServerMessage, source_uuid: str):
		if source_uuid not in self.__pressed_button_client_uuids:
			self.__pressed_button_client_uuids.append(source_uuid)
		if source_uuid in self.__name_per_client_uuid:
			print(f"button pressed by {self.__name_per_client_uuid[source_uuid]}")
		else:
			print(f"button pressed by {source_uuid}")
		self.__presses_total += 1
		if self.__presses_total == 3:
			self.process_response(
				client_server_message=ThreePressesTransmissionBaseClientServerMessage(
					client_uuid=source_uuid,
					power=self.__power_structure.get_power()
				)
			)

	def __button_reset(self, reset_button: ResetButtonBaseClientServerMessage, source_uuid: str):
		for client_uuid in self.__pressed_button_client_uuids:
			client_server_message = ResetTransmissionBaseClientServerMessage(
				client_uuid=client_uuid
			)
			self.process_response(
				client_server_message=client_server_message
			)
		self.__pressed_button_client_uuids.clear()

	def __three_presses_transmission_sent(self, three_presses_transmission: ThreePressesTransmissionBaseClientServerMessage, source_uuid: str):
		self.__pressed_button_client_uuids.clear()

	def __ping_requested(self, ping_request: PingRequestBaseClientServerMessage, source_uuid: str):
		self.process_response(
			client_server_message=PingResponseBaseClientServerMessage(
				client_uuid=source_uuid,
				ping_index=self.__pings_total
			)
		)
		self.__pings_total += 1

	def __echo_requested(self, echo_request: EchoRequestBaseClientServerMessage, source_uuid: str):
		message = echo_request.get_message()
		self.process_response(
			client_server_message=EchoResponseBaseClientServerMessage(
				message=message,
				client_uuid=source_uuid
			)
		)

	def __error_requested(self, error_request: ErrorRequestBaseClientServerMessage, source_uuid: str):
		constructor_arguments = error_request.get_response_constructor_arguments()
		if constructor_arguments is None:
			constructor_arguments = {}
		constructor_arguments["client_uuid"] = source_uuid
		self.process_response(
			client_server_message=ErrorResponseBaseClientServerMessage(
				**constructor_arguments
			)
		)


class ButtonStructureFactory(StructureFactory):

	def __init__(self):
		super().__init__()

		pass

	def get_structure(self) -> Structure:
		return ButtonStructure()


##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################


class MessengerTest(unittest.TestCase):

	def setUp(self) -> None:

		print(f"setUp: started: {datetime.utcnow()}")

		kafka_manager = get_default_kafka_manager_factory().get_kafka_manager()

		print(f"setUp: initialized: {datetime.utcnow()}")

		topics = kafka_manager.get_topics().get_result()  # type: List[str]

		print(f"setUp: get_topics: {datetime.utcnow()}")

		for topic in topics:

			print(f"setUp: topic: {topic}: {datetime.utcnow()}")

			async_handle = kafka_manager.remove_topic(
				topic_name=topic
			)

			print(f"setUp: async: {topic}: {datetime.utcnow()}")

			async_handle.get_result()

			print(f"setUp: result: {topic}: {datetime.utcnow()}")

		time.sleep(1)

	def test_initialize_client_messenger(self):

		client_messenger = get_default_client_messenger()

		self.assertIsNotNone(client_messenger)

		client_messenger.dispose()

	def test_initialize_server_messenger(self):

		server_messenger = get_default_server_messenger()

		self.assertIsNotNone(server_messenger)

	def test_server_messenger_start_and_stop(self):

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(3)

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

	def test_connect_client_to_server(self):

		client_messenger = get_default_client_messenger()

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger.connect_to_server()

		client_messenger.send_to_server(
			request_client_server_message=HelloWorldBaseClientServerMessage()
		)

		time.sleep(1)

		server_messenger.stop_receiving_from_clients()

		client_messenger.dispose()

	def test_press_button_three_times(self):
		# send three presses and wait for a reply

		client_messenger = get_default_client_messenger()

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, ThreePressesTransmissionBaseClientServerMessage)

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending announcement")

		client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="Test Name"
			)
		)

		print(f"{datetime.utcnow()}: sending first press")

		client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		print(f"{datetime.utcnow()}: sending second press")

		client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		print(f"{datetime.utcnow()}: sending third press")

		client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing")

		client_messenger.dispose()

		print(f"{datetime.utcnow()}: disposed")

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertEqual(1, callback_total)
		self.assertIsNone(found_exception)

	def test_one_client_sends_two_presses_then_reset(self):
		# send two presses of the button, then send a reset, and finally wait for a reply

		client_messenger = get_default_client_messenger()

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, ResetTransmissionBaseClientServerMessage)

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending announcement")

		client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="Test Name"
			)
		)

		print(f"{datetime.utcnow()}: sending first press")

		client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		print(f"{datetime.utcnow()}: sending second press")

		client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		print(f"{datetime.utcnow()}: sending reset")

		client_messenger.send_to_server(
			request_client_server_message=ResetButtonBaseClientServerMessage()
		)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing")

		client_messenger.dispose()

		print(f"{datetime.utcnow()}: disposed")

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertEqual(1, callback_total)
		self.assertIsNone(found_exception)

	def test_two_clients_each_send_one_press_then_reset(self):

		first_client_messenger = get_default_client_messenger()

		second_client_messenger = get_default_client_messenger()

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		first_client_messenger.connect_to_server()
		second_client_messenger.connect_to_server()

		callback_total = 0

		def first_callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: first_callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, ResetTransmissionBaseClientServerMessage)

		first_found_exception = None  # type: Exception

		def first_on_exception(exception: Exception):
			nonlocal first_found_exception
			first_found_exception = exception

		first_client_messenger.receive_from_server(
			callback=first_callback,
			on_exception=first_on_exception
		)

		def second_callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: second_callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, ResetTransmissionBaseClientServerMessage)

		second_found_exception = None  # type: Exception

		def second_on_exception(exception: Exception):
			nonlocal second_found_exception
			second_found_exception = exception

		second_client_messenger.receive_from_server(
			callback=second_callback,
			on_exception=second_on_exception
		)

		print(f"{datetime.utcnow()}: sending first announcement")

		first_client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="First"
			)
		)

		print(f"{datetime.utcnow()}: sending second announcement")

		second_client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="Second"
			)
		)

		time.sleep(1)

		print(f"{datetime.utcnow()}: sending first press")

		first_client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		print(f"{datetime.utcnow()}: sending second press")

		second_client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		print(f"{datetime.utcnow()}: sending reset")

		first_client_messenger.send_to_server(
			request_client_server_message=ResetButtonBaseClientServerMessage()
		)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing")

		first_client_messenger.dispose()
		second_client_messenger.dispose()

		print(f"{datetime.utcnow()}: disposed")

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertEqual(2, callback_total)
		self.assertIsNone(first_found_exception)
		self.assertIsNone(second_found_exception)

	def test_two_clients_each_send_one_press_then_third_client_reset(self):

		first_client_messenger = get_default_client_messenger()
		second_client_messenger = get_default_client_messenger()
		third_client_messenger = get_default_client_messenger()

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		first_client_messenger.connect_to_server()
		second_client_messenger.connect_to_server()
		third_client_messenger.connect_to_server()

		callback_total = 0

		def first_callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: first_callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, ResetTransmissionBaseClientServerMessage)

		first_found_exception = None  # type: Exception

		def first_on_exception(exception: Exception):
			nonlocal first_found_exception
			first_found_exception = exception

		first_client_messenger.receive_from_server(
			callback=first_callback,
			on_exception=first_on_exception
		)

		def second_callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: second_callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, ResetTransmissionBaseClientServerMessage)

		second_found_exception = None  # type: Exception

		def second_on_exception(exception: Exception):
			nonlocal second_found_exception
			second_found_exception = exception

		second_client_messenger.receive_from_server(
			callback=second_callback,
			on_exception=second_on_exception
		)

		def third_callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: third_callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			raise Exception(f"Third client should not receive a message.")

		third_found_exception = None  # type: Exception

		def third_on_exception(exception: Exception):
			nonlocal third_found_exception
			third_found_exception = exception

		third_client_messenger.receive_from_server(
			callback=third_callback,
			on_exception=third_on_exception
		)

		print(f"{datetime.utcnow()}: sending first announcement")

		first_client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="First"
			)
		)

		print(f"{datetime.utcnow()}: sending second announcement")

		second_client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="Second"
			)
		)

		print(f"{datetime.utcnow()}: sending third announcement")

		third_client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="Third"
			)
		)

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: sending first press")

		first_client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: sending second press")

		second_client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: sending reset")

		third_client_messenger.send_to_server(
			request_client_server_message=ResetButtonBaseClientServerMessage()
		)

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing")

		first_client_messenger.dispose()
		second_client_messenger.dispose()
		third_client_messenger.dispose()

		print(f"{datetime.utcnow()}: disposed")

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertIsNone(first_found_exception)
		self.assertIsNone(second_found_exception)
		self.assertIsNone(third_found_exception)
		self.assertEqual(2, callback_total)

	def test_client_disconnects_before_receiving_intended_message(self):
		# the first client sends a press, disconnects, then the second client resets
		# the server messenger should detect that the client disconnected and release the socket gracefully

		first_client_messenger = get_default_client_messenger()

		second_client_messenger = get_default_client_messenger()

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		first_client_messenger.connect_to_server()
		second_client_messenger.connect_to_server()

		callback_total = 0

		def first_callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: first_callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			raise Exception("This client should have been disposed of already.")

		first_found_exception = None  # type: Exception

		def first_on_exception(exception: Exception):
			nonlocal first_found_exception
			first_found_exception = exception

		first_client_messenger.receive_from_server(
			callback=first_callback,
			on_exception=first_on_exception
		)

		def second_callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: second_callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			raise Exception("This client should not be receiving a message.")

		second_found_exception = None  # type: Exception

		def second_on_exception(exception: Exception):
			nonlocal second_found_exception
			second_found_exception = exception

		second_client_messenger.receive_from_server(
			callback=second_callback,
			on_exception=second_on_exception
		)

		print(f"{datetime.utcnow()}: sending first announcement")

		first_client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="First"
			)
		)

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: sending second announcement")

		second_client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="Second"
			)
		)

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: sending first press")

		first_client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		time.sleep(1)

		print(f"{datetime.utcnow()}: disposing first client")

		first_client_messenger.dispose()

		time.sleep(1)

		print(f"{datetime.utcnow()}: sending reset")

		second_client_messenger.send_to_server(
			request_client_server_message=ResetButtonBaseClientServerMessage()
		)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(1)

		print(f"{datetime.utcnow()}: disposing")

		second_client_messenger.dispose()

		time.sleep(1)

		print(f"{datetime.utcnow()}: disposed")

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		time.sleep(1)

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertEqual(0, callback_total)
		self.assertIsNone(first_found_exception)
		self.assertIsNone(second_found_exception)

	def test_ping(self):

		client_messenger = get_default_client_messenger()

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, PingResponseBaseClientServerMessage)
			ping_response_base_client_server_message = client_server_message  # type: PingResponseBaseClientServerMessage
			self.assertEqual(0, ping_response_base_client_server_message.get_ping_index())

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending first announcement")

		client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="First"
			)
		)

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: sending first press")

		client_messenger.send_to_server(
			request_client_server_message=PingRequestBaseClientServerMessage()
		)

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing")

		client_messenger.dispose()

		print(f"{datetime.utcnow()}: disposed")

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertEqual(1, callback_total)
		self.assertIsNone(found_exception)

	def test_single_client_quickly_pings_using_threading(self):
		# spam pings and detect timing differences between sends and receives

		client_messenger = get_default_client_messenger()

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger.connect_to_server()

		expected_pings_total = 1000
		callback_total = 0
		expected_ping_index = 0
		first_message_datetime = None  # type: datetime
		last_message_datetime = None  # type: datetime

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			nonlocal expected_ping_index
			nonlocal first_message_datetime
			nonlocal last_message_datetime
			nonlocal expected_pings_total

			#print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, PingResponseBaseClientServerMessage)
			ping_response_base_client_server_message = client_server_message  # type: PingResponseBaseClientServerMessage
			self.assertEqual(expected_ping_index, ping_response_base_client_server_message.get_ping_index())
			expected_ping_index += 1
			if expected_ping_index == 1:
				first_message_datetime = datetime.utcnow()
			if expected_ping_index == expected_pings_total:
				last_message_datetime = datetime.utcnow()

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending first announcement")

		client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="First"
			)
		)

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: sending first press")

		sent_first_ping_datetime = None  # type: datetime
		sent_last_ping_datetime = None  # type: datetime

		def ping_thread_method():
			nonlocal client_messenger
			nonlocal expected_pings_total
			nonlocal sent_first_ping_datetime
			nonlocal sent_last_ping_datetime

			sent_first_ping_datetime = datetime.utcnow()
			client_messenger.send_to_server(
				request_client_server_message=PingRequestBaseClientServerMessage()
			)
			for index in range(expected_pings_total - 2):
				client_messenger.send_to_server(
					request_client_server_message=PingRequestBaseClientServerMessage()
				)
			sent_last_ping_datetime = datetime.utcnow()
			client_messenger.send_to_server(
				request_client_server_message=PingRequestBaseClientServerMessage()
			)

		ping_thread = start_thread(ping_thread_method)
		ping_thread.join()

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: waiting for messages")

		while last_message_datetime is None:
			time.sleep(1)
		time.sleep(1)

		print(f"{datetime.utcnow()}: disposing")

		client_messenger.dispose()

		print(f"{datetime.utcnow()}: disposed")

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertEqual(expected_pings_total, callback_total)

		print(f"Sent first message datetime: {sent_first_ping_datetime}")
		print(f"Received first message datetime: {first_message_datetime}")
		print(f"Diff: {(first_message_datetime - sent_first_ping_datetime).total_seconds()} seconds")
		print(f"Sent last message datetime: {sent_last_ping_datetime}")
		print(f"Received last message datetime: {last_message_datetime}")
		print(f"Diff: {(last_message_datetime - sent_last_ping_datetime).total_seconds()} seconds")
		seconds_total = (last_message_datetime - first_message_datetime).total_seconds()
		messages_per_second = expected_pings_total / seconds_total
		print(f"Messages per seconds: {messages_per_second}")
		print(f"Seconds per message: {1.0 / messages_per_second}")

		self.assertIsNone(found_exception)

	def test_single_client_quickly_pings_burst(self):
		# spam pings and detect timing differences between sends and receives

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		expected_pings_total = 1000

		print(f"{datetime.utcnow()}: sending first press")

		found_exception = None  # type: Exception

		def ping_thread_method():
			nonlocal expected_pings_total
			nonlocal found_exception

			client_messenger = get_default_client_messenger()

			client_messenger.connect_to_server()

			expected_ping_index = 0
			received_first_message_datetime = None  # type: datetime
			received_last_message_datetime = None  # type: datetime
			callback_semaphore = Semaphore()

			def callback(client_server_message: ClientServerMessage):
				nonlocal expected_pings_total
				nonlocal expected_ping_index
				nonlocal received_first_message_datetime
				nonlocal received_last_message_datetime
				nonlocal callback_semaphore

				#print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
				self.assertIsInstance(client_server_message, PingResponseBaseClientServerMessage)
				ping_response_base_client_server_message = client_server_message  # type: PingResponseBaseClientServerMessage
				self.assertEqual(expected_ping_index, ping_response_base_client_server_message.get_ping_index())

				callback_semaphore.acquire()
				expected_ping_index += 1
				if expected_ping_index == 1:
					received_first_message_datetime = datetime.utcnow()
				if expected_ping_index == expected_pings_total:
					received_last_message_datetime = datetime.utcnow()
				callback_semaphore.release()

			def on_exception(exception: Exception):
				nonlocal found_exception
				found_exception = exception

			client_messenger.receive_from_server(
				callback=callback,
				on_exception=on_exception
			)

			print(f"{datetime.utcnow()}: sending first announcement")

			client_messenger.send_to_server(
				request_client_server_message=AnnounceBaseClientServerMessage(
					name="First"
				)
			)

			sent_first_ping_datetime = datetime.utcnow()
			client_messenger.send_to_server(
				request_client_server_message=PingRequestBaseClientServerMessage()
			)
			for index in range(expected_pings_total - 2):
				client_messenger.send_to_server(
					request_client_server_message=PingRequestBaseClientServerMessage()
				)
			sent_last_ping_datetime = datetime.utcnow()
			client_messenger.send_to_server(
				request_client_server_message=PingRequestBaseClientServerMessage()
			)

			print(f"{datetime.utcnow()}: waiting for messages")

			while received_last_message_datetime is None:
				time.sleep(1)
			time.sleep(1)

			print(f"Sent first message datetime: {sent_first_ping_datetime}")
			print(f"Received first message datetime: {received_first_message_datetime}")
			print(f"Diff: {(received_first_message_datetime - sent_first_ping_datetime).total_seconds()} seconds")
			print(f"Sent last message datetime: {sent_last_ping_datetime}")
			print(f"Received last message datetime: {received_last_message_datetime}")
			print(f"Diff: {(received_last_message_datetime - sent_last_ping_datetime).total_seconds()} seconds")
			seconds_total = (sent_last_ping_datetime - sent_first_ping_datetime).total_seconds()
			messages_per_second = expected_pings_total / seconds_total
			print(f"Seconds to send all messages: {seconds_total}")
			print(f"Sent messages per seconds: {messages_per_second}")
			print(f"Seconds per sent message: {1.0 / messages_per_second}")
			seconds_total = (received_last_message_datetime - received_first_message_datetime).total_seconds()
			messages_per_second = expected_pings_total / seconds_total
			print(f"Seconds to receive all messages: {seconds_total}")
			print(f"Received messages per seconds: {messages_per_second}")
			print(f"Seconds per received message: {1.0 / messages_per_second}")

			print(f"{datetime.utcnow()}: disposing")

			client_messenger.dispose()

			print(f"{datetime.utcnow()}: disposed")

		ping_thread = start_thread(ping_thread_method)
		ping_thread.join()

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertIsNone(found_exception)

	def test_single_client_quickly_pings_delayed(self):
		# spam pings and detect timing differences between sends and receives

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		test_seconds = 10
		test_messages_per_second = 500
		expected_pings_total = test_seconds * test_messages_per_second
		delay_between_sending_message_seconds = (1.0 / test_messages_per_second) * 0.6
		is_result_plotted = False

		#expected_pings_total = 1000
		#delay_between_sending_message_seconds = 0.0025

		print(f"{datetime.utcnow()}: sending first press")

		found_exception = None  # type: Exception

		def ping_thread_method():
			nonlocal expected_pings_total
			nonlocal delay_between_sending_message_seconds
			nonlocal found_exception

			client_messenger = get_default_client_messenger()

			client_messenger.connect_to_server()

			expected_ping_index = 0
			callback_semaphore = Semaphore()

			received_datetimes = []  # type: List[datetime]
			sent_datetimes = []  # type: List[datetime]

			def callback(client_server_message: ClientServerMessage):
				nonlocal expected_pings_total
				nonlocal expected_ping_index
				nonlocal received_datetimes
				nonlocal callback_semaphore

				#print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
				self.assertIsInstance(client_server_message, PingResponseBaseClientServerMessage)
				ping_response_base_client_server_message = client_server_message  # type: PingResponseBaseClientServerMessage
				#self.assertEqual(expected_ping_index, ping_response_base_client_server_message.get_ping_index())

				callback_semaphore.acquire()
				expected_ping_index += 1
				received_datetimes.append(datetime.utcnow())
				callback_semaphore.release()

			def on_exception(exception: Exception):
				nonlocal found_exception
				found_exception = exception

			client_messenger.receive_from_server(
				callback=callback,
				on_exception=on_exception
			)

			print(f"{datetime.utcnow()}: sending first announcement")

			client_messenger.send_to_server(
				request_client_server_message=AnnounceBaseClientServerMessage(
					name="First"
				)
			)

			print(f"{datetime.utcnow()}: starting to send messages")

			sent_datetimes.append(datetime.utcnow())
			client_messenger.send_to_server(
				request_client_server_message=PingRequestBaseClientServerMessage()
			)
			time.sleep(delay_between_sending_message_seconds)
			for index in range(expected_pings_total - 2):
				sent_datetimes.append(datetime.utcnow())
				client_messenger.send_to_server(
					request_client_server_message=PingRequestBaseClientServerMessage()
				)
				time.sleep(delay_between_sending_message_seconds)
			sent_datetimes.append(datetime.utcnow())
			client_messenger.send_to_server(
				request_client_server_message=PingRequestBaseClientServerMessage()
			)

			print(f"{datetime.utcnow()}: waiting for messages")

			while len(received_datetimes) != expected_pings_total:
				time.sleep(1)
				print(f"len(received_datetimes): {len(received_datetimes)}")
			time.sleep(1)

			self.assertEqual(expected_pings_total, len(sent_datetimes))
			self.assertEqual(expected_pings_total, len(received_datetimes))

			diff_seconds_totals = []  # type: List[float]
			for sent_datetime, received_datetime in zip(sent_datetimes, received_datetimes):
				seconds_total = (received_datetime - sent_datetime).total_seconds()
				diff_seconds_totals.append(seconds_total)

			print(f"Time to send {(sent_datetimes[-1] - sent_datetimes[0]).total_seconds()} seconds")
			print(f"Messages per second to send: {expected_pings_total / (sent_datetimes[-1] - sent_datetimes[0]).total_seconds()}")
			print(f"Time to receive {(received_datetimes[-1] - received_datetimes[0]).total_seconds()} seconds")
			print(f"Messages per second to receive: {expected_pings_total / (received_datetimes[-1] - received_datetimes[0]).total_seconds()}")
			print(f"Min diff seconds {min(diff_seconds_totals)} at {diff_seconds_totals.index(min(diff_seconds_totals))}")
			print(f"Max diff seconds {max(diff_seconds_totals)} at {diff_seconds_totals.index(max(diff_seconds_totals))}")
			print(f"Ave diff seconds {sum(diff_seconds_totals)/expected_pings_total}")

			if is_result_plotted:
				plt.scatter(sent_datetimes, range(len(sent_datetimes)), s=1, c="red")
				plt.scatter(received_datetimes, range(len(received_datetimes)), s=1, c="blue")
				plt.show()

			cutoff = 150
			print(f"Min diff seconds {min(diff_seconds_totals[cutoff:])} at {diff_seconds_totals.index(min(diff_seconds_totals[cutoff:]))}")
			print(f"Max diff seconds {max(diff_seconds_totals[cutoff:])} at {diff_seconds_totals.index(max(diff_seconds_totals[cutoff:]))}")
			print(f"Ave diff seconds {sum(diff_seconds_totals[cutoff:]) / (expected_pings_total - cutoff)}")

			print(f"{datetime.utcnow()}: disposing")

			client_messenger.dispose()

			print(f"{datetime.utcnow()}: disposed")

		ping_thread = start_thread(ping_thread_method)
		ping_thread.join()

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertIsNone(found_exception)

	def test_single_client_quickly_echos_burst_0B(self):
		# spam pings and detect timing differences between sends and receives

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		expected_pings_total = 1000
		message_contents = ""

		print(f"{datetime.utcnow()}: sending first press")

		found_exception = None  # type: Exception

		def ping_thread_method():
			nonlocal expected_pings_total
			nonlocal found_exception
			nonlocal message_contents

			client_messenger = get_default_client_messenger()

			client_messenger.connect_to_server()

			expected_ping_index = 0
			received_first_message_datetime = None  # type: datetime
			received_last_message_datetime = None  # type: datetime
			callback_semaphore = Semaphore()

			def callback(client_server_message: ClientServerMessage):
				nonlocal expected_pings_total
				nonlocal expected_ping_index
				nonlocal received_first_message_datetime
				nonlocal received_last_message_datetime
				nonlocal callback_semaphore

				# print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
				self.assertIsInstance(client_server_message, EchoResponseBaseClientServerMessage)

				callback_semaphore.acquire()
				expected_ping_index += 1
				if expected_ping_index == 1:
					received_first_message_datetime = datetime.utcnow()
				if expected_ping_index == expected_pings_total:
					received_last_message_datetime = datetime.utcnow()
				callback_semaphore.release()

			def on_exception(exception: Exception):
				nonlocal found_exception
				found_exception = exception

			client_messenger.receive_from_server(
				callback=callback,
				on_exception=on_exception
			)

			print(f"{datetime.utcnow()}: sending first announcement")

			client_messenger.send_to_server(
				request_client_server_message=AnnounceBaseClientServerMessage(
					name="First"
				)
			)

			sent_first_ping_datetime = datetime.utcnow()
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=message_contents,
					is_ordered=True
				)
			)
			for index in range(expected_pings_total - 2):
				client_messenger.send_to_server(
					request_client_server_message=EchoRequestBaseClientServerMessage(
						message=message_contents,
						is_ordered=True
					)
				)
			sent_last_ping_datetime = datetime.utcnow()
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=message_contents,
					is_ordered=True
				)
			)

			print(f"{datetime.utcnow()}: waiting for messages")

			while received_last_message_datetime is None:
				time.sleep(1)
			time.sleep(1)

			print(f"Sent first message datetime: {sent_first_ping_datetime}")
			print(f"Received first message datetime: {received_first_message_datetime}")
			print(f"Diff: {(received_first_message_datetime - sent_first_ping_datetime).total_seconds()} seconds")
			print(f"Sent last message datetime: {sent_last_ping_datetime}")
			print(f"Received last message datetime: {received_last_message_datetime}")
			print(f"Diff: {(received_last_message_datetime - sent_last_ping_datetime).total_seconds()} seconds")
			seconds_total = (sent_last_ping_datetime - sent_first_ping_datetime).total_seconds()
			messages_per_second = expected_pings_total / seconds_total
			print(f"Seconds to send all messages: {seconds_total}")
			print(f"Sent messages per seconds: {messages_per_second}")
			print(f"Seconds per sent message: {1.0 / messages_per_second}")
			seconds_total = (received_last_message_datetime - received_first_message_datetime).total_seconds()
			messages_per_second = expected_pings_total / seconds_total
			print(f"Seconds to receive all messages: {seconds_total}")
			print(f"Received messages per seconds: {messages_per_second}")
			print(f"Seconds per received message: {1.0 / messages_per_second}")

			print(f"{datetime.utcnow()}: disposing")

			client_messenger.dispose()

			print(f"{datetime.utcnow()}: disposed")

		ping_thread = start_thread(ping_thread_method)
		ping_thread.join()

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertIsNone(found_exception)

	def test_single_client_quickly_echos_burst_1KB(self):
		# spam pings and detect timing differences between sends and receives

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		expected_pings_total = 1000
		message_contents = "12345678" * 128

		print(f"{datetime.utcnow()}: sending first press")

		found_exception = None  # type: Exception

		def ping_thread_method():
			nonlocal expected_pings_total
			nonlocal found_exception
			nonlocal message_contents

			client_messenger = get_default_client_messenger()

			client_messenger.connect_to_server()

			expected_ping_index = 0
			received_first_message_datetime = None  # type: datetime
			received_last_message_datetime = None  # type: datetime
			callback_semaphore = Semaphore()

			def callback(client_server_message: ClientServerMessage):
				nonlocal expected_pings_total
				nonlocal expected_ping_index
				nonlocal received_first_message_datetime
				nonlocal received_last_message_datetime
				nonlocal callback_semaphore

				# print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
				self.assertIsInstance(client_server_message, EchoResponseBaseClientServerMessage)

				callback_semaphore.acquire()
				expected_ping_index += 1
				if expected_ping_index == 1:
					received_first_message_datetime = datetime.utcnow()
				if expected_ping_index == expected_pings_total:
					received_last_message_datetime = datetime.utcnow()
				callback_semaphore.release()

			def on_exception(exception: Exception):
				nonlocal found_exception
				found_exception = exception

			client_messenger.receive_from_server(
				callback=callback,
				on_exception=on_exception
			)

			print(f"{datetime.utcnow()}: sending first announcement")

			client_messenger.send_to_server(
				request_client_server_message=AnnounceBaseClientServerMessage(
					name="First"
				)
			)

			sent_first_ping_datetime = datetime.utcnow()
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=message_contents,
					is_ordered=True
				)
			)
			for index in range(expected_pings_total - 2):
				client_messenger.send_to_server(
					request_client_server_message=EchoRequestBaseClientServerMessage(
						message=message_contents,
						is_ordered=True
					)
				)
			sent_last_ping_datetime = datetime.utcnow()
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=message_contents,
					is_ordered=True
				)
			)

			print(f"{datetime.utcnow()}: waiting for messages")

			while received_last_message_datetime is None:
				time.sleep(1)
			time.sleep(1)

			print(f"Sent first message datetime: {sent_first_ping_datetime}")
			print(f"Received first message datetime: {received_first_message_datetime}")
			print(f"Diff: {(received_first_message_datetime - sent_first_ping_datetime).total_seconds()} seconds")
			print(f"Sent last message datetime: {sent_last_ping_datetime}")
			print(f"Received last message datetime: {received_last_message_datetime}")
			print(f"Diff: {(received_last_message_datetime - sent_last_ping_datetime).total_seconds()} seconds")
			seconds_total = (sent_last_ping_datetime - sent_first_ping_datetime).total_seconds()
			messages_per_second = expected_pings_total / seconds_total
			print(f"Seconds to send all messages: {seconds_total}")
			print(f"Sent messages per seconds: {messages_per_second}")
			print(f"Seconds per sent message: {1.0 / messages_per_second}")
			seconds_total = (received_last_message_datetime - received_first_message_datetime).total_seconds()
			messages_per_second = expected_pings_total / seconds_total
			print(f"Seconds to receive all messages: {seconds_total}")
			print(f"Received messages per seconds: {messages_per_second}")
			print(f"Seconds per received message: {1.0 / messages_per_second}")

			print(f"{datetime.utcnow()}: disposing")

			client_messenger.dispose()

			print(f"{datetime.utcnow()}: disposed")

		ping_thread = start_thread(ping_thread_method)
		ping_thread.join()

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertIsNone(found_exception)

	def test_single_client_quickly_echos_burst_5KB(self):
		# spam pings and detect timing differences between sends and receives

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		expected_pings_total = 1000
		message_contents = "12345678" * 128 * 5

		print(f"{datetime.utcnow()}: sending first press")

		found_exception = None  # type: Exception

		def ping_thread_method():
			nonlocal expected_pings_total
			nonlocal found_exception
			nonlocal message_contents

			client_messenger = get_default_client_messenger()

			client_messenger.connect_to_server()

			expected_ping_index = 0
			received_first_message_datetime = None  # type: datetime
			received_last_message_datetime = None  # type: datetime
			callback_semaphore = Semaphore()

			def callback(client_server_message: ClientServerMessage):
				nonlocal expected_pings_total
				nonlocal expected_ping_index
				nonlocal received_first_message_datetime
				nonlocal received_last_message_datetime
				nonlocal callback_semaphore

				# print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
				self.assertIsInstance(client_server_message, EchoResponseBaseClientServerMessage)

				callback_semaphore.acquire()
				expected_ping_index += 1
				if expected_ping_index == 1:
					received_first_message_datetime = datetime.utcnow()
				if expected_ping_index == expected_pings_total:
					received_last_message_datetime = datetime.utcnow()
				callback_semaphore.release()

			def on_exception(exception: Exception):
				nonlocal found_exception
				found_exception = exception

			client_messenger.receive_from_server(
				callback=callback,
				on_exception=on_exception
			)

			print(f"{datetime.utcnow()}: sending first announcement")

			client_messenger.send_to_server(
				request_client_server_message=AnnounceBaseClientServerMessage(
					name="First"
				)
			)

			sent_first_ping_datetime = datetime.utcnow()
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=message_contents,
					is_ordered=True
				)
			)
			for index in range(expected_pings_total - 2):
				client_messenger.send_to_server(
					request_client_server_message=EchoRequestBaseClientServerMessage(
						message=message_contents,
						is_ordered=True
					)
				)
			sent_last_ping_datetime = datetime.utcnow()
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=message_contents,
					is_ordered=True
				)
			)

			print(f"{datetime.utcnow()}: waiting for messages")

			while received_last_message_datetime is None:
				time.sleep(1)
			time.sleep(1)

			print(f"Sent first message datetime: {sent_first_ping_datetime}")
			print(f"Received first message datetime: {received_first_message_datetime}")
			print(f"Diff: {(received_first_message_datetime - sent_first_ping_datetime).total_seconds()} seconds")
			print(f"Sent last message datetime: {sent_last_ping_datetime}")
			print(f"Received last message datetime: {received_last_message_datetime}")
			print(f"Diff: {(received_last_message_datetime - sent_last_ping_datetime).total_seconds()} seconds")
			seconds_total = (sent_last_ping_datetime - sent_first_ping_datetime).total_seconds()
			messages_per_second = expected_pings_total / seconds_total
			print(f"Seconds to send all messages: {seconds_total}")
			print(f"Sent messages per seconds: {messages_per_second}")
			print(f"Seconds per sent message: {1.0 / messages_per_second}")
			seconds_total = (received_last_message_datetime - received_first_message_datetime).total_seconds()
			messages_per_second = expected_pings_total / seconds_total
			print(f"Seconds to receive all messages: {seconds_total}")
			print(f"Received messages per seconds: {messages_per_second}")
			print(f"Seconds per received message: {1.0 / messages_per_second}")

			print(f"{datetime.utcnow()}: disposing")

			client_messenger.dispose()

			print(f"{datetime.utcnow()}: disposed")

		ping_thread = start_thread(ping_thread_method)
		ping_thread.join()

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertIsNone(found_exception)

	def test_single_client_quickly_echos_burst_10KB(self):
		# spam pings and detect timing differences between sends and receives

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		expected_pings_total = 1000
		message_contents = "12345678" * 128 * 10

		print(f"{datetime.utcnow()}: sending first press")

		found_exception = None  # type: Exception

		def ping_thread_method():
			nonlocal expected_pings_total
			nonlocal found_exception
			nonlocal message_contents

			client_messenger = get_default_client_messenger()

			client_messenger.connect_to_server()

			expected_ping_index = 0
			received_first_message_datetime = None  # type: datetime
			received_last_message_datetime = None  # type: datetime
			callback_semaphore = Semaphore()

			def callback(client_server_message: ClientServerMessage):
				nonlocal expected_pings_total
				nonlocal expected_ping_index
				nonlocal received_first_message_datetime
				nonlocal received_last_message_datetime
				nonlocal callback_semaphore

				# print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
				self.assertIsInstance(client_server_message, EchoResponseBaseClientServerMessage)

				callback_semaphore.acquire()
				expected_ping_index += 1
				if expected_ping_index == 1:
					received_first_message_datetime = datetime.utcnow()
				if expected_ping_index == expected_pings_total:
					received_last_message_datetime = datetime.utcnow()
				callback_semaphore.release()

			def on_exception(exception: Exception):
				nonlocal found_exception
				found_exception = exception

			client_messenger.receive_from_server(
				callback=callback,
				on_exception=on_exception
			)

			print(f"{datetime.utcnow()}: sending first announcement")

			client_messenger.send_to_server(
				request_client_server_message=AnnounceBaseClientServerMessage(
					name="First"
				)
			)

			sent_first_ping_datetime = datetime.utcnow()
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=message_contents,
					is_ordered=True
				)
			)
			for index in range(expected_pings_total - 2):
				client_messenger.send_to_server(
					request_client_server_message=EchoRequestBaseClientServerMessage(
						message=message_contents,
						is_ordered=True
					)
				)
			sent_last_ping_datetime = datetime.utcnow()
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=message_contents,
					is_ordered=True
				)
			)

			print(f"{datetime.utcnow()}: waiting for messages")

			while received_last_message_datetime is None:
				time.sleep(1)
			time.sleep(1)

			print(f"Sent first message datetime: {sent_first_ping_datetime}")
			print(f"Received first message datetime: {received_first_message_datetime}")
			print(f"Diff: {(received_first_message_datetime - sent_first_ping_datetime).total_seconds()} seconds")
			print(f"Sent last message datetime: {sent_last_ping_datetime}")
			print(f"Received last message datetime: {received_last_message_datetime}")
			print(f"Diff: {(received_last_message_datetime - sent_last_ping_datetime).total_seconds()} seconds")
			seconds_total = (sent_last_ping_datetime - sent_first_ping_datetime).total_seconds()
			messages_per_second = expected_pings_total / seconds_total
			print(f"Seconds to send all messages: {seconds_total}")
			print(f"Sent messages per seconds: {messages_per_second}")
			print(f"Seconds per sent message: {1.0 / messages_per_second}")
			seconds_total = (received_last_message_datetime - received_first_message_datetime).total_seconds()
			messages_per_second = expected_pings_total / seconds_total
			print(f"Seconds to receive all messages: {seconds_total}")
			print(f"Received messages per seconds: {messages_per_second}")
			print(f"Seconds per received message: {1.0 / messages_per_second}")

			print(f"{datetime.utcnow()}: disposing")

			client_messenger.dispose()

			print(f"{datetime.utcnow()}: disposed")

		ping_thread = start_thread(ping_thread_method)
		ping_thread.join()

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertIsNone(found_exception)

	def test_client_attempts_message_impossible_for_structure_state_but_exception_in_callback(self):
		# attempt to reset the presses without first pressing the button

		client_messenger = get_default_client_messenger()

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger.connect_to_server()

		callback_total = 0

		expected_exception = Exception(f"Client should not receive any messages as part of this test.")

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			nonlocal expected_exception
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			raise expected_exception

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending press")

		client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		time.sleep(1)

		print(f"{datetime.utcnow()}: sending announcement")

		client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="Test Name"
			)
		)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing")

		client_messenger.dispose()

		print(f"{datetime.utcnow()}: disposed")

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertEqual(1, callback_total)
		self.assertIsNotNone(found_exception)
		self.assertEqual(expected_exception, found_exception)

	def test_client_attempts_message_impossible_for_structure_state(self):
		# attempt to reset the presses without first pressing the button

		client_messenger = get_default_client_messenger()

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, AnnounceFailedBaseClientServerMessage)

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending press")

		client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		time.sleep(1)

		print(f"{datetime.utcnow()}: sending announcement")

		client_messenger.send_to_server(
			request_client_server_message=AnnounceBaseClientServerMessage(
				name="Test Name"
			)
		)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing")

		client_messenger.dispose()

		print(f"{datetime.utcnow()}: disposed")

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertEqual(1, callback_total)
		self.assertIsNone(found_exception)

	def test_order_of_messages(self):
		# send multiple messages from the same client to the server, expecting the response order to be the same

		messages_total = 1000

		print(f"{datetime.utcnow()}: setting up server")

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		print(f"{datetime.utcnow()}: setting up client")
		time.sleep(1)

		client_messenger = get_default_client_messenger()

		callback_total = 0
		last_message_index = -1
		failed_at_message_index = None  # type: int

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			nonlocal last_message_index
			nonlocal failed_at_message_index
			# print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			self.assertIsInstance(client_server_message, EchoResponseBaseClientServerMessage)
			echo_response_client_server_message = client_server_message  # type: EchoResponseBaseClientServerMessage

			if int(echo_response_client_server_message.get_message()) == last_message_index + 1:
				# correct message received
				last_message_index += 1
			else:
				if failed_at_message_index is None:
					failed_at_message_index = last_message_index

			callback_total += 1

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			if found_exception is None:
				found_exception = exception

		# TODO determine why the first thread to spawn as a part of the connect_to_server process does not die
		client_messenger.connect_to_server()

		time.sleep(1)

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)
		
		time.sleep(1)

		for message_index in range(messages_total):
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=str(message_index),
					is_ordered=True
				)
			)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing client messenger: start")

		client_messenger.dispose()

		print(f"{datetime.utcnow()}: disposing client messenger: end")

		time.sleep(1)

		print(f"{datetime.utcnow()}: server_messenger.stop_receiving_from_clients(): start")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: server_messenger.stop_receiving_from_clients(): end")

		if found_exception is not None:
			raise found_exception
		self.assertIsNone(failed_at_message_index)

		print(f"end")

	def test_two_clients_becoming_out_of_sync(self):
		# as the delay between two different clients send messages shrinks, how often are the messages received in the wrong order

		current_delay_between_messages_seconds = 1
		delay_percentage_decrease_delta = 0.1
		minimum_delay_between_messages_seconds = 0.0001
		accepted_delay_between_messages_that_could_result_in_disorder = 0.001

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		print(f"{datetime.utcnow()}: setting up clients")
		time.sleep(1)

		client_messengers = []  # type: List[ClientMessenger]
		client_messengers.append(get_default_client_messenger())
		client_messengers.append(get_default_client_messenger())

		callback_total = 0
		last_message_index = -1
		failed_at_message_index = None  # type: int

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			nonlocal last_message_index
			nonlocal failed_at_message_index
			#print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			self.assertIsInstance(client_server_message, EchoResponseBaseClientServerMessage)
			echo_response_client_server_message = client_server_message  # type: EchoResponseBaseClientServerMessage

			if int(echo_response_client_server_message.get_message()) == last_message_index + 1:
				# correct message received
				last_message_index += 1
			else:
				if failed_at_message_index is None:
					failed_at_message_index = last_message_index

			callback_total += 1

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			if found_exception is None:
				found_exception = exception

		for client_messenger in client_messengers:
			client_messenger.connect_to_server()
			client_messenger.receive_from_server(
				callback=callback,
				on_exception=on_exception
			)

		print(f"{datetime.utcnow()}: sending messages")

		client_messengers_index = 0
		message_index = 0
		client_messengers[client_messengers_index].send_to_server(
			request_client_server_message=EchoRequestBaseClientServerMessage(
				message=str(message_index),
				is_ordered=True
			)
		)
		message_index += 1
		while minimum_delay_between_messages_seconds < current_delay_between_messages_seconds and failed_at_message_index is None:
			time.sleep(current_delay_between_messages_seconds)
			client_messengers_index += 1
			if client_messengers_index == len(client_messengers):
				client_messengers_index = 0
			client_messengers[client_messengers_index].send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=str(message_index),
					is_ordered=True
				)
			)
			message_index += 1
			current_delay_between_messages_seconds -= current_delay_between_messages_seconds * delay_percentage_decrease_delta

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing")

		for client_messenger in client_messengers:
			client_messenger.dispose()

		print(f"{datetime.utcnow()}: disposed")

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		if found_exception is not None:
			raise found_exception
		print(f"{datetime.utcnow()}: last successful index {failed_at_message_index} with delay of {current_delay_between_messages_seconds} seconds")
		self.assertLess(current_delay_between_messages_seconds, accepted_delay_between_messages_that_could_result_in_disorder)

	def test_dispose_client_too_quickly_before_receiving_all_messages(self):
		# a thread seems to remain alive when this happens
		# NOTE: the client_socket read only gets to 988 before it stops reading

		messages_total = 1000

		print(f"{datetime.utcnow()}: setting up server")

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		print(f"{datetime.utcnow()}: setting up client")
		time.sleep(1)

		client_messenger = get_default_client_messenger()

		callback_total = 0
		last_message_index = -1
		failed_at_message_index = None  # type: int

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			nonlocal last_message_index
			nonlocal failed_at_message_index
			# print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			self.assertIsInstance(client_server_message, EchoResponseBaseClientServerMessage)
			echo_response_client_server_message = client_server_message  # type: EchoResponseBaseClientServerMessage

			if int(echo_response_client_server_message.get_message()) == last_message_index + 1:
				# correct message received
				last_message_index += 1
			else:
				if failed_at_message_index is None:
					failed_at_message_index = last_message_index

			callback_total += 1

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			if found_exception is None:
				found_exception = exception

		print(f"{datetime.utcnow()}: connecting to server")

		client_messenger.connect_to_server()

		print(f"{datetime.utcnow()}: receiving from server")

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending messages")

		for message_index in range(messages_total):
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=str(message_index),
					is_ordered=True
				)
			)

		print(f"{datetime.utcnow()}: immediately disposing")

		client_messenger.dispose()

		server_messenger.stop_receiving_from_clients()

		if found_exception is not None:
			raise found_exception
		self.assertIsNone(failed_at_message_index)

	def test_parse_client_server_message_raises_exception_when_receiving_in_server_messenger(self):

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, ErrorResponseBaseClientServerMessage)

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending error messages")

		expected_exception = f"test exception: {uuid.uuid4()}"

		client_messenger.send_to_server(
			request_client_server_message=ErrorRequestBaseClientServerMessage(
				is_constructor_exception_to_set=expected_exception
			)
		)

		print(f"{datetime.utcnow()}: wait for messages")

		time.sleep(5)

		client_messenger.dispose()

		time.sleep(1)

		with self.assertRaises(Exception) as assertedException:
			server_messenger.stop_receiving_from_clients()

		self.assertEqual(expected_exception, str(assertedException.exception))

		# the server encountered an exception, closing the connection
		self.assertIsInstance(found_exception, ReadWriteSocketClosedException)

	def test_determining_client_server_message_type_raises_exception_when_sending_to_server_messenger(self):

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, ErrorResponseBaseClientServerMessage)

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending error messages")

		expected_exception = f"test exception: {uuid.uuid4()}"

		with self.assertRaises(Exception) as assertedException:
			client_messenger.send_to_server(
				request_client_server_message=ErrorRequestBaseClientServerMessage(
					get_client_server_message_type_exception=expected_exception
				)
			)

		self.assertEqual(expected_exception, str(assertedException.exception))

		time.sleep(5)

		client_messenger.dispose()

		time.sleep(1)

		server_messenger.stop_receiving_from_clients()

		if found_exception is not None:
			raise found_exception

	def test_getting_json_of_client_server_message_raises_exception_when_sending_to_server_messenger(self):

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, ErrorResponseBaseClientServerMessage)

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending error messages")

		expected_exception = f"test exception: {uuid.uuid4()}"

		with self.assertRaises(Exception) as assertedException:
			client_messenger.send_to_server(
				request_client_server_message=ErrorRequestBaseClientServerMessage(
					to_json_exception=expected_exception
				)
			)

		self.assertEqual(expected_exception, str(assertedException.exception))

		time.sleep(5)

		client_messenger.dispose()

		time.sleep(1)

		server_messenger.stop_receiving_from_clients()

		if found_exception is not None:
			raise found_exception

	def test_check_if_is_response_client_server_message_raises_exception_when_processing_in_server_messenger(self):

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, ErrorResponseBaseClientServerMessage)

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending error messages")

		expected_exception = f"test exception: {uuid.uuid4()}"

		client_messenger.send_to_server(
			request_client_server_message=ErrorRequestBaseClientServerMessage(
				is_response_exception=expected_exception
			)
		)

		time.sleep(1)

		client_messenger.send_to_server(
			request_client_server_message=PingRequestBaseClientServerMessage()
		)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		client_messenger.dispose()

		time.sleep(1)

		with self.assertRaises(Exception) as assertedException:
			server_messenger.stop_receiving_from_clients()

		self.assertEqual(expected_exception, str(assertedException.exception))

		# the server encountered an exception but did not close the connect due to it and is still receiving requests
		if found_exception is not None:
			raise found_exception

	def test_getting_destination_uuid_from_client_server_message_raises_exception_when_processing_in_server_messenger(self):

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			if callback_total == 0:
				self.assertIsInstance(client_server_message, ErrorResponseBaseClientServerMessage)
			else:
				self.assertIsInstance(client_server_message, PingResponseBaseClientServerMessage)
			callback_total += 1

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending error messages")

		expected_exception = f"test exception: {uuid.uuid4()}"

		client_messenger.send_to_server(
			request_client_server_message=ErrorRequestBaseClientServerMessage(
				get_destination_uuid_exception=expected_exception
			)
		)

		time.sleep(1)

		client_messenger.send_to_server(
			request_client_server_message=PingRequestBaseClientServerMessage()
		)

		time.sleep(5)

		client_messenger.dispose()

		time.sleep(1)

		server_messenger.stop_receiving_from_clients()

		# the server encountered an exception but did not close the connect due to it and is still receiving requests
		if found_exception is not None:
			raise found_exception

	def test_checking_if_is_structural_influence_from_client_server_message_raises_exception_when_processing_in_server_messenger(self):

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			if callback_total == 0:
				self.assertIsInstance(client_server_message, ErrorResponseBaseClientServerMessage)
			else:
				self.assertIsInstance(client_server_message, PingResponseBaseClientServerMessage)
			callback_total += 1

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending error messages")

		expected_exception = f"test exception: {uuid.uuid4()}"

		client_messenger.send_to_server(
			request_client_server_message=ErrorRequestBaseClientServerMessage(
				is_structural_influence_exception=expected_exception
			)
		)

		time.sleep(1)

		client_messenger.send_to_server(
			request_client_server_message=PingRequestBaseClientServerMessage()
		)

		time.sleep(5)

		client_messenger.dispose()

		time.sleep(1)

		with self.assertRaises(Exception) as assertedException:
			server_messenger.stop_receiving_from_clients()

		self.assertEqual(expected_exception, str(assertedException.exception))

		# the server encountered an exception but did not close the connect due to it and is still receiving requests
		if found_exception is not None:
			raise found_exception

	def test_checking_if_is_ordered_from_client_server_message_raises_exception_when_processing_in_server_messenger_with_ping_exception(self):

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			if callback_total == 0:
				self.assertIsInstance(client_server_message, ErrorResponseBaseClientServerMessage)
			else:
				self.assertIsInstance(client_server_message, PingResponseBaseClientServerMessage)
			callback_total += 1

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending error messages")

		expected_exception = f"test exception: {uuid.uuid4()}"

		client_messenger.send_to_server(
			request_client_server_message=ErrorRequestBaseClientServerMessage(
				is_ordered_exception=expected_exception
			)
		)

		time.sleep(1)

		with self.assertRaises(ReadWriteSocketClosedException):
			client_messenger.send_to_server(
				request_client_server_message=PingRequestBaseClientServerMessage()
			)

		time.sleep(5)

		client_messenger.dispose()

		time.sleep(1)

		with self.assertRaises(Exception) as assertedException:
			server_messenger.stop_receiving_from_clients()

		self.assertEqual(expected_exception, str(assertedException.exception))

		# the client messenger encountered an exception within the read loop after the server messenger encountered an exception and closed the connection
		self.assertIsInstance(found_exception, ReadWriteSocketClosedException)

	def test_checking_if_is_ordered_from_client_server_message_raises_exception_when_processing_in_server_messenger(self):

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			if callback_total == 0:
				self.assertIsInstance(client_server_message, ErrorResponseBaseClientServerMessage)
			else:
				self.assertIsInstance(client_server_message, PingResponseBaseClientServerMessage)
			callback_total += 1

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending error messages")

		expected_exception = f"test exception: {uuid.uuid4()}"

		client_messenger.send_to_server(
			request_client_server_message=ErrorRequestBaseClientServerMessage(
				is_ordered_exception=expected_exception
			)
		)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		client_messenger.dispose()

		time.sleep(1)

		with self.assertRaises(Exception) as assertedException:
			server_messenger.stop_receiving_from_clients()

		self.assertEqual(expected_exception, str(assertedException.exception))

		# the client messenger encountered an exception within the read loop after the server messenger encountered an exception and closed the connection
		self.assertIsInstance(found_exception, ReadWriteSocketClosedException)

	def test_getting_structural_error_client_server_message_response_from_client_server_message_raises_exception_when_processing_in_server_messenger_but_succeeds(self):

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			if callback_total == 0:
				self.assertIsInstance(client_server_message, ErrorResponseBaseClientServerMessage)
			else:
				self.assertIsInstance(client_server_message, PingResponseBaseClientServerMessage)
			callback_total += 1

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending error messages")

		expected_exception = f"test exception: {uuid.uuid4()}"

		client_messenger.send_to_server(
			request_client_server_message=ErrorRequestBaseClientServerMessage(
				get_structural_error_client_server_message_response_exception=expected_exception
			)
		)

		time.sleep(1)

		client_messenger.send_to_server(
			request_client_server_message=PingRequestBaseClientServerMessage()
		)

		time.sleep(5)

		client_messenger.dispose()

		time.sleep(1)

		server_messenger.stop_receiving_from_clients()

		# the server encountered an exception but did not close the connect due to it and is still receiving requests
		if found_exception is not None:
			raise found_exception

	def test_getting_structural_error_client_server_message_response_from_client_server_message_raises_exception_when_processing_in_server_messenger_and_causes_exception(self):

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			if callback_total == 0:
				self.assertIsInstance(client_server_message, ErrorResponseBaseClientServerMessage)
			else:
				self.assertIsInstance(client_server_message, PingResponseBaseClientServerMessage)
			callback_total += 1

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending error messages")

		expected_exception = f"test exception: {uuid.uuid4()}"

		client_messenger.send_to_server(
			request_client_server_message=PressButtonBaseClientServerMessage()
		)

		time.sleep(1)

		client_messenger.send_to_server(
			request_client_server_message=ErrorRequestBaseClientServerMessage(
				get_structural_error_client_server_message_response_exception=expected_exception
			)
		)

		time.sleep(1)

		client_messenger.send_to_server(
			request_client_server_message=PingRequestBaseClientServerMessage()
		)

		time.sleep(5)

		client_messenger.dispose()

		time.sleep(1)

		with self.assertRaises(Exception) as assertedException:
			server_messenger.stop_receiving_from_clients()

		self.assertEqual(expected_exception, str(assertedException.exception))

		# the server encountered an exception but did not close the connect due to it and is still receiving requests
		if found_exception is not None:
			raise found_exception

	def test_getting_structural_error_client_server_message_response_from_client_server_message_raises_exception_when_processing_in_server_messenger_and_causes_exception(self):

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0

		def callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: callback: client_server_message: {client_server_message.to_json()}")
			if callback_total == 0:
				self.assertIsInstance(client_server_message, ErrorResponseBaseClientServerMessage)
			else:
				self.assertIsInstance(client_server_message, PingResponseBaseClientServerMessage)
			callback_total += 1

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending error messages")

		expected_exception = f"test exception: {uuid.uuid4()}"

		client_messenger.send_to_server(
			request_client_server_message=ErrorRequestBaseClientServerMessage(
				response_constructor_arguments={
					"is_constructor_exception_to_set": expected_exception
				}
			)
		)

		time.sleep(1)

		client_messenger.send_to_server(
			request_client_server_message=PingRequestBaseClientServerMessage()
		)

		time.sleep(5)

		client_messenger.dispose()

		time.sleep(1)

		with self.assertRaises(Exception) as assertedException:
			server_messenger.stop_receiving_from_clients()

		self.assertEqual(expected_exception, str(assertedException.exception))

		# the server encountered an exception but did not close the connect due to it and is still receiving requests
		if found_exception is not None:
			raise found_exception

	def test_unordered_client_server_messages_100m_10s(self):

		messages_total = 100
		message_subset_length = 10

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0
		previous_ordered_index = -1 - message_subset_length
		previous_unordered_index = -1
		is_printing = False

		def callback(echo_response: EchoResponseBaseClientServerMessage):
			nonlocal callback_total
			nonlocal previous_ordered_index
			nonlocal previous_unordered_index
			nonlocal is_printing
			nonlocal message_subset_length

			#print(f"{datetime.utcnow()}: callback: echo_response: {echo_response.to_json()}")
			self.assertIsInstance(echo_response, EchoResponseBaseClientServerMessage)
			callback_total += 1

			index = int(echo_response.get_message())
			print(f"index: {index}")
			subset_index = int(index / message_subset_length) % 2
			print(f"subset_index: {subset_index}")
			previous_subset_index = math.floor((index - 1) / message_subset_length) % 2
			print(f"previous_subset_index: {previous_subset_index}")
			if subset_index == 0:
				if previous_subset_index != subset_index:
					if previous_ordered_index + message_subset_length + 1 != index:
						raise Exception(f"Failed to jump to next index at index: {index}")
					else:
						if is_printing:
							print(f"{datetime.utcnow()}: found ordered index: {index}")
				else:
					if previous_ordered_index + 1 != index:
						raise Exception(f"Failed to find next index at index: {index}")
					else:
						if is_printing:
							print(f"{datetime.utcnow()}: found ordered index: {index}")
				previous_ordered_index = index
			else:
				if previous_subset_index != subset_index:
					if previous_unordered_index + message_subset_length + 1 != index:
						raise Exception(f"Failed to jump to next index at index: {index}")
					else:
						if is_printing:
							print(f"{datetime.utcnow()}: found unordered index: {index}")
				else:
					if previous_unordered_index + 1 != index:
						raise Exception(f"Failed to find next index at index: {index}")
					else:
						if is_printing:
							print(f"{datetime.utcnow()}: found unordered index: {index}")
				previous_unordered_index = index

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending messages")

		for index in range(messages_total):
			subset_index = int(index / message_subset_length) % 2
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=str(index),
					is_ordered=(subset_index == 0)
				)
			)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing client")

		client_messenger.dispose()

		time.sleep(1)

		server_messenger.stop_receiving_from_clients()

		# the server encountered an exception but did not close the connect due to it and is still receiving requests
		if found_exception is not None:
			raise found_exception

	def test_unordered_client_server_messages_100m_1s(self):

		messages_total = 100
		message_subset_length = 1

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0
		previous_ordered_index = -1 - message_subset_length
		previous_unordered_index = -1
		is_printing = False

		def callback(echo_response: EchoResponseBaseClientServerMessage):
			nonlocal callback_total
			nonlocal previous_ordered_index
			nonlocal previous_unordered_index
			nonlocal is_printing
			nonlocal message_subset_length

			#print(f"{datetime.utcnow()}: callback: echo_response: {echo_response.to_json()}")
			self.assertIsInstance(echo_response, EchoResponseBaseClientServerMessage)
			callback_total += 1

			index = int(echo_response.get_message())
			#print(f"index: {index}")
			subset_index = int(index / message_subset_length) % 2
			#print(f"subset_index: {subset_index}")
			previous_subset_index = math.floor((index - 1) / message_subset_length) % 2
			#print(f"previous_subset_index: {previous_subset_index}")
			if subset_index == 0:
				if previous_subset_index != subset_index:
					if previous_ordered_index + message_subset_length + 1 != index:
						raise Exception(f"Failed to jump to next index at index: {index}")
					else:
						if is_printing:
							print(f"{datetime.utcnow()}: found ordered index: {index}")
				else:
					if previous_ordered_index + 1 != index:
						raise Exception(f"Failed to find next index at index: {index}")
					else:
						if is_printing:
							print(f"{datetime.utcnow()}: found ordered index: {index}")
				previous_ordered_index = index
			else:
				if previous_subset_index != subset_index:
					if previous_unordered_index + message_subset_length + 1 != index:
						raise Exception(f"Failed to jump to next index at index: {index}")
					else:
						if is_printing:
							print(f"{datetime.utcnow()}: found unordered index: {index}")
				else:
					if previous_unordered_index + 1 != index:
						raise Exception(f"Failed to find next index at index: {index}")
					else:
						if is_printing:
							print(f"{datetime.utcnow()}: found unordered index: {index}")
				previous_unordered_index = index

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending messages")

		for index in range(messages_total):
			subset_index = int(index / message_subset_length) % 2
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=str(index),
					is_ordered=(subset_index == 0)
				)
			)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing client")

		client_messenger.dispose()

		time.sleep(1)

		server_messenger.stop_receiving_from_clients()

		# the server encountered an exception but did not close the connect due to it and is still receiving requests
		if found_exception is not None:
			raise found_exception

	def test_unordered_client_server_messages_1000m_1s(self):

		messages_total = 1000
		message_subset_length = 1

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		client_messenger = get_default_client_messenger()

		client_messenger.connect_to_server()

		callback_total = 0
		previous_ordered_index = -1 - message_subset_length
		previous_unordered_index = -1
		is_printing = False

		def callback(echo_response: EchoResponseBaseClientServerMessage):
			nonlocal callback_total
			nonlocal previous_ordered_index
			nonlocal previous_unordered_index
			nonlocal is_printing
			nonlocal message_subset_length

			#print(f"{datetime.utcnow()}: callback: echo_response: {echo_response.to_json()}")
			self.assertIsInstance(echo_response, EchoResponseBaseClientServerMessage)
			callback_total += 1

			index = int(echo_response.get_message())
			#print(f"index: {index}")
			subset_index = int(index / message_subset_length) % 2
			#print(f"subset_index: {subset_index}")
			previous_subset_index = math.floor((index - 1) / message_subset_length) % 2
			#print(f"previous_subset_index: {previous_subset_index}")
			if subset_index == 0:
				if previous_subset_index != subset_index:
					if previous_ordered_index + message_subset_length + 1 != index:
						raise Exception(f"Failed to jump to next index at index: {index}")
					else:
						if is_printing:
							print(f"{datetime.utcnow()}: found ordered index: {index}")
				else:
					if previous_ordered_index + 1 != index:
						raise Exception(f"Failed to find next index at index: {index}")
					else:
						if is_printing:
							print(f"{datetime.utcnow()}: found ordered index: {index}")
				previous_ordered_index = index
			else:
				if previous_subset_index != subset_index:
					if previous_unordered_index + message_subset_length + 1 != index:
						raise Exception(f"Failed to jump to next index at index: {index}")
					else:
						if is_printing:
							print(f"{datetime.utcnow()}: found unordered index: {index}")
				else:
					if previous_unordered_index + 1 != index:
						raise Exception(f"Failed to find next index at index: {index}")
					else:
						if is_printing:
							print(f"{datetime.utcnow()}: found unordered index: {index}")
				previous_unordered_index = index

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messenger.receive_from_server(
			callback=callback,
			on_exception=on_exception
		)

		print(f"{datetime.utcnow()}: sending messages")

		for index in range(messages_total):
			subset_index = int(index / message_subset_length) % 2
			client_messenger.send_to_server(
				request_client_server_message=EchoRequestBaseClientServerMessage(
					message=str(index),
					is_ordered=(subset_index == 0)
				)
			)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing client")

		client_messenger.dispose()

		time.sleep(1)

		server_messenger.stop_receiving_from_clients()

		# the server encountered an exception but did not close the connect due to it and is still receiving requests
		if found_exception is not None:
			raise found_exception

	def test_child_structure_power_once(self):

		pass
