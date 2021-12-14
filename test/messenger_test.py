from __future__ import annotations
import unittest
from src.austin_heller_repo.socket_kafka_message_framework import ClientMessenger, ServerMessenger, ClientServerMessage, ClientServerMessageTypeEnum, Structure, StructureStateEnum, UpdateStructureInfluence, StructureFactory
from austin_heller_repo.socket import ClientSocketFactory, ServerSocketFactory
from austin_heller_repo.common import HostPointer
from austin_heller_repo.kafka_manager import KafkaManagerFactory, KafkaWrapper
from austin_heller_repo.threading import start_thread, Semaphore
from typing import List, Tuple, Dict, Callable, Type
import uuid
import time
from datetime import datetime
from abc import ABC, abstractmethod
import multiprocessing as mp
import matplotlib.pyplot as plt


is_socket_debug_active = False
is_client_messenger_debug_active = False
is_server_messenger_debug_active = False
is_kafka_debug_active = False


class BaseClientServerMessage(ClientServerMessage, ABC):
	pass

	@staticmethod
	def parse_from_json(*, json_object: Dict):
		base_client_server_message_type = BaseClientServerMessageTypeEnum(json_object["__type"])
		if base_client_server_message_type == BaseClientServerMessageTypeEnum.Announce:
			return AnnounceBaseClientServerMessage.parse_from_json(
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
		else:
			raise Exception(f"Unexpected BaseClientServerMessageTypeEnum: {base_client_server_message_type}")


class BaseClientServerMessageTypeEnum(ClientServerMessageTypeEnum):
	HelloWorld = "hello_world"  # basic test
	Announce = "announce"  # announces name to structure
	PressButton = "press_button"  # structural influence, three presses cause broadcast of transmission to users
	ResetButton = "reset_button"  # structural_influence, resets number of presses and informs button pressers that it was reset
	ResetTransmission = "reset_transmission"  # directed to specific users that pressed the button
	ThreePressesTransmission = "three_presses_transmission"  # broadcasts to all users that the button was pressed three times and then resets the button
	PingRequest = "ping_request"  # pings the server and gets a response
	PingResponse = "ping_response"  # the response from the ping request


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

	def is_directed_to_destination_uuid(self, *, destination_uuid: str) -> bool:
		return False

	def is_structural_influence(self) -> bool:
		return False


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

	def is_directed_to_destination_uuid(self, *, destination_uuid: str) -> bool:
		return False

	def is_structural_influence(self) -> bool:
		return True


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

	def is_directed_to_destination_uuid(self, *, destination_uuid: str) -> bool:
		return False

	def is_structural_influence(self) -> bool:
		return True


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

	def is_directed_to_destination_uuid(self, *, destination_uuid: str) -> bool:
		return False

	def is_structural_influence(self) -> bool:
		return True


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

	def is_directed_to_destination_uuid(self, *, destination_uuid: str) -> bool:
		return self.__client_uuid == destination_uuid

	def is_structural_influence(self) -> bool:
		return False


class ThreePressesTransmissionBaseClientServerMessage(BaseClientServerMessage):

	def __init__(self, *, client_uuid: str):
		super().__init__()

		self.__client_uuid = client_uuid

	def get_client_server_message_type(self) -> ClientServerMessageTypeEnum:
		return BaseClientServerMessageTypeEnum.ThreePressesTransmission

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["client_uuid"] = self.__client_uuid
		return json_object

	@staticmethod
	def parse_from_json(*, json_object: Dict) -> ThreePressesTransmissionBaseClientServerMessage:
		ClientServerMessage.remove_base_keys(
			json_object=json_object
		)
		if len(json_object) != 1:
			raise Exception(f"Unexpected properties sent to be parsed into {ThreePressesTransmissionBaseClientServerMessage.__name__}")
		return ThreePressesTransmissionBaseClientServerMessage(
			**json_object
		)

	def is_response(self) -> bool:
		return True

	def is_directed_to_destination_uuid(self, *, destination_uuid: str) -> bool:
		return self.__client_uuid == destination_uuid

	def is_structural_influence(self) -> bool:
		return True


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

	def is_directed_to_destination_uuid(self, *, destination_uuid: str) -> bool:
		return False

	def is_structural_influence(self) -> bool:
		return True


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

	def is_directed_to_destination_uuid(self, *, destination_uuid: str) -> bool:
		return self.__client_uuid == destination_uuid

	def is_structural_influence(self) -> bool:
		return False


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

	def __name_announced(self, update_structure_influence: UpdateStructureInfluence):
		client_uuid = update_structure_influence.get_socket_kafka_message().get_source_uuid()
		announce_base_client_server_message = update_structure_influence.get_socket_kafka_message().get_client_server_message()  # type: AnnounceBaseClientServerMessage
		self.__name_per_client_uuid[client_uuid] = announce_base_client_server_message.get_name()

	def __button_pressed(self, update_structure_influence: UpdateStructureInfluence):
		client_uuid = update_structure_influence.get_socket_kafka_message().get_source_uuid()
		if client_uuid not in self.__pressed_button_client_uuids:
			self.__pressed_button_client_uuids.append(client_uuid)
		if client_uuid in self.__name_per_client_uuid.keys():
			print(f"button pressed by {self.__name_per_client_uuid[client_uuid]}")
		else:
			print(f"button pressed by {client_uuid}")
		self.__presses_total += 1
		if self.__presses_total == 3:
			update_structure_influence.add_response_client_server_message(
				client_server_message=ThreePressesTransmissionBaseClientServerMessage(
					client_uuid=client_uuid
				)
			)

	def __button_reset(self, update_structure_influence: UpdateStructureInfluence):
		for client_uuid in self.__pressed_button_client_uuids:
			client_server_message = ResetTransmissionBaseClientServerMessage(
				client_uuid=client_uuid
			)
			update_structure_influence.add_response_client_server_message(
				client_server_message=client_server_message
			)
		self.__pressed_button_client_uuids.clear()

	def __three_presses_transmission_sent(self, update_structure_influence: UpdateStructureInfluence):
		self.__pressed_button_client_uuids.clear()

	def __ping_requested(self, update_structure_influence: UpdateStructureInfluence):
		client_uuid = update_structure_influence.get_socket_kafka_message().get_source_uuid()
		update_structure_influence.add_response_client_server_message(
			client_server_message=PingResponseBaseClientServerMessage(
				client_uuid=client_uuid,
				ping_index=self.__pings_total
			)
		)
		self.__pings_total += 1


class ButtonStructureFactory(StructureFactory):

	def __init__(self):
		super().__init__()

		pass

	def get_structure(self) -> Structure:
		return ButtonStructure()


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
			server_read_failed_delay_seconds=0.01,
			is_ssl=False,
			is_debug=is_socket_debug_active
		),
		server_host_pointer=get_default_local_host_pointer(),
		client_server_message_class=BaseClientServerMessage,
		receive_from_server_polling_seconds=0,
		is_debug=is_client_messenger_debug_active
	)


def get_default_server_messenger() -> ServerMessenger:

	kafka_topic_name = str(uuid.uuid4())

	kafka_manager = get_default_kafka_manager_factory().get_kafka_manager()

	kafka_manager.add_topic(
		topic_name=kafka_topic_name
	).get_result()

	return ServerMessenger(
		server_socket_factory=ServerSocketFactory(
			to_client_packet_bytes_length=4096,
			listening_limit_total=10,
			accept_timeout_seconds=1.0,
			client_read_failed_delay_seconds=0.001,
			is_ssl=False,
			is_debug=is_socket_debug_active
		),
		kafka_manager_factory=get_default_kafka_manager_factory(),
		local_host_pointer=get_default_local_host_pointer(),
		kafka_topic_name=kafka_topic_name,
		client_server_message_class=BaseClientServerMessage,
		structure_factory=ButtonStructureFactory(),
		is_debug=is_server_messenger_debug_active
	)


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

		client_messenger.receive_from_server(
			callback=callback
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

		client_messenger.receive_from_server(
			callback=callback
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

		first_client_messenger.receive_from_server(
			callback=first_callback
		)

		def second_callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: second_callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, ResetTransmissionBaseClientServerMessage)

		second_client_messenger.receive_from_server(
			callback=second_callback
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

		first_client_messenger.receive_from_server(
			callback=first_callback
		)

		def second_callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: second_callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			self.assertIsInstance(client_server_message, ResetTransmissionBaseClientServerMessage)

		second_client_messenger.receive_from_server(
			callback=second_callback
		)

		def third_callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: third_callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			raise Exception(f"Third client should not receive a message.")

		third_client_messenger.receive_from_server(
			callback=third_callback
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

		first_client_messenger.receive_from_server(
			callback=first_callback
		)

		def second_callback(client_server_message: ClientServerMessage):
			nonlocal callback_total
			print(f"{datetime.utcnow()}: second_callback: client_server_message: {client_server_message.to_json()}")
			callback_total += 1
			raise Exception("This client should not be receiving a message.")

		second_client_messenger.receive_from_server(
			callback=second_callback
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

		time.sleep(0.25)

		print(f"{datetime.utcnow()}: disposing first client")

		first_client_messenger.dispose()

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: sending reset")

		second_client_messenger.send_to_server(
			request_client_server_message=ResetButtonBaseClientServerMessage()
		)

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

		print(f"{datetime.utcnow()}: disposing")

		second_client_messenger.dispose()

		print(f"{datetime.utcnow()}: disposed")

		print(f"{datetime.utcnow()}: stopping")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped")

		time.sleep(5)

		self.assertEqual(0, callback_total)

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

		client_messenger.receive_from_server(
			callback=callback
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

		client_messenger.receive_from_server(
			callback=callback
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

	def test_single_client_quickly_pings_burst(self):
		# spam pings and detect timing differences between sends and receives

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		expected_pings_total = 1370

		print(f"{datetime.utcnow()}: sending first press")

		def ping_thread_method():
			nonlocal expected_pings_total

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

			client_messenger.receive_from_server(
				callback=callback
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

	def test_single_client_quickly_pings_delayed(self):
		# spam pings and detect timing differences between sends and receives

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		expected_pings_total = 1000
		delay_between_sending_message_seconds = 0.01

		print(f"{datetime.utcnow()}: sending first press")

		def ping_thread_method():
			nonlocal expected_pings_total
			nonlocal delay_between_sending_message_seconds

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

			client_messenger.receive_from_server(
				callback=callback
			)

			print(f"{datetime.utcnow()}: sending first announcement")

			client_messenger.send_to_server(
				request_client_server_message=AnnounceBaseClientServerMessage(
					name="First"
				)
			)

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

			print(f"Min diff seconds {min(diff_seconds_totals)} at {diff_seconds_totals.index(min(diff_seconds_totals))}")
			print(f"Max diff seconds {max(diff_seconds_totals)} at {diff_seconds_totals.index(max(diff_seconds_totals))}")
			print(f"Ave diff seconds {sum(diff_seconds_totals)/expected_pings_total}")

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

	def test_two_clients_becoming_out_of_sync(self):

		pass
