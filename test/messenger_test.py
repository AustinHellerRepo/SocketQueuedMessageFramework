from __future__ import annotations
import unittest
from src.austin_heller_repo.socket_kafka_message_framework import ClientMessenger, ServerMessenger, ClientServerMessage, ClientServerMessageTypeEnum, Structure, StructureStateEnum, UpdateStructureInfluence, StructureFactory
from austin_heller_repo.socket import ClientSocketFactory, ServerSocketFactory
from austin_heller_repo.common import HostPointer
from austin_heller_repo.kafka_manager import KafkaManagerFactory, KafkaWrapper
from typing import List, Tuple, Dict, Callable, Type
import uuid
import time
from datetime import datetime
from abc import ABC, abstractmethod


is_socket_debug_active = False
is_server_messenger_debug_active = True


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
		else:
			raise Exception(f"Unexpected BaseClientServerMessageTypeEnum: {base_client_server_message_type}")


class BaseClientServerMessageTypeEnum(ClientServerMessageTypeEnum):
	HelloWorld = "hello_world"  # basic test
	Announce = "announce"  # announces name to structure
	PressButton = "press_button"  # structural influence, three presses cause broadcast of transmission to users
	ResetButton = "reset_button"  # structural_influence, resets number of presses and informs button pressers that it was reset
	ResetTransmission = "reset_transmission"  # directed to specific users that pressed the button
	ThreePressesTransmission = "three_presses_transmission"  # broadcasts to all users that the button was pressed three times and then resets the button


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

	def is_directed_to_destination_uuid(self, *, destination_uuid: str) -> bool:
		return self.__client_uuid == destination_uuid

	def is_structural_influence(self) -> bool:
		return True


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
		read_polling_seconds=0.1,
		is_cancelled_polling_seconds=0.1,
		new_topic_partitions_total=1,
		new_topic_replication_factor=1,
		remove_topic_cluster_propagation_blocking_timeout_seconds=30,
		is_debug=True
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
		receive_from_server_polling_seconds=0.001
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
			client_read_failed_delay_seconds=0.1,
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
