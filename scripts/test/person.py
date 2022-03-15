from __future__ import annotations
from datetime import datetime
from typing import List, Tuple, Dict, Type, Callable
from src.austin_heller_repo.socket_queued_message_framework import Structure, StructureStateEnum, SourceTypeEnum, ClientMessengerFactory, ClientMessenger, StructureFactory, StructureInfluence, ClientServerMessage

try:
	from .chatroom import PersonMessageAnnouncementChatRoomClientServerMessage, ChatRoomClientServerMessage, ChatRoomClientServerMessageTypeEnum, PersonMessageBroadcastChatRoomClientServerMessage
except ImportError:
	from chatroom import PersonMessageAnnouncementChatRoomClientServerMessage, ChatRoomClientServerMessage, ChatRoomClientServerMessageTypeEnum, PersonMessageBroadcastChatRoomClientServerMessage


class PersonStructureStateEnum(StructureStateEnum):
	Active = "active"


class ChatRoomStructureStateEnum(StructureStateEnum):
	Active = "active"


class PersonSourceTypeEnum(SourceTypeEnum):
	Person = "person"
	ChatRoom = "chat_room"


class ChatRoomStructure(Structure):

	def __init__(self, *, source_uuid: str):
		super().__init__(
			states=ChatRoomStructureStateEnum,
			initial_state=ChatRoomStructureStateEnum.Active
		)

		self.__source_uuid = source_uuid

	def on_client_connected(self, *, source_uuid: str, source_type: SourceTypeEnum, tag_json: Dict or None):
		raise Exception(f"Unexpected connection from source {source_type.value}")

	def send_message(self, *, message: str):
		self.send_client_server_message(
			client_server_message=PersonMessageAnnouncementChatRoomClientServerMessage(
				message=message,
				destination_uuid=self.__source_uuid
			)
		)


class PersonStructure(Structure):

	def __init__(self, *, chat_room_client_messenger_factory: ClientMessengerFactory):
		super().__init__(
			states=PersonStructureStateEnum,
			initial_state=PersonStructureStateEnum.Active
		)

		self.__chat_room_client_messenger_factory = chat_room_client_messenger_factory

		self.__chat_room_structure = None  # type: ChatRoomStructure

		self.add_transition(
			client_server_message_type=ChatRoomClientServerMessageTypeEnum.PersonMessageBroadcast,
			from_source_type=PersonSourceTypeEnum.ChatRoom,
			start_structure_state=PersonStructureStateEnum.Active,
			end_structure_state=PersonStructureStateEnum.Active,
			on_transition=self.__person_message_broadcast_transition
		)

		self.__initialize()

	def __initialize(self):

		self.connect_to_outbound_messenger(
			client_messenger_factory=self.__chat_room_client_messenger_factory,
			source_type=PersonSourceTypeEnum.ChatRoom,
			tag_json=None
		)

	def __person_message_broadcast_transition(self, structure_influence: StructureInfluence):
		client_server_message = structure_influence.get_client_server_message()
		if not isinstance(client_server_message, PersonMessageBroadcastChatRoomClientServerMessage):
			raise Exception(f"Unexpected message type: {type(client_server_message)}")
		else:
			message = client_server_message.get_message()
			print(message)

	def on_client_connected(self, *, source_uuid: str, source_type: SourceTypeEnum, tag_json: Dict or None):
		if source_type == PersonSourceTypeEnum.ChatRoom:
			self.__chat_room_structure = ChatRoomStructure(
				source_uuid=source_uuid
			)
			self.register_child_structure(
				structure=self.__chat_room_structure
			)
		else:
			raise Exception(f"Unexpected connection from source type: {source_type.value}")

	def send_message(self, *, message: str):
		self.__chat_room_structure.send_message(
			message=message
		)


class PersonStructureFactory(StructureFactory):

	def __init__(self, *, chat_room_client_messenger_factory: ClientMessengerFactory):

		self.__chat_room_client_messenger_factory = chat_room_client_messenger_factory

	def get_structure(self) -> Structure:
		return PersonStructure(
			chat_room_client_messenger_factory=self.__chat_room_client_messenger_factory
		)
