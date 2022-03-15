from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Tuple, Dict, Type
import json
from src.austin_heller_repo.socket_queued_message_framework import ClientServerMessageTypeEnum, ClientServerMessage, SourceTypeEnum, StructureInfluence, Structure, StructureStateEnum, StructureTransitionException, StructureFactory
from austin_heller_repo.threading import Semaphore


class ChatRoomSourceTypeEnum(SourceTypeEnum):
	ChatRoom = "chat_room"
	Person = "person"


class ChatRoomStructureStateEnum(StructureStateEnum):
	Active = "active"


class PersonStructureStateEnum(StructureStateEnum):
	Active = "active"


class ChatRoomClientServerMessageTypeEnum(ClientServerMessageTypeEnum):
	ChatRoomError = "chat_room_error"
	PersonMessageAnnouncement = "person_message_announcement"
	PersonMessageBroadcast = "person_message_broadcast"


class ChatRoomClientServerMessage(ClientServerMessage, ABC):

	def __init__(self, *, destination_uuid: str):
		super().__init__(
			destination_uuid=destination_uuid
		)

		pass

	@classmethod
	def get_client_server_message_type_class(cls) -> Type[ClientServerMessageTypeEnum]:
		return ChatRoomClientServerMessageTypeEnum


###############################################################################
# Chat Room
###############################################################################

class ChatRoomErrorChatRoomClientServerMessage(ChatRoomClientServerMessage):

	def __init__(self, *, structure_state_name: str, client_server_message_json_string: str, destination_uuid: str):
		super().__init__(
			destination_uuid=destination_uuid
		)

		self.__structure_state_name = structure_state_name
		self.__client_server_message_json_string = client_server_message_json_string

	def get_structure_state(self) -> ChatRoomStructureStateEnum:
		return ChatRoomStructureStateEnum(self.__structure_state_name)

	def get_client_server_message(self) -> ChatRoomClientServerMessage:
		return ChatRoomClientServerMessage.parse_from_json(
			json_object=json.loads(self.__client_server_message_json_string)
		)

	@classmethod
	def get_client_server_message_type(cls) -> ClientServerMessageTypeEnum:
		return ChatRoomClientServerMessageTypeEnum.ChatRoomError

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["structure_state_name"] = self.__structure_state_name
		json_object["client_server_message_json_string"] = self.__client_server_message_json_string
		return json_object

	def get_structural_error_client_server_message_response(self, *, structure_transition_exception: StructureTransitionException, destination_uuid: str) -> ClientServerMessage:
		return None


###############################################################################
# Person
###############################################################################

class PersonMessageAnnouncementChatRoomClientServerMessage(ChatRoomClientServerMessage):

	def __init__(self, *, message: str, destination_uuid: str):
		super().__init__(
			destination_uuid=destination_uuid
		)

		self.__message = message

	def get_message(self) -> str:
		return self.__message

	@classmethod
	def get_client_server_message_type(cls) -> ClientServerMessageTypeEnum:
		return ChatRoomClientServerMessageTypeEnum.PersonMessageAnnouncement

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["message"] = self.__message
		return json_object

	def get_structural_error_client_server_message_response(self, *, structure_transition_exception: StructureTransitionException, destination_uuid: str) -> ClientServerMessage:
		return ChatRoomErrorChatRoomClientServerMessage(
			structure_state_name=structure_transition_exception.get_structure_state().value,
			client_server_message_json_string=json.dumps(structure_transition_exception.get_structure_influence().get_client_server_message().to_json()),
			destination_uuid=destination_uuid
		)


class PersonMessageBroadcastChatRoomClientServerMessage(ChatRoomClientServerMessage):

	def __init__(self, *, message: str, destination_uuid: str):
		super().__init__(
			destination_uuid=destination_uuid
		)

		self.__message = message

	def get_message(self) -> str:
		return self.__message

	@classmethod
	def get_client_server_message_type(cls) -> ClientServerMessageTypeEnum:
		return ChatRoomClientServerMessageTypeEnum.PersonMessageBroadcast

	def to_json(self) -> Dict:
		json_object = super().to_json()
		json_object["message"] = self.__message
		return json_object

	def get_structural_error_client_server_message_response(self, *, structure_transition_exception: StructureTransitionException, destination_uuid: str) -> ClientServerMessage:
		return ChatRoomErrorChatRoomClientServerMessage(
			structure_state_name=structure_transition_exception.get_structure_state().value,
			client_server_message_json_string=json.dumps(structure_transition_exception.get_structure_influence().get_client_server_message().to_json()),
			destination_uuid=destination_uuid
		)


###############################################################################
# Structures
###############################################################################

class PersonStructure(Structure):

	def __init__(self, *, source_uuid: str):
		super().__init__(
			states=PersonStructureStateEnum,
			initial_state=PersonStructureStateEnum.Active
		)

		self.__source_uuid = source_uuid

	def on_client_connected(self, *, source_uuid: str, source_type: SourceTypeEnum, tag_json: Dict or None):
		raise Exception(f"Unexpected connection from source {source_type.value}")

	def send_message(self, *, message: str):
		self.send_client_server_message(
			client_server_message=PersonMessageBroadcastChatRoomClientServerMessage(
				message=message,
				destination_uuid=self.__source_uuid
			)
		)


class ChatRoomStructure(Structure):

	def __init__(self):
		super().__init__(
			states=ChatRoomStructureStateEnum,
			initial_state=ChatRoomStructureStateEnum.Active
		)

		self.__person_structure_per_source_uuid = {}  # type: Dict[str, PersonStructure]
		self.__person_structure_per_source_uuid_semaphore = Semaphore()

		self.add_transition(
			client_server_message_type=ChatRoomClientServerMessageTypeEnum.PersonMessageAnnouncement,
			from_source_type=ChatRoomSourceTypeEnum.Person,
			start_structure_state=ChatRoomStructureStateEnum.Active,
			end_structure_state=ChatRoomStructureStateEnum.Active,
			on_transition=self.__person_message_announcement_transition
		)

	def on_client_connected(self, *, source_uuid: str, source_type: SourceTypeEnum, tag_json: Dict or None):
		if source_type == ChatRoomSourceTypeEnum.Person:
			person_structure = PersonStructure(
				source_uuid=source_uuid
			)
			self.register_child_structure(
				structure=person_structure
			)
			self.__person_structure_per_source_uuid_semaphore.acquire()
			try:
				self.__person_structure_per_source_uuid[source_uuid] = person_structure
			finally:
				self.__person_structure_per_source_uuid_semaphore.release()
		else:
			raise Exception(f"Unexpected connection from source type: {source_type.value}")

	def __person_message_announcement_transition(self, structure_influence: StructureInfluence):
		client_server_message = structure_influence.get_client_server_message()
		if not isinstance(client_server_message, PersonMessageAnnouncementChatRoomClientServerMessage):
			raise Exception(f"Unexpected client server message type: {type(client_server_message)}")
		else:
			message = client_server_message.get_message()
			if len(message) > 80:
				message = message[:80]
			print(f"{structure_influence.get_source_uuid()}: {message}")
			self.__person_structure_per_source_uuid_semaphore.acquire()
			try:
				for source_uuid, person_structure in self.__person_structure_per_source_uuid.items():
					if source_uuid != structure_influence.get_source_uuid():
						person_structure.send_message(
							message=message
						)
			finally:
				self.__person_structure_per_source_uuid_semaphore.release()


class ChatRoomStructureFactory(StructureFactory):

	def get_structure(self) -> Structure:
		return ChatRoomStructure()
