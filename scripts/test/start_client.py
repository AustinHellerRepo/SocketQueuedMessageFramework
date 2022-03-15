from __future__ import annotations
import sys
import os

sys.path.append(os.getcwd())

from austin_heller_repo.common import HostPointer
from src.austin_heller_repo.socket_queued_message_framework import ClientMessengerFactory, ClientMessenger, ClientSocketFactory

try:
	from .person import PersonStructure, ChatRoomClientServerMessage
except ImportError:
	from person import PersonStructure, ChatRoomClientServerMessage


print(f"Welcome to the chat client.")
host_address = input(f"Please enter the chat room address: ")

if host_address != "":
	host_port = 30227

	print(f"Type any message and hit Enter.")

	person_structure = PersonStructure(
		chat_room_client_messenger_factory=ClientMessengerFactory(
			client_socket_factory=ClientSocketFactory(),
			server_host_pointer=HostPointer(
				host_address=host_address,
				host_port=host_port
			),
			client_server_message_class=ChatRoomClientServerMessage,
			is_debug=False
		)
	)

	try:
		message = None
		while message is None:
			message = input(f"")
			if message != "":
				person_structure.send_message(
					message=message
				)
				message = None
	finally:
		person_structure.dispose()
