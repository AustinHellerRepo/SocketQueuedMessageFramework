from __future__ import annotations
import sys
import os

sys.path.append(os.getcwd())

import time
from src.austin_heller_repo.socket_queued_message_framework import ServerMessengerFactory, ServerMessenger, ServerSocketFactory
from austin_heller_repo.common import HostPointer

try:
	from .chatroom import ChatRoomStructureFactory, ChatRoomSourceTypeEnum, ChatRoomClientServerMessage
except ImportError:
	from chatroom import ChatRoomStructureFactory, ChatRoomSourceTypeEnum, ChatRoomClientServerMessage


server_messenger = ServerMessenger(
	server_socket_factory_and_local_host_pointer_per_source_type={
		ChatRoomSourceTypeEnum.Person: (
			ServerSocketFactory(),
			HostPointer(
				host_address="localhost",
				host_port=30227
			)
		)
	},
	client_server_message_class=ChatRoomClientServerMessage,
	source_type_enum_class=ChatRoomSourceTypeEnum,
	server_messenger_source_type=ChatRoomSourceTypeEnum.ChatRoom,
	structure_factory=ChatRoomStructureFactory(),
	is_debug=False
)

server_messenger.start_receiving_from_clients()

try:
	is_running = True
	while is_running:
		time.sleep(10.0)
finally:
	try:
		server_messenger.stop_receiving_from_clients()
	finally:
		server_messenger.dispose()
