# TODO install git@github.com:AustinHellerRepo/CertificateManager
import unittest
from src.austin_heller_repo.socket_queued_message_framework import ClientMessenger, ServerMessenger
from test.messenger_test import BaseClientServerMessage, ButtonStructureFactory, EchoResponseBaseClientServerMessage, HelloWorldBaseClientServerMessage, EchoRequestBaseClientServerMessage
from test.messenger_efficiency_test import show_plot
from austin_heller_repo.certificate_manager import CertificateManagerClient, Certificate
from austin_heller_repo.socket import ClientSocketFactory, ServerSocketFactory
from austin_heller_repo.common import HostPointer
from austin_heller_repo.threading import SingletonMemorySequentialQueueFactory, Semaphore, start_thread
import tempfile
from typing import List, Tuple, Dict, Callable, Type
import os
import time
from datetime import datetime


is_socket_debug_active = False
is_client_messenger_debug_active = False
is_server_messenger_debug_active = False


def get_default_certificate_manager_client() -> CertificateManagerClient:
	return CertificateManagerClient(
		client_socket_factory=ClientSocketFactory(
			to_server_packet_bytes_length=4096
		),
		server_host_pointer=HostPointer(
			host_address="172.17.0.1",
			host_port=35123
		)
	)


certificate_manager_client = get_default_certificate_manager_client()

client_certificate = certificate_manager_client.request_certificate(
	name="client"
)

client_private_key_tempfile = tempfile.NamedTemporaryFile(
	delete=False
)

client_public_certificate_tempfile = tempfile.NamedTemporaryFile(
	delete=False
)

client_certificate.save_to_file(
	private_key_file_path=client_private_key_tempfile.name,
	signed_certificate_file_path=client_public_certificate_tempfile.name
)

server_certificate = certificate_manager_client.request_certificate(
	name="localhost"
)

server_private_key_tempfile = tempfile.NamedTemporaryFile(
	delete=False
)

server_public_certificate_tempfile = tempfile.NamedTemporaryFile(
	delete=False
)

server_certificate.save_to_file(
	private_key_file_path=server_private_key_tempfile.name,
	signed_certificate_file_path=server_public_certificate_tempfile.name
)

root_ssl_certificate_tempfile = tempfile.NamedTemporaryFile(
	delete=False
)

certificate_manager_client.get_root_certificate(
	save_to_file_path=root_ssl_certificate_tempfile.name
)


def get_default_local_host_pointer() -> HostPointer:
	return HostPointer(
		host_address="localhost",
		host_port=36429
	)


def get_default_client_messenger() -> ClientMessenger:
	return ClientMessenger(
		client_socket_factory=ClientSocketFactory(
			to_server_packet_bytes_length=4096,
			ssl_private_key_file_path=client_private_key_tempfile.name,
			ssl_certificate_file_path=client_public_certificate_tempfile.name,
			root_ssl_certificate_file_path=root_ssl_certificate_tempfile.name,
			is_debug=is_socket_debug_active
		),
		server_host_pointer=get_default_local_host_pointer(),
		client_server_message_class=BaseClientServerMessage,
		is_debug=is_client_messenger_debug_active
	)


def get_default_server_messenger() -> ServerMessenger:

	sequential_queue_factory = SingletonMemorySequentialQueueFactory()

	return ServerMessenger(
		server_socket_factory=ServerSocketFactory(
			to_client_packet_bytes_length=4096,
			listening_limit_total=10,
			accept_timeout_seconds=10.0,
			ssl_private_key_file_path=server_private_key_tempfile.name,
			ssl_certificate_file_path=server_public_certificate_tempfile.name,
			root_ssl_certificate_file_path=root_ssl_certificate_tempfile.name,
			is_debug=is_socket_debug_active
		),
		sequential_queue_factory=sequential_queue_factory,
		local_host_pointer=get_default_local_host_pointer(),
		client_server_message_class=BaseClientServerMessage,
		structure_factory=ButtonStructureFactory(),
		is_debug=is_server_messenger_debug_active
	)


class SslSocketTest(unittest.TestCase):

	@classmethod
	def tearDownClass(cls) -> None:

		os.unlink(client_private_key_tempfile.name)
		os.unlink(client_public_certificate_tempfile.name)
		os.unlink(server_private_key_tempfile.name)
		os.unlink(server_public_certificate_tempfile.name)
		os.unlink(root_ssl_certificate_tempfile.name)

	def test_many_clients_sending_many_messages_delayed_threaded_64c_100m_md200ms_cb0ms_0h1e(self):

		clients_total = 64
		messages_per_client_total = 100
		message_delay_seconds = 0.200
		client_begin_sending_messages_delay_seconds = 0
		hellos_total = 0
		echoes_total = 1

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		message_send_start_time_per_message_uuid = {}  # type: Dict[str, datetime]
		message_send_end_time_per_message_uuid = {}  # type: Dict[str, datetime]
		message_receive_start_time_per_message_uuid = {}  # type: Dict[str, datetime]
		message_receive_end_time_per_message_uuid = {}  # type: Dict[str, datetime]

		callback_total = 0
		callback_total_semaphore = Semaphore()

		def callback(echo_response: EchoResponseBaseClientServerMessage):
			nonlocal callback_total
			nonlocal callback_total_semaphore
			message_uuid = echo_response.get_message()
			message_receive_start_time_per_message_uuid[message_uuid] = datetime.utcnow()
			callback_total_semaphore.acquire()
			callback_total += 1
			callback_total_semaphore.release()
			self.assertIsInstance(echo_response, EchoResponseBaseClientServerMessage)
			message_receive_end_time_per_message_uuid[message_uuid] = datetime.utcnow()

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messengers = []
		for index in range(clients_total):
			client_messenger = get_default_client_messenger()
			client_messenger.connect_to_server()

			time.sleep(0.1)

			client_messenger.receive_from_server(
				callback=callback,
				on_exception=on_exception
			)

			client_messengers.append(client_messenger)

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: sending messages")

		global_message_index = 0
		global_message_index_semaphore = Semaphore()

		def get_global_message_index() -> int:
			nonlocal global_message_index
			nonlocal global_message_index_semaphore

			global_message_index_semaphore.acquire()
			available_global_message_index = global_message_index
			global_message_index += 1
			global_message_index_semaphore.release()
			return available_global_message_index

		def send_message_thread_method(client_messenger: ClientMessenger):
			nonlocal message_delay_seconds
			hello_index = 0
			echo_index = 0
			for message_index in range(messages_per_client_total):
				message = str(get_global_message_index())
				message_send_start_time_per_message_uuid[message] = datetime.utcnow()
				if hello_index < hellos_total:
					client_messenger.send_to_server(
						request_client_server_message=HelloWorldBaseClientServerMessage()
					)
					hello_index += 1
					if hello_index == hellos_total and echo_index == echoes_total:
						hello_index = 0
						echo_index = 0
				elif echo_index < echoes_total:
					client_messenger.send_to_server(
						request_client_server_message=EchoRequestBaseClientServerMessage(
							message=message,
							is_ordered=True
						)
					)
					echo_index += 1
					if hello_index == hellos_total and echo_index == echoes_total:
						hello_index = 0
						echo_index = 0
				else:
					raise Exception(f"Unexpected hello to echo ratio")
				time.sleep(message_delay_seconds)
				message_send_end_time_per_message_uuid[message] = datetime.utcnow()

		send_message_threads = []
		for client_messenger in client_messengers:
			send_message_threads.append(start_thread(send_message_thread_method, client_messenger))
			time.sleep(client_begin_sending_messages_delay_seconds)

		print(f"{datetime.utcnow()}: waiting for messages")

		for send_message_thread in send_message_threads:
			send_message_thread.join()

		time.sleep(12)

		print(f"{datetime.utcnow()}: dispose client_messengers: start")

		for client_messenger in client_messengers:
			client_messenger.dispose()

		print(f"{datetime.utcnow()}: dispose client_messengers: end")

		time.sleep(1)

		print(f"{datetime.utcnow()}: stopping server")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped server")

		time.sleep(5)

		self.assertEqual(clients_total * messages_per_client_total * (echoes_total / (echoes_total + hellos_total)), callback_total)

		if found_exception is not None:
			raise found_exception

		show_plot(
			clients_total=clients_total,
			message_send_start_time_per_message_uuid=message_send_start_time_per_message_uuid,
			message_send_end_time_per_message_uuid=message_send_end_time_per_message_uuid,
			message_receive_start_time_per_message_uuid=message_receive_start_time_per_message_uuid,
			message_receive_end_time_per_message_uuid=message_receive_end_time_per_message_uuid
		)

	def test_many_clients_sending_many_messages_delayed_threaded_16c_100m_md50ms_cb0ms(self):

		clients_total = 16
		messages_per_client_total = 100
		message_delay_seconds = 0.050
		client_begin_sending_messages_delay_seconds = 0

		server_messenger = get_default_server_messenger()

		server_messenger.start_receiving_from_clients()

		time.sleep(1)

		message_send_start_time_per_message_uuid = {}  # type: Dict[str, datetime]
		message_send_end_time_per_message_uuid = {}  # type: Dict[str, datetime]
		message_receive_start_time_per_message_uuid = {}  # type: Dict[str, datetime]
		message_receive_end_time_per_message_uuid = {}  # type: Dict[str, datetime]

		callback_total = 0
		callback_total_semaphore = Semaphore()

		def callback(echo_response: EchoResponseBaseClientServerMessage):
			nonlocal callback_total
			nonlocal callback_total_semaphore
			message_uuid = echo_response.get_message()
			message_receive_start_time_per_message_uuid[message_uuid] = datetime.utcnow()
			callback_total_semaphore.acquire()
			callback_total += 1
			callback_total_semaphore.release()
			self.assertIsInstance(echo_response, EchoResponseBaseClientServerMessage)
			message_receive_end_time_per_message_uuid[message_uuid] = datetime.utcnow()

		found_exception = None  # type: Exception

		def on_exception(exception: Exception):
			nonlocal found_exception
			found_exception = exception

		client_messengers = []
		for index in range(clients_total):
			client_messenger = get_default_client_messenger()
			client_messenger.connect_to_server()

			time.sleep(0.1)

			client_messenger.receive_from_server(
				callback=callback,
				on_exception=on_exception
			)

			client_messengers.append(client_messenger)

		time.sleep(0.1)

		print(f"{datetime.utcnow()}: sending messages")

		global_message_index = 0
		global_message_index_semaphore = Semaphore()

		def get_global_message_index() -> int:
			nonlocal global_message_index
			nonlocal global_message_index_semaphore

			global_message_index_semaphore.acquire()
			available_global_message_index = global_message_index
			global_message_index += 1
			global_message_index_semaphore.release()
			return available_global_message_index

		def send_message_thread_method(client_messenger: ClientMessenger):
			nonlocal message_delay_seconds
			for message_index in range(messages_per_client_total):
				message = str(get_global_message_index())
				message_send_start_time_per_message_uuid[message] = datetime.utcnow()
				client_messenger.send_to_server(
					request_client_server_message=EchoRequestBaseClientServerMessage(
						message=message,
						is_ordered=True
					)
				)
				time.sleep(message_delay_seconds)
				message_send_end_time_per_message_uuid[message] = datetime.utcnow()

		send_message_threads = []
		for client_messenger in client_messengers:
			send_message_threads.append(start_thread(send_message_thread_method, client_messenger))
			time.sleep(client_begin_sending_messages_delay_seconds)

		print(f"{datetime.utcnow()}: waiting for messages")

		for send_message_thread in send_message_threads:
			send_message_thread.join()

		time.sleep(12)

		print(f"{datetime.utcnow()}: dispose client_messengers: start")

		for client_messenger in client_messengers:
			client_messenger.dispose()

		print(f"{datetime.utcnow()}: dispose client_messengers: end")

		time.sleep(1)

		print(f"{datetime.utcnow()}: stopping server")

		server_messenger.stop_receiving_from_clients()

		print(f"{datetime.utcnow()}: stopped server")

		time.sleep(5)

		self.assertEqual(clients_total * messages_per_client_total, callback_total)

		if found_exception is not None:
			raise found_exception

		show_plot(
			clients_total=clients_total,
			message_send_start_time_per_message_uuid=message_send_start_time_per_message_uuid,
			message_send_end_time_per_message_uuid=message_send_end_time_per_message_uuid,
			message_receive_start_time_per_message_uuid=message_receive_start_time_per_message_uuid,
			message_receive_end_time_per_message_uuid=message_receive_end_time_per_message_uuid
		)