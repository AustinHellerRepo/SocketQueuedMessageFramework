import unittest
from austin_heller_repo.socket import ServerSocketFactory, ClientSocketFactory, ServerSocket, ClientSocket
from austin_heller_repo.kafka_manager import KafkaManagerFactory, KafkaManager, KafkaAsyncWriter, KafkaReader, KafkaMessage, KafkaWrapper
from austin_heller_repo.common import HostPointer
from austin_heller_repo.threading import start_thread
import time
from typing import List, Tuple, Dict
from datetime import datetime
import matplotlib.pyplot as plt
import uuid


def get_default_local_host_pointer() -> HostPointer:
	return HostPointer(
		host_address="0.0.0.0",
		host_port=32734
	)


def get_default_client_socket_factory() -> ClientSocketFactory:
	return ClientSocketFactory(
		to_server_packet_bytes_length=4096,
		server_read_failed_delay_seconds=0,
		is_ssl=False
	)


def get_default_server_socket_factory() -> ServerSocketFactory:
	return ServerSocketFactory(
		to_client_packet_bytes_length=4096,
		listening_limit_total=10,
		accept_timeout_seconds=1.0,
		client_read_failed_delay_seconds=0,
		is_ssl=False
	)


def get_default_kafka_manager_factory() -> KafkaManagerFactory:
	return KafkaManagerFactory(
		kafka_wrapper=KafkaWrapper(
			host_pointer=HostPointer(
				host_address="0.0.0.0",
				host_port=9092
			)
		),
		read_polling_seconds=0.01,
		is_cancelled_polling_seconds=0.01,
		new_topic_partitions_total=1,
		new_topic_replication_factor=1,
		remove_topic_cluster_propagation_blocking_timeout_seconds=30
	)


class SocketKafkaTest(unittest.TestCase):

	def test_connect_client_to_server(self):

		client_socket = get_default_client_socket_factory().get_client_socket()

		server_socket = get_default_server_socket_factory().get_server_socket()

		accepted_client_socket = None  # type: ClientSocket

		def on_accepted_client_method(client_socket: ClientSocket):
			nonlocal accepted_client_socket
			accepted_client_socket = client_socket

		server_socket.start_accepting_clients(
			host_ip_address=get_default_local_host_pointer().get_host_address(),
			host_port=get_default_local_host_pointer().get_host_port(),
			on_accepted_client_method=on_accepted_client_method
		)

		time.sleep(1)

		client_socket.connect_to_server(
			ip_address=get_default_local_host_pointer().get_host_address(),
			port=get_default_local_host_pointer().get_host_port()
		)

		time.sleep(1)

		client_socket.close()

		time.sleep(1)

		server_socket.stop_accepting_clients()

		time.sleep(1)

		server_socket.close()

		time.sleep(1)

	def test_send_from_client_to_server(self):

		expected_messages_total = 1000

		client_socket = get_default_client_socket_factory().get_client_socket()

		server_socket = get_default_server_socket_factory().get_server_socket()

		write_datetimes = []  # type: List[datetime]
		read_datetimes = []  # type: List[datetime]

		accepted_client_socket = None  # type: ClientSocket

		def on_accepted_client_method(client_socket: ClientSocket):
			nonlocal accepted_client_socket
			accepted_client_socket = client_socket

		server_socket.start_accepting_clients(
			host_ip_address=get_default_local_host_pointer().get_host_address(),
			host_port=get_default_local_host_pointer().get_host_port(),
			on_accepted_client_method=on_accepted_client_method
		)

		time.sleep(1)

		client_socket.connect_to_server(
			ip_address=get_default_local_host_pointer().get_host_address(),
			port=get_default_local_host_pointer().get_host_port()
		)

		time.sleep(1)

		def relay_messages_thread_method():
			nonlocal accepted_client_socket
			nonlocal expected_messages_total

			for index in range(expected_messages_total):
				message = accepted_client_socket.read()
				accepted_client_socket.write(message)

		def write_messages_thread_method():
			nonlocal client_socket
			nonlocal expected_messages_total
			nonlocal write_datetimes

			for index in range(expected_messages_total):
				write_datetimes.append(datetime.utcnow())
				client_socket.write(str(index))
				time.sleep(0.01)

		def read_messages_thread_method():
			nonlocal client_socket
			nonlocal expected_messages_total
			nonlocal read_datetimes

			for index in range(expected_messages_total):
				message = client_socket.read()
				self.assertEqual(str(index), message)
				read_datetimes.append(datetime.utcnow())

		relay_messages_thread = start_thread(relay_messages_thread_method)
		write_messages_thread = start_thread(write_messages_thread_method)
		read_messages_thread = start_thread(read_messages_thread_method)

		write_messages_thread.join()
		relay_messages_thread.join()
		read_messages_thread.join()

		client_socket.close()

		time.sleep(1)

		accepted_client_socket.close()

		time.sleep(1)

		server_socket.stop_accepting_clients()

		time.sleep(1)

		server_socket.close()

		time.sleep(1)

		diff_seconds_totals = []  # type: List[float]
		for write_datetime, read_datetime in zip(write_datetimes, read_datetimes):
			seconds_total = (read_datetime - write_datetime).total_seconds()
			diff_seconds_totals.append(seconds_total)

		print(f"Min diff seconds {min(diff_seconds_totals)} at {diff_seconds_totals.index(min(diff_seconds_totals))}")
		print(f"Max diff seconds {max(diff_seconds_totals)} at {diff_seconds_totals.index(max(diff_seconds_totals))}")
		print(f"Ave diff seconds {sum(diff_seconds_totals) / expected_messages_total}")

		plt.scatter(write_datetimes, range(len(write_datetimes)), s=1, c="red")
		plt.scatter(read_datetimes, range(len(read_datetimes)), s=1, c="blue")
		plt.show()

	def test_send_from_client_to_server_then_to_kafka_write_and_read(self):

		expected_messages_total = 1000

		client_socket = get_default_client_socket_factory().get_client_socket()

		server_socket = get_default_server_socket_factory().get_server_socket()

		kafka_topic_name = str(uuid.uuid4())

		kafka_manager = get_default_kafka_manager_factory().get_kafka_manager()

		kafka_manager.add_topic(
			topic_name=kafka_topic_name
		).get_result()

		kafka_writer = kafka_manager.get_async_writer().get_result()  # type: KafkaAsyncWriter

		kafka_reader = kafka_manager.get_reader(
			topic_name=kafka_topic_name,
			is_from_beginning=True
		).get_result()  # type: KafkaReader

		write_datetimes = []  # type: List[datetime]
		read_datetimes = []  # type: List[datetime]

		accepted_client_socket = None  # type: ClientSocket

		def on_accepted_client_method(client_socket: ClientSocket):
			nonlocal accepted_client_socket
			accepted_client_socket = client_socket

		server_socket.start_accepting_clients(
			host_ip_address=get_default_local_host_pointer().get_host_address(),
			host_port=get_default_local_host_pointer().get_host_port(),
			on_accepted_client_method=on_accepted_client_method
		)

		time.sleep(1)

		client_socket.connect_to_server(
			ip_address=get_default_local_host_pointer().get_host_address(),
			port=get_default_local_host_pointer().get_host_port()
		)

		time.sleep(1)

		def write_messages_to_kafka_thread_method():
			nonlocal accepted_client_socket
			nonlocal expected_messages_total
			nonlocal kafka_writer
			nonlocal kafka_topic_name

			for index in range(expected_messages_total):
				message = accepted_client_socket.read()
				kafka_writer.write_message(
					topic_name=kafka_topic_name,
					message_bytes=message.encode()
				).get_result()

		def write_messages_to_socket_thread_method():
			nonlocal client_socket
			nonlocal expected_messages_total
			nonlocal write_datetimes

			for index in range(expected_messages_total):
				write_datetimes.append(datetime.utcnow())
				client_socket.write(str(index))
				time.sleep(0.01)

		def read_messages_from_kafka_thread_method():
			nonlocal kafka_reader
			nonlocal accepted_client_socket
			nonlocal expected_messages_total

			for index in range(expected_messages_total):
				kafka_message = kafka_reader.read_message().get_result()  # type: KafkaMessage
				accepted_client_socket.write(kafka_message.get_message_bytes().decode())

		def read_messages_from_socket_thread_method():
			nonlocal client_socket
			nonlocal expected_messages_total
			nonlocal read_datetimes

			for index in range(expected_messages_total):
				message = client_socket.read()
				self.assertEqual(str(index), message)
				read_datetimes.append(datetime.utcnow())

		read_messages_from_socket_thread = start_thread(read_messages_from_socket_thread_method)
		read_messages_from_kafka_thread = start_thread(read_messages_from_kafka_thread_method)
		write_messages_to_kafka_thread = start_thread(write_messages_to_kafka_thread_method)
		write_messages_to_socket_thread = start_thread(write_messages_to_socket_thread_method)

		read_messages_from_socket_thread.join()
		read_messages_from_kafka_thread.join()
		write_messages_to_kafka_thread.join()
		write_messages_to_socket_thread.join()

		client_socket.close()

		time.sleep(1)

		accepted_client_socket.close()

		time.sleep(1)

		server_socket.stop_accepting_clients()

		time.sleep(1)

		server_socket.close()

		time.sleep(1)

		diff_seconds_totals = []  # type: List[float]
		for write_datetime, read_datetime in zip(write_datetimes, read_datetimes):
			seconds_total = (read_datetime - write_datetime).total_seconds()
			diff_seconds_totals.append(seconds_total)

		print(f"Min diff seconds {min(diff_seconds_totals)} at {diff_seconds_totals.index(min(diff_seconds_totals))}")
		print(f"Max diff seconds {max(diff_seconds_totals)} at {diff_seconds_totals.index(max(diff_seconds_totals))}")
		print(f"Ave diff seconds {sum(diff_seconds_totals) / expected_messages_total}")

		plt.scatter(write_datetimes, range(len(write_datetimes)), s=1, c="red")
		plt.scatter(read_datetimes, range(len(read_datetimes)), s=1, c="blue")
		plt.show()

