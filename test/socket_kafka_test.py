import unittest
from austin_heller_repo.socket import ServerSocketFactory, ClientSocketFactory, ServerSocket, ClientSocket
from austin_heller_repo.kafka_manager import KafkaManagerFactory, KafkaManager, KafkaAsyncWriter, KafkaReader, KafkaMessage, KafkaWrapper
from austin_heller_repo.common import HostPointer
from austin_heller_repo.threading import start_thread
import time
from typing import List, Tuple, Dict
from datetime import datetime
import matplotlib.pyplot as plt


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

		client_socket.close(
			is_forced=False
		)

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

		client_socket.close(
			is_forced=False
		)

		time.sleep(1)

		accepted_client_socket.close(
			is_forced=False
		)

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

