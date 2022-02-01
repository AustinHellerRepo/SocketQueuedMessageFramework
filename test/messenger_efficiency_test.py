import unittest
from datetime import datetime
import time
from typing import List, Tuple, Dict, Callable, Type
from test.messenger_test import get_default_local_host_pointer, get_default_client_messenger_factory, get_default_server_messenger_factory, HelloWorldBaseClientServerMessage, EchoRequestBaseClientServerMessage, EchoResponseBaseClientServerMessage, AnnounceBaseClientServerMessage, HelloWorldBaseClientServerMessage
from austin_heller_repo.threading import AsyncHandle, start_thread, Semaphore
from src.austin_heller_repo.socket_queued_message_framework import ClientMessenger, ClientServerMessage
import matplotlib.pyplot as plt


is_plotted = True


def show_plot(*, clients_total: int, message_send_start_time_per_message_uuid: Dict[str, datetime], message_send_end_time_per_message_uuid: Dict[str, datetime], message_receive_start_time_per_message_uuid: Dict[str, datetime], message_receive_end_time_per_message_uuid: Dict[str, datetime]):
	send_diff_seconds_per_message_uuid = {}  # type: Dict[str, float]
	receive_diff_seconds_per_message_uuid = {}  # type: Dict[str, float]
	total_diff_seconds_per_message_uuid = {}  # type: Dict[str, float]
	first_send_start_time = None
	last_send_start_time = None
	first_receive_end_time = None
	last_receive_end_time = None
	messages_total = len(message_send_start_time_per_message_uuid.keys())
	for message_uuid in message_send_start_time_per_message_uuid.keys():
		if message_uuid in message_receive_end_time_per_message_uuid:
			send_diff_seconds_per_message_uuid[message_uuid] = (message_send_end_time_per_message_uuid[message_uuid] - message_send_start_time_per_message_uuid[message_uuid]).total_seconds()
			receive_diff_seconds_per_message_uuid[message_uuid] = (message_receive_end_time_per_message_uuid[message_uuid] - message_receive_start_time_per_message_uuid[message_uuid]).total_seconds()
			total_diff_seconds_per_message_uuid[message_uuid] = (message_receive_end_time_per_message_uuid[message_uuid] - message_send_start_time_per_message_uuid[message_uuid]).total_seconds()
			if first_send_start_time is None or message_send_start_time_per_message_uuid[message_uuid] < first_send_start_time:
				first_send_start_time = message_send_start_time_per_message_uuid[message_uuid]
			if last_send_start_time is None or message_send_start_time_per_message_uuid[message_uuid] > last_send_start_time:
				last_send_start_time = message_send_start_time_per_message_uuid[message_uuid]
			if first_receive_end_time is None or message_receive_end_time_per_message_uuid[message_uuid] < first_receive_end_time:
				first_receive_end_time = message_receive_end_time_per_message_uuid[message_uuid]
			if last_receive_end_time is None or message_receive_end_time_per_message_uuid[message_uuid] > last_receive_end_time:
				last_receive_end_time = message_receive_end_time_per_message_uuid[message_uuid]

	average_send_diff_seconds = 0
	average_receive_diff_seconds = 0
	average_total_diff_seconds = 0
	for message_uuid in send_diff_seconds_per_message_uuid.keys():
		average_send_diff_seconds += send_diff_seconds_per_message_uuid[message_uuid]
		average_receive_diff_seconds += receive_diff_seconds_per_message_uuid[message_uuid]
		average_total_diff_seconds += total_diff_seconds_per_message_uuid[message_uuid]
	average_send_diff_seconds /= messages_total
	average_receive_diff_seconds /= messages_total
	average_total_diff_seconds /= messages_total

	total_diff_seconds_per_global_message_index = []
	for index in range(messages_total):
		index_string = str(index)
		if index_string in total_diff_seconds_per_message_uuid:
			total_diff_seconds_per_global_message_index.append(total_diff_seconds_per_message_uuid[index_string])

	print(f"{datetime.utcnow()}: first_send_start_time: {first_send_start_time}")
	print(f"{datetime.utcnow()}: last_send_start_time: {last_send_start_time}")
	print(f"{datetime.utcnow()}: first_receive_end_time: {first_receive_end_time}")
	print(f"{datetime.utcnow()}: last_receive_end_time: {last_receive_end_time}")
	print(f"{datetime.utcnow()}: total time: {(last_receive_end_time - first_send_start_time).total_seconds()}")
	print(f"{datetime.utcnow()}: seconds per message per client: {(last_receive_end_time - first_send_start_time).total_seconds() / (messages_total / clients_total)}")
	print(f"{datetime.utcnow()}: messages per second per client: {1.0 / ((last_receive_end_time - first_send_start_time).total_seconds() / (messages_total / clients_total))}")
	print(f"{datetime.utcnow()}: average_send_diff_seconds: {average_send_diff_seconds}")
	print(f"{datetime.utcnow()}: average_receive_diff_seconds: {average_receive_diff_seconds}")
	print(f"{datetime.utcnow()}: average_total_diff_seconds: {average_total_diff_seconds}")

	if is_plotted:
		plt.scatter(range(len(total_diff_seconds_per_global_message_index)), total_diff_seconds_per_global_message_index, s=1, c="red")
		plt.show()


class MessengerEfficiencyTest(unittest.TestCase):

	def test_many_clients_sending_many_messages_burst_10c_100m(self):

		clients_total = 10
		messages_per_client_total = 100

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
		for message_index in range(messages_per_client_total):
			for client_messenger in client_messengers:
				message = str(global_message_index)
				message_send_start_time_per_message_uuid[message] = datetime.utcnow()
				client_messenger.send_to_server(
					client_server_message=EchoRequestBaseClientServerMessage(
						message=message,
						is_ordered=True
					)
				)
				global_message_index += 1
				message_send_end_time_per_message_uuid[message] = datetime.utcnow()

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

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

	def test_many_clients_sending_many_messages_burst_100c_100m(self):

		clients_total = 100
		messages_per_client_total = 100

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
		for message_index in range(messages_per_client_total):
			for client_messenger in client_messengers:
				message = str(global_message_index)
				message_send_start_time_per_message_uuid[message] = datetime.utcnow()
				client_messenger.send_to_server(
					client_server_message=EchoRequestBaseClientServerMessage(
						message=message,
						is_ordered=True
					)
				)
				global_message_index += 1
				message_send_end_time_per_message_uuid[message] = datetime.utcnow()

		print(f"{datetime.utcnow()}: waiting for messages")

		time.sleep(5)

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

	def test_many_clients_sending_many_messages_burst_threaded_100c_100m(self):

		clients_total = 100
		messages_per_client_total = 100

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
			for message_index in range(messages_per_client_total):
				message = str(get_global_message_index())
				message_send_start_time_per_message_uuid[message] = datetime.utcnow()
				client_messenger.send_to_server(
					client_server_message=EchoRequestBaseClientServerMessage(
						message=message,
						is_ordered=True
					)
				)
				message_send_end_time_per_message_uuid[message] = datetime.utcnow()

		send_message_threads = []
		for client_messenger in client_messengers:
			send_message_threads.append(start_thread(send_message_thread_method, client_messenger))

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

	def test_many_clients_sending_many_messages_burst_threaded_delayed_100c_100m_10ms(self):

		clients_total = 100
		messages_per_client_total = 100

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
			for message_index in range(messages_per_client_total):
				message = str(get_global_message_index())
				message_send_start_time_per_message_uuid[message] = datetime.utcnow()
				time.sleep(0.1)
				client_messenger.send_to_server(
					client_server_message=EchoRequestBaseClientServerMessage(
						message=message,
						is_ordered=True
					)
				)
				message_send_end_time_per_message_uuid[message] = datetime.utcnow()

		send_message_threads = []
		for client_messenger in client_messengers:
			send_message_threads.append(start_thread(send_message_thread_method, client_messenger))

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

	def test_many_clients_sending_many_messages_delayed_threaded_10c_100m_100ms(self):

		clients_total = 10
		messages_per_client_total = 100

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
			for message_index in range(messages_per_client_total):
				message = str(get_global_message_index())
				message_send_start_time_per_message_uuid[message] = datetime.utcnow()
				client_messenger.send_to_server(
					client_server_message=EchoRequestBaseClientServerMessage(
						message=message,
						is_ordered=True
					)
				)
				time.sleep(0.1)
				message_send_end_time_per_message_uuid[message] = datetime.utcnow()

		send_message_threads = []
		for client_messenger in client_messengers:
			send_message_threads.append(start_thread(send_message_thread_method, client_messenger))
			#time.sleep(1)

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

	def test_many_clients_sending_many_messages_delayed_threaded_10c_100m_75ms(self):

		clients_total = 10
		messages_per_client_total = 100

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
			for message_index in range(messages_per_client_total):
				message = str(get_global_message_index())
				message_send_start_time_per_message_uuid[message] = datetime.utcnow()
				client_messenger.send_to_server(
					client_server_message=EchoRequestBaseClientServerMessage(
						message=message,
						is_ordered=True
					)
				)
				time.sleep(0.075)
				message_send_end_time_per_message_uuid[message] = datetime.utcnow()

		send_message_threads = []
		for client_messenger in client_messengers:
			send_message_threads.append(start_thread(send_message_thread_method, client_messenger))
			#time.sleep(1)

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

	def test_many_clients_sending_many_messages_delayed_threaded_10c_100m_50ms(self):

		clients_total = 10
		messages_per_client_total = 100

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
			for message_index in range(messages_per_client_total):
				message = str(get_global_message_index())
				message_send_start_time_per_message_uuid[message] = datetime.utcnow()
				client_messenger.send_to_server(
					client_server_message=EchoRequestBaseClientServerMessage(
						message=message,
						is_ordered=True
					)
				)
				time.sleep(0.05)
				message_send_end_time_per_message_uuid[message] = datetime.utcnow()

		send_message_threads = []
		for client_messenger in client_messengers:
			send_message_threads.append(start_thread(send_message_thread_method, client_messenger))
			#time.sleep(1)

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

	def test_many_clients_sending_many_messages_delayed_threaded_20c_100m_50ms(self):

		clients_total = 20
		messages_per_client_total = 100

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
			for message_index in range(messages_per_client_total):
				message = str(get_global_message_index())
				message_send_start_time_per_message_uuid[message] = datetime.utcnow()
				client_messenger.send_to_server(
					client_server_message=EchoRequestBaseClientServerMessage(
						message=message,
						is_ordered=True
					)
				)
				time.sleep(0.05)
				message_send_end_time_per_message_uuid[message] = datetime.utcnow()

		send_message_threads = []
		for client_messenger in client_messengers:
			send_message_threads.append(start_thread(send_message_thread_method, client_messenger))
			#time.sleep(1)

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

	def test_many_clients_sending_many_messages_delayed_threaded_64c_100m_md100ms_cb100ms(self):

		clients_total = 64
		messages_per_client_total = 100
		message_delay_seconds = 0.100
		client_begin_sending_messages_delay_seconds = 0.1

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
					client_server_message=EchoRequestBaseClientServerMessage(
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

	def test_many_clients_sending_many_messages_delayed_threaded_16c_100m_md50ms_cb0ms_9h1e(self):

		clients_total = 16
		messages_per_client_total = 100
		message_delay_seconds = 0.050
		client_begin_sending_messages_delay_seconds = 0
		hellos_total = 9
		echoes_total = 1

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
						client_server_message=HelloWorldBaseClientServerMessage()
					)
					hello_index += 1
					if hello_index == hellos_total and echo_index == echoes_total:
						hello_index = 0
						echo_index = 0
				elif echo_index < echoes_total:
					client_messenger.send_to_server(
						client_server_message=EchoRequestBaseClientServerMessage(
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

	def test_many_clients_sending_many_messages_delayed_threaded_64c_100m_md50ms_cb0ms_49h1e(self):

		clients_total = 64
		messages_per_client_total = 100
		message_delay_seconds = 0.050
		client_begin_sending_messages_delay_seconds = 0
		hellos_total = 49
		echoes_total = 1

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
						client_server_message=HelloWorldBaseClientServerMessage()
					)
					hello_index += 1
					if hello_index == hellos_total and echo_index == echoes_total:
						hello_index = 0
						echo_index = 0
				elif echo_index < echoes_total:
					client_messenger.send_to_server(
						client_server_message=EchoRequestBaseClientServerMessage(
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

	def test_many_clients_sending_many_messages_delayed_threaded_64c_100m_md300ms_cb0ms_0h1e(self):

		clients_total = 64
		messages_per_client_total = 100
		message_delay_seconds = 0.300
		client_begin_sending_messages_delay_seconds = 0
		hellos_total = 0
		echoes_total = 1

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
						client_server_message=HelloWorldBaseClientServerMessage()
					)
					hello_index += 1
					if hello_index == hellos_total and echo_index == echoes_total:
						hello_index = 0
						echo_index = 0
				elif echo_index < echoes_total:
					client_messenger.send_to_server(
						client_server_message=EchoRequestBaseClientServerMessage(
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

	def test_many_clients_sending_many_messages_delayed_threaded_64c_100m_md200ms_cb0ms_0h1e(self):

		clients_total = 64
		messages_per_client_total = 100
		message_delay_seconds = 0.200
		client_begin_sending_messages_delay_seconds = 0
		hellos_total = 0
		echoes_total = 1

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
						client_server_message=HelloWorldBaseClientServerMessage()
					)
					hello_index += 1
					if hello_index == hellos_total and echo_index == echoes_total:
						hello_index = 0
						echo_index = 0
				elif echo_index < echoes_total:
					client_messenger.send_to_server(
						client_server_message=EchoRequestBaseClientServerMessage(
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

		server_messenger = get_default_server_messenger_factory().get_server_messenger()

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
			client_messenger = get_default_client_messenger_factory().get_client_messenger()
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
					client_server_message=EchoRequestBaseClientServerMessage(
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

	# TODO implement 0KB, 1KB, then 2KB, etc.
