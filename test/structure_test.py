import unittest
from src.austin_heller_repo.socket_kafka_message_framework import StructureFactory, Structure, StructureStateEnum, UpdateStructureInfluence
from austin_heller_repo.kafka_manager import KafkaReader, KafkaWrapper, KafkaManagerFactory, KafkaManager, KafkaAsyncWriter
from austin_heller_repo.common import StringEnum
from typing import List, Tuple, Dict, Callable, Type


class TransitionEnum(StringEnum):
	FromInitialToMiddle = "from_initial_to_middle"
	FromMiddleToFinal = "from_middle_to_final"


class BaseStructureStateEnum(StructureStateEnum):
	Initial = "initial"
	Middle = "middle"
	Final = "final"


class BaseStructure(Structure):

	def __init__(self, *, on_transition_from_initial_to_middle: Callable[[str], None], on_transition_from_middle_to_final: Callable[[str], None]):
		super().__init__(
			states=BaseStructureStateEnum,
			initial_state=BaseStructureStateEnum.Initial
		)

		self.__on_transition_from_initial_to_middle = on_transition_from_initial_to_middle
		self.__on_transition_from_middle_to_final = on_transition_from_middle_to_final

		self.add_transition(
			trigger=TransitionEnum.FromInitialToMiddle.value,
			source=BaseStructureStateEnum.Initial.value,
			dest=BaseStructureStateEnum.Middle.value,
			before=f"_{BaseStructure.__name__}{BaseStructure.__on_transition_from_initial_to_middle.__name__}"
		)

		self.add_transition(
			trigger=TransitionEnum.FromMiddleToFinal.value,
			source=BaseStructureStateEnum.Middle.value,
			dest=BaseStructureStateEnum.Final.value,
			before=f"_{BaseStructure.__name__}{BaseStructure.__on_transition_from_middle_to_final.__name__}"
		)

	def __on_transition_from_initial_to_middle(self, message: str):
		self.__on_transition_from_initial_to_middle(message)

	def __on_transition_from_middle_to_final(self, message: str):
		self.__on_transition_from_middle_to_final(message)


class StructureTest(unittest.TestCase):

	def test_initialize(self):

		def on_transition_from_initial_to_middle(message: str):
			print("on_transition_from_initial_to_middle started")

		def on_transition_from_middle_to_final(message: str):
			print("on_transition_from_middle_to_final started")

		base_structure = BaseStructure(
			on_transition_from_initial_to_middle=on_transition_from_initial_to_middle,
			on_transition_from_middle_to_final=on_transition_from_middle_to_final
		)

		self.assertIsNotNone(base_structure)

	def test_transition(self):

		found_messages = []  # type: List[Tuple[int, str]]

		def on_transition_from_initial_to_middle(message: str):
			nonlocal found_messages
			found_messages.append((0, message))

		def on_transition_from_middle_to_final(message: str):
			nonlocal found_messages
			found_messages.append((1, message))

		base_structure = BaseStructure(
			on_transition_from_initial_to_middle=on_transition_from_initial_to_middle,
			on_transition_from_middle_to_final=on_transition_from_middle_to_final
		)

		self.assertIsNotNone(base_structure)

		base_structure.trigger(TransitionEnum.FromInitialToMiddle.value, "first")

		base_structure.trigger(TransitionEnum.FromMiddleToFinal.value, "second")

		self.assertEqual(2, len(found_messages))
		self.assertEqual((0, "first"), found_messages[0])
		self.assertEqual((1, "second"), found_messages[1])
