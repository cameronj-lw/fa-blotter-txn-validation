
# core python
from abc import ABC
from dataclasses import dataclass

# native
from domain.models import Transaction


class Event(ABC):
    """ Base class for domain events """


@dataclass
class TransactionCreatedEvent(Event):
    transaction: Transaction


@dataclass
class TransactionUpdatedEvent(Event):
    transaction_before: Transaction
    transaction_after: Transaction


@dataclass
class TransactionDeletedEvent(Event):
    transaction: Transaction

