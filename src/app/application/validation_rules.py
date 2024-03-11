
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Union

from domain.models import Transaction



@dataclass
class TransactionValidationRule(ABC):
    name: Union[str, None] = None

    def __post_init__(self):
        if not self.name:
            self.name = self.__class__.__name__

    def __str__(self):
        return self.name

    @abstractmethod
    def is_broken(self, transaction: Transaction):
        pass



class TransactionQuantityMax100(TransactionValidationRule):
    def is_broken(self, transaction: Transaction):
        return transaction.Quantity > 100

