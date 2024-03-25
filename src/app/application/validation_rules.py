
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Union

from domain.models import Transaction



@dataclass
class TransactionValidationRule(ABC):
    name: Union[str, None] = None

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def __post_init__(self):
        if not self.name:
            self.name = self.cn

    def __str__(self):
        return self.name

    @abstractmethod
    def is_broken(self, transaction: Transaction):
        pass



class TransactionQuantityMax100(TransactionValidationRule):
    def is_broken(self, transaction: Transaction):
        return transaction.Quantity > 100



class TransactionNotFoundInLZ(TransactionValidationRule):
    def is_broken(self, transaction: Transaction):
        return False  # TODO: implement
        # Query Nelson's APX txn view to supplement with attributes (portfolio code, sec symbol, ...)
        # Query LZ txns matching trade date, portfolio code, quantity, ... 
        # if not found, return True

