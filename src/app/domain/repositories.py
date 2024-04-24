
# core python
from abc import ABC, abstractmethod
import datetime
from typing import List, Union

# native
from domain.models import Heartbeat, Transaction, Blotter, BlotterTradeSettlementCriteria, BlotterType


class HeartbeatRepository(ABC):
    
    @abstractmethod
    def create(self, heartbeat: Heartbeat) -> int:
        pass

    @abstractmethod
    def get(self, data_date: Union[datetime.date,None]=None, group: Union[str,None]=None, name: Union[str,None]=None) -> List[Heartbeat]:
        pass

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__


class TransactionRepository(ABC):
    
    @abstractmethod
    def create(self, transaction: Transaction) -> int:
        pass

    @abstractmethod
    def get(self, trade_date: Union[datetime.date,None]=None, portfolio_code: Union[str,None]=None) -> List[Transaction]:
        pass

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def __str__(self):
        return self.cn

class BlotterRepository(ABC):
    
    @abstractmethod
    def create(self, blotter: Blotter) -> int:
        pass

    @abstractmethod
    def get(self, settlement_criteria: Union[BlotterTradeSettlementCriteria,None]=None
                , type_: Union[BlotterType,None]=None, trade_date: Union[datetime.date,None]=None) -> List[Blotter]:
        pass

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

