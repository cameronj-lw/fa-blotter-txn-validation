
# core python
from dataclasses import dataclass, field
import datetime
from enum import Enum
from types import SimpleNamespace
from typing import Literal


class Transaction(SimpleNamespace):
    """ Facilitates object instance creation from dict """
    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def __str__(self):
        return f"{self.TransactionCode} of {self.Quantity} units of {self.SecurityID1} in {self.PortfolioID} on {self.TradeDate}"
        

class TransactionComment(SimpleNamespace):
    """ Facilitates object instance creation from dict """
    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def __str__(self):
        return f"Comment in {self.PortfolioID} on {self.TradeDate}: {self.Comment}"


BlotterTradeSettlementCriteria = Enum('BlotterTradeSettlementCriteria', 't_plus_zero t_plus_one')
BlotterType = Enum('BlotterType', 'regular amendment adhoc')
BlotterSendStatus = Enum('BlotterSendStatus', 'IN_PROGRESS SUCCESS FAIL UNKNOWN')

@dataclass
class Blotter:
    """ Blotter which can optionally specify a batch of transactions """
    settlement_criteria: BlotterTradeSettlementCriteria
    type_: BlotterType
    trade_date: datetime.date = field(default_factory=datetime.date.today)
    status: BlotterSendStatus = BlotterSendStatus.UNKNOWN
    modified_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    transactions: list[Transaction] = field(default_factory=list)

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def __str__(self):
        settlement_criteria_str = 'T+1' if self.settlement_criteria == BlotterTradeSettlementCriteria.t_plus_one else 'T+0'
        return f"{self.trade_date} {settlement_criteria_str} {self.type_.name} blotter: {self.status.name} as of {self.modified_at.strftime('%Y-%m-%d %H:%M:%S')}"


@dataclass
class Heartbeat:
    group: str
    name: str
    data_date: datetime.date = field(default_factory=datetime.date.today)
    modified_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def to_dict(self):
        """ Export an instance to dict format """
        return {
            'group': self.group
            , 'name': self.name
            , 'data_date': self.data_date.isoformat() if self.data_date else self.data_date
            , 'modified_at': self.modified_at.isoformat() if self.modified_at else self.modified_at
        }

    @classmethod
    def from_dict(cls, data: dict):
        """ Create an instance from dict """
        try:
            # Validate required fields
            group = data['group']
            name = data['name']

            # Optional fields
            data_date = data.get('data_date', datetime.date.today())
            if isinstance(data_date, str):
                data_date = datetime.date.fromisoformat(data_date)

            modified_at = data.get('modified_at', datetime.datetime.now())
            if isinstance(modified_at, str):
                modified_at = datetime.datetime.fromisoformat(modified_at)

            return cls(group=group, name=name, data_date=data_date, modified_at=modified_at)
        except KeyError as e:
            raise InvalidDictError(f"Missing required field: {e}")


@dataclass
class Alert:
    title: str
    body: str

