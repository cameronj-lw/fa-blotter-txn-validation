
from dataclasses import dataclass, field
import datetime
from typing import Dict

# native
from application.validation_rules import TransactionValidationRule
from domain.models import Transaction, BlotterTradeSettlementCriteria, BlotterType
from domain.repositories import TransactionRepository


@dataclass
class TransactionValidationRuleBrokenException(Exception):
    rule: TransactionValidationRule
    transaction: Transaction


# @dataclass
# class BlotterNotFoundException(Exception):
#     settlement_criteria: BlotterTradeSettlementCriteria
#     type_: BlotterType
#     trade_date: datetime.date = field(default_factory=datetime.date.today)


@dataclass
class TransactionCountMismatchException(Exception):
    repos_counts: Dict[TransactionRepository, int]

