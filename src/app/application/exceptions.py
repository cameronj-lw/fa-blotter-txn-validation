
from dataclasses import dataclass

# native
from application.validation_rules import TransactionValidationRule
from domain.models import Transaction


@dataclass
class TransactionValidationRuleBrokenException(Exception):
    rule: TransactionValidationRule
    transaction: Transaction



