
# core python
from dataclasses import dataclass, field
from typing import Any, List

# native
from application.exceptions import TransactionValidationRuleBrokenException
from application.validation_rules import TransactionValidationRule
from domain.models import Transaction


@dataclass
class TransactionValidator:
    rules: List[TransactionValidationRule] = field(default_factory=list)

    def validate(self, transaction: Transaction):
        for rule in self.rules:
            if rule.is_broken(transaction):
                raise TransactionValidationRuleBrokenException(rule, transaction)


