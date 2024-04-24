
# core python
from dataclasses import dataclass, field
import logging
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
            logging.info(f'Checking rule {rule}')
            if rule.is_broken(transaction):
                raise TransactionValidationRuleBrokenException(rule, transaction)

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__
    
    def __str__(self):
        return f"{self.cn}, validating transaction rules: {', '.join([str(vr) for vr in self.rules])}"


