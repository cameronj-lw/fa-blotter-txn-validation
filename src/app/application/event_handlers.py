
# core python
from dataclasses import dataclass
import json
from typing import Union

# native
from application.exceptions import TransactionValidationRuleBrokenException
from application.validators import TransactionValidator
from domain.event_handlers import EventHandler
from domain.events import Event, TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent



@dataclass
class TransactionEventHandler(EventHandler):
    validator: Union[TransactionValidator, None] = None

    def handle(self, event: Union[TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent]):
        try:
            if isinstance(event, TransactionCreatedEvent):
                print(f'{self.__class__.__name__} consuming transaction created event:')
                print(f'{event.transaction}')
                if self.validator:
                    self.validator.validate(event.transaction)
                    print('Passed all validations.')
                return True
            elif isinstance(event, TransactionUpdatedEvent):
                print(f'{self.__class__.__name__} consuming transaction updated event:')
                print(f'BEFORE: {event.transaction_before}')
                print(f' AFTER: {event.transaction_after}')
                if self.validator:
                    self.validator.validate(event.transaction_after)
                    print('Passed all validations.')
                return True
            elif isinstance(event, TransactionDeletedEvent):
                print(f'{self.__class__.__name__} consuming transaction deleted event:')
                print(f'{event.transaction}')
                if self.validator:
                    self.validator.validate(event.transaction)
                    print('Passed all validations.')
                return True

        except TransactionValidationRuleBrokenException as e:
            print(f'{e.transaction} failed rule {e.rule}!')
            return True


