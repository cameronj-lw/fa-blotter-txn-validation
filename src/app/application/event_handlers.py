
# core python
from dataclasses import dataclass
import json
import logging
from typing import Union

# native
from application.exceptions import TransactionValidationRuleBrokenException
from application.validators import TransactionValidator
from domain.event_handlers import EventHandler
from domain.events import (Event, TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent
    , TransactionCommentCreatedEvent, TransactionCommentUpdatedEvent, TransactionCommentDeletedEvent
)



@dataclass
class TransactionEventHandler(EventHandler):
    validator: Union[TransactionValidator, None] = None

    def handle(self, event: Union[TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent
                , TransactionCommentCreatedEvent, TransactionCommentUpdatedEvent, TransactionCommentDeletedEvent]):
        try:
            if isinstance(event, TransactionCreatedEvent):
                logging.info(f'{self.cn} consuming transaction created event:')
                logging.info(f'{event.transaction}')
                if self.validator:
                    self.validator.validate(event.transaction)
                    logging.info('Passed all validations.')
                return True
            elif isinstance(event, TransactionUpdatedEvent):
                logging.info(f'{self.cn} consuming transaction updated event:')
                logging.info(f'BEFORE: {event.transaction_before}')
                logging.info(f' AFTER: {event.transaction_after}')
                if self.validator:
                    self.validator.validate(event.transaction_after)
                    logging.info('Passed all validations.')
                return True
            elif isinstance(event, TransactionDeletedEvent):
                logging.info(f'{self.cn} consuming transaction deleted event:')
                logging.info(f'{event.transaction}')
                if self.validator:
                    self.validator.validate(event.transaction)
                    logging.info('Passed all validations.')
                return True
            else:
                logging.info(f'{self.cn} ignoring {event.cn}')
                return True

        except TransactionValidationRuleBrokenException as e:
            logging.info(f'{e.transaction} failed rule {e.rule}!')
            return True

    def __str__(self):
        return f"{self.cn}, using validator {self.validator}"


