
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import logging
from typing import Any, List, Union

from domain.models import Alert, Transaction, Blotter, BlotterTradeSettlementCriteria, BlotterType, BlotterSendStatus
from domain.repositories import BlotterRepository
from domain.services import AlertService



@dataclass
class TransactionValidationRule(ABC):
    name: Union[str, None] = None
    fail_alert_services: Union[List[AlertService],None] = None

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def __post_init__(self):
        if not self.name:
            self.name = self.cn

    def __str__(self):
        return self.name

    @abstractmethod
    def is_broken(self, transaction: Transaction) -> bool:
        pass

    def send_alert_for_transaction(self, transaction: Transaction):
        """ If there are no fail_alert_services, this will do nothing """ 
        """ Subclasses may override """

        title = body = f'{e.transaction} failed rule {self}!'
        logging.info(title)

        # If the rule has any alert services, send alerts using them:
        alert = Alert(title=title, body=body)  # TODO: different body and message?
        logging.info(f'{self} sending alert {alert}')

        if self.fail_alert_services:
            for service in self.fail_alert_services:
                # TODO_EH: what if the alert sending fails?
                service.send_alert(alert)


class TransactionQuantityMax100(TransactionValidationRule):
    def is_broken(self, transaction: Transaction):
        return transaction.Quantity > 100



class TransactionNotFoundInLZ(TransactionValidationRule):
    def is_broken(self, transaction: Transaction):
        return False  # TODO: implement
        # Query Nelson's APX txn view to supplement with attributes (portfolio code, sec symbol, ...)
        # Query LZ txns matching trade date, portfolio code, quantity, ... 
        # if not found, return True

# from application.exceptions import BlotterNotFoundException  # here to avoid circular reference

class TransactionPostedAfterBlotterSent(TransactionValidationRule):
    def __init__(self, blotter_repo: BlotterRepository, fail_alert_services: Union[List[AlertService],None] = None):
        super().__init__(name=None, fail_alert_services=fail_alert_services)
        self.blotter_repo = blotter_repo

    def is_broken(self, transaction: Transaction):
        blotter = self.get_relevant_blotter(transaction)
        if blotter.status in (BlotterSendStatus.IN_PROGRESS, BlotterSendStatus.SUCCESS):
            return True
        else:
            return False
        
    def get_relevant_blotter(self, transaction: Transaction):
        # return BlotterSendStatus.SUCCESS  # TODO_TEST: actuall use blotter_repo
        trade_date = transaction.TradeDate  # TODO_EH: error handling - what if there is no TradeDate?
        settle_date = transaction.SettleDate  # TODO_EH: error handling - what if there is no SettleDate?

        if trade_date == settle_date:
            settlement_criteria = BlotterTradeSettlementCriteria.t_plus_zero
            type_ = BlotterType.regular
        else:
            # TODO_EH: Asusmption: T+1 if TD != SD ... is this assumption desirable?
            settlement_criteria = BlotterTradeSettlementCriteria.t_plus_one
            type_ = BlotterType.amendment

        relevant_blotters = self.blotter_repo.get(settlement_criteria=settlement_criteria, type_=type_, trade_date=trade_date)
        if not len(relevant_blotters):
            pass  # TODO_EH: raise BlotterNotFoundException(settlement_criteria=settlement_criteria, type_=type_, trade_date=trade_date)
        else:
            # TODO_EH: is it possible / problematic if there are 2+ relevant_blotters?
            return relevant_blotters[0]
            
    def send_alert_for_transaction(self, transaction: Transaction):
        """ If there are no fail_alert_services, this will do nothing """ 
        
        title = f'Transaction posted after blotter has been sent!'
        blotter = self.get_relevant_blotter(transaction)
        body = f"The following transaction was posted: {transaction}   \n{blotter}"

        # If the rule has any alert services, send alerts using them:
        alert = Alert(title=title, body=body)  # TODO: different body and message?

        if self.fail_alert_services:
            logging.info(f'{self} sending alert {alert}')
            for service in self.fail_alert_services:
                # TODO_EH: what if the alert sending fails?
                service.send_alert(alert)



