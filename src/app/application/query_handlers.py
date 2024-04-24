
from dataclasses import dataclass
import datetime
from typing import List, Union

from application.exceptions import TransactionCountMismatchException
from domain.models import Transaction, BlotterTradeSettlementCriteria, BlotterType
from domain.repositories import TransactionRepository


@dataclass
class TransactionCountComparator:
    repos: List[TransactionRepository]

    def compare(self, trade_date: Union[datetime.date,None]=None, settlement_criteria: Union[BlotterTradeSettlementCriteria,None]=None):
        """ Handle the query """

        # Populate defaults
        if not trade_date:
            trade_date = datetime.date.today()
        if not settlement_criteria:
            settlement_criteria = payload.get('settlement_criteria') or BlotterTradeSettlementCriteria.t_plus_one

        # Query counts from each repo; save to dict
        repos_counts = {}
        for repo in self.repos:
            all_txns = repo.get(trade_date=trade_date)
            if settlement_criteria == BlotterTradeSettlementCriteria.t_plus_one:
                relevant_txns = [t for t in all_txns if t.TradeDate != t.SettleDate]
            else:
                relevant_txns = [t for t in all_txns if t.TradeDate == t.SettleDate]

            repos_counts[repo] = len(relevant_txns)

        # Check if all counts are identical
        counts = [v for k, v in repos_counts.items()]
        if len(set(counts)) > 1:
            raise TransactionCountMismatchException(repos_counts=repos_counts)


