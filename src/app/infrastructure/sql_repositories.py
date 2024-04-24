
# core python
import datetime
import logging
from typing import List, Union

# pypi
import pandas as pd

# native
from domain.models import Heartbeat, Transaction
from domain.repositories import HeartbeatRepository, TransactionRepository
from infrastructure.models import MGMTDBHeartbeat
from infrastructure.sql_tables import MGMTDBMonitorTable, LWDBNotificationTable, APXDBvPortfolioTransactionView, APXDBvPortfolioTransactionLWFundsView



""" LWDB """

class LWDBBONASentTransactionRepository(TransactionRepository):
    table = LWDBNotificationTable()

    def create(self, transaction: Transaction) -> int:
        raise NotImplementedError("Cannot write to {self.cn}")

    def get(self, trade_date: Union[datetime.date,None]=None, portfolio_code: Union[str,None]=None) -> List[Transaction]:
        query_result = pd.concat([
            self.table.read(scenario='CUSTODIAN.PRIMARY', status='Sent', trade_date=trade_date, portfolio_code=portfolio_code),
            self.table.read(scenario='SSCNET.PRIMARY', status='Sent', trade_date=trade_date, portfolio_code=portfolio_code),
        ])
        # TODO: filter for transactions in LW Fund portfolios only
        # query_result = self.table.read(scenario='CUSTODIAN.PRIMARY', status='Sent', trade_date=trade_date, portfolio_code=portfolio_code)
        
        # Convert to dict:
        query_result_dicts = query_result.to_dict('records')

        transactions = [Transaction(**qrd) for qrd in query_result_dicts]
        return transactions
        
    def __str__(self):
        return 'Transactions sent via BONA gateway'


""" APXDB """

class APXDBTransactionRepository(TransactionRepository):
    table = APXDBvPortfolioTransactionLWFundsView()

    def create(self, transaction: Transaction) -> int:
        raise NotImplementedError("Cannot write to {self.cn}")

    def get(self, trade_date: Union[datetime.date,None]=None, portfolio_code: Union[str,None]=None) -> List[Transaction]:
        query_result = self.table.read(trade_date=trade_date)
        
        # Convert to dict:
        query_result_dicts = query_result.to_dict('records')

        transactions = [Transaction(**qrd) for qrd in query_result_dicts]
        return transactions
        
    def __str__(self):
        return 'Transactions posted to APX'


class APXDBFABlotterV2Repository(TransactionRepository):
    # TODO: change this class to pull from FA Blotter v2 view
    table = APXDBvPortfolioTransactionLWFundsView()

    def create(self, transaction: Transaction) -> int:
        raise NotImplementedError("Cannot write to {self.cn}")

    def get(self, trade_date: Union[datetime.date,None]=None, portfolio_code: Union[str,None]=None) -> List[Transaction]:
        query_result = self.table.read(trade_date=trade_date)
        
        # Convert to dict:
        query_result_dicts = query_result.to_dict('records')

        transactions = [Transaction(**qrd) for qrd in query_result_dicts]
        return transactions
        
    def __str__(self):
        return 'Transactions slated for FA Blotter v2'


""" MGMTDB """

class MGMTDBHeartbeatRepository(HeartbeatRepository):
    # Specify which class callers are recommended to use when instantiating heartbeat instances
    heartbeat_class = MGMTDBHeartbeat
    table = MGMTDBMonitorTable()

    def create(self, heartbeat: Heartbeat) -> int:

        # Create heartbeat instance
        hb_dict = heartbeat.to_dict()

        # Populate with table base scenario
        hb_dict['scenario'] = self.table.base_scenario

        # Truncate asofuser if needed
        hb_dict['asofuser'] = (hb_dict['asofuser'] if len(hb_dict['asofuser']) <= 32 else hb_dict['asofuser'][:32])

        # Columns used as basis for upsert
        pk_columns = ['data_dt', 'scenario', 'run_group', 'run_name', 'run_type', 'run_host', 'run_status_text']

        # Remove columns not in the table def
        hb_dict = {k: hb_dict[k] for k in hb_dict if k in self.table.c.keys()}

        # Bulk insert new rows:
        logging.debug(f"{self.cn}: About to upsert {hb_dict}")
        res = self.table.upsert(pk_column_name=pk_columns, data=hb_dict)  # TODO_EH: error handling?
        if isinstance(res, int):
            row_cnt = res
        else:
            row_cnt = res.rowcount
        if abs(row_cnt) != 1:
            raise UnexpectedRowCountException(f"Expected 1 row to be saved, but there were {row_cnt}!")
        logging.debug(f'End of {self.cn} create: {heartbeat}')
        return row_cnt

    def get(self, data_date: Union[datetime.date,None]=None, group: Union[str,None]=None, name: Union[str,None]=None) -> List[Heartbeat]:
        # Query table - returns result into df:
        query_result = self.table.read(scenario=self.table.base_scenario, data_date=data_date, run_group=group, run_name=name, run_type='INFO', run_status_text='HEARTBEAT')
        
        # Convert to dict:
        query_result_dicts = query_result.to_dict('records')

        # Create list of heartbeats
        heartbeats = [self.heartbeat_class.from_dict(qrd) for qrd in query_result_dicts]

        # Return result heartbeats list
        return heartbeats

    @classmethod
    def readable_name(self):
        return 'MGMTDB Monitor table'
