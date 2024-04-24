
import datetime
import os
from typing import List, Union

from domain.models import Blotter, BlotterTradeSettlementCriteria, BlotterType, BlotterSendStatus
from domain.repositories import BlotterRepository
from infrastructure.models import FABlotterV1BlotterFile
from infrastructure.util.config import AppConfig
from infrastructure.util.file import prepare_dated_file_path

class FABlotterV1BlotterRepository(BlotterRepository):
    
    def create(self, blotter: Blotter) -> int:
        raise NotImplementedError()

    def get(self, settlement_criteria: Union[BlotterTradeSettlementCriteria,None]=None
                , type_: Union[BlotterType,None]=None, trade_date: Union[datetime.date,None]=None) -> List[FABlotterV1BlotterFile]:
        
        blotter_file_full_path = self.get_blotter_file_full_path(settlement_criteria=settlement_criteria, type_=type_, trade_date=trade_date)
        if os.path.exists(blotter_file_full_path):
            return [FABlotterV1BlotterFile(settlement_criteria=settlement_criteria, type_=type_, trade_date=trade_date, 
                            status=BlotterSendStatus.SUCCESS, modified_at=datetime.datetime.fromtimestamp(os.path.getmtime(blotter_file_full_path)))
            ]
        else:
            return [FABlotterV1BlotterFile(settlement_criteria=settlement_criteria, type_=type_, trade_date=trade_date, 
                            status=BlotterSendStatus.UNKNOWN)
            ]

    # TODO_CLEANUP: remove below once confirmed not using
    def get_blotter_file_full_path(self, settlement_criteria: Union[BlotterTradeSettlementCriteria,None]=None
                , type_: Union[BlotterType,None]=None, trade_date: Union[datetime.date,None]=None) -> str:

        if trade_date is None:
            trade_date = datetime.date.today()
                
        base_dir = AppConfig().get('files', 'bona_notification_dir')

        if settlement_criteria == BlotterTradeSettlementCriteria.t_plus_zero:
            if type_ == BlotterType.regular:
                return os.path.join(base_dir, trade_date.strftime('%Y%m'), trade_date.strftime('%d'), 'BLOTTER', 
                                    f"CIBCLWMutualFundAcctSameDaySettlement_{trade_date.strftime('%Y%m%d')}.xlsx")
            elif type_ == BlotterType.amendment:
                pass  # TODO: need this?
        elif settlement_criteria == BlotterTradeSettlementCriteria.t_plus_one:
            if type_ == BlotterType.regular:
                return os.path.join(base_dir, trade_date.strftime('%Y%m'), trade_date.strftime('%d'), 'BLOTTER', 
                                    f"CIBCLWMutualFundAcct_{trade_date.strftime('%Y%m%d')}.xlsx")
            elif type_ == BlotterType.amendment:
                # TODO: correctly implement
                return os.path.join(base_dir, trade_date.strftime('%Y%m'), trade_date.strftime('%d'), 'BLOTTER', 
                                    f"CIBCLWMutualFundAcct_{trade_date.strftime('%Y%m%d')}.xlsx")
        
        return None

