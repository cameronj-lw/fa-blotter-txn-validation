
# core python
import argparse
import datetime
import json
import logging
import os
import sys

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from application.exceptions import TransactionCountMismatchException
from application.query_handlers import TransactionCountComparator
from domain.models import BlotterTradeSettlementCriteria
from infrastructure.services import MSTeamsAlertService
from infrastructure.sql_repositories import APXDBTransactionRepository, LWDBBONASentTransactionRepository, APXDBFABlotterV2Repository
from infrastructure.util.config import AppConfig
from infrastructure.util.logging import setup_logging




def main():
    parser = argparse.ArgumentParser(description='Transaction count comparator')
    parser.add_argument('--trade_date', '-td', type=str, required=True, help='Trade date, YYYYMMDD format')
    parser.add_argument('--settlement_criteria', '-sc', type=int, choices=[0, 1], required=True, help='0 for T+0, 1 for T+1')
    parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
    
    args = parser.parse_args()

    trade_date = datetime.datetime.strptime(args.trade_date, '%Y%m%d').date()
    settlement_criteria = BlotterTradeSettlementCriteria.t_plus_one if args.settlement_criteria else BlotterTradeSettlementCriteria.t_plus_zero
    
    repos = [
        APXDBTransactionRepository(),
        LWDBBONASentTransactionRepository(),
        APXDBFABlotterV2Repository(),
    ]

    comparator = TransactionCountComparator(
        repos=repos
    )
    base_dir = AppConfig().get("logging", "base_dir")
    os.environ['APP_NAME'] = AppConfig().get("app_name", "fa_blotter_txn_counts_comparator")
    setup_logging(base_dir=base_dir, log_level_override=args.log_level)
    logging.info(f"Comparing transaction counts for {trade_date} {settlement_criteria.name} between the following repositories: {', '.join([r.cn for r in repos])}")

    try:
        comparator.compare(trade_date=trade_date, settlement_criteria=settlement_criteria)
        logging.info(f"Comparison passed.")
    except TransactionCountMismatchException as e:
        readable_dict = {str(k): v for k, v in e.repos_counts.items()}
        logging.info(f"Comparison failed due to differences in counts: {json.dumps(readable_dict, indent=4)}")



if __name__ == '__main__':
    main()
