
# core python
import argparse
import logging
import os
import sys

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from application.event_handlers import TransactionEventHandler
from application.validation_rules import TransactionQuantityMax100, TransactionPostedAfterBlotterSent
from application.validators import TransactionValidator
from infrastructure.file_repositories import FABlotterV1BlotterRepository
from infrastructure.message_subscribers import KafkaAPXTransactionMessageConsumer
from infrastructure.services import MSTeamsAlertService
from infrastructure.sql_repositories import MGMTDBHeartbeatRepository
from infrastructure.util.config import AppConfig
from infrastructure.util.logging import setup_logging




def main():
    parser = argparse.ArgumentParser(description='Kafka Consumer')
    parser.add_argument('--reset_offset', '-ro', action='store_true', default=False, help='Reset consumer offset to beginning')
    parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
    
    args = parser.parse_args()

    kafka_consumer = KafkaAPXTransactionMessageConsumer(
        event_handler = TransactionEventHandler(
            validator=TransactionValidator([
                TransactionQuantityMax100(),
                TransactionPostedAfterBlotterSent(
                    blotter_repo=FABlotterV1BlotterRepository(),
                    fail_alert_services=[MSTeamsAlertService(AppConfig().get('transaction_posted_after_blotter_sent', 'ms_teams_webhook_url'))]
                )
            ])
        )
        , heartbeat_repo = MGMTDBHeartbeatRepository()
    )
    base_dir = AppConfig().get("logging", "base_dir")
    os.environ['APP_NAME'] = AppConfig().get("app_name", "fa_blotter_txn_validation")
    setup_logging(base_dir=base_dir, log_level_override=args.log_level)
    logging.info(f'Consuming transactions...')
    kafka_consumer.consume(reset_offset=args.reset_offset)



if __name__ == '__main__':
    main()
