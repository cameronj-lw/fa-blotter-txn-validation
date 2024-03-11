
# core python
import os
import sys

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from application.event_handlers import TransactionEventHandler
from application.validation_rules import TransactionQuantityMax100
from application.validators import TransactionValidator
from infrastructure.message_subscribers import KafkaAPXTransactionMessageConsumer




def main():
    kafka_consumer = KafkaAPXTransactionMessageConsumer(
        event_handler = TransactionEventHandler(
            validator=TransactionValidator([
                TransactionQuantityMax100()
            ])
        )
    )
    kafka_consumer.consume(reset_offset=False)



if __name__ == '__main__':
    main()
