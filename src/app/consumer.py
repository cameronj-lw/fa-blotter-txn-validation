
# core python
import os
import sys

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from infrastructure.event_handlers import RawAPXTransactionEventHandler
from infrastructure.event_subscribers import KafkaRawAPXTransactionEventConsumer




def main():
    kafka_consumer = KafkaRawAPXTransactionEventConsumer(
        event_handler = RawAPXTransactionEventHandler()
    )
    kafka_consumer.consume(reset_offset=False)



if __name__ == '__main__':
    main()
