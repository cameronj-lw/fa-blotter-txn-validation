
# core python
import json
from abc import ABC, abstractmethod
from typing import Union

# pypi
from confluent_kafka import Consumer, OFFSET_BEGINNING, OFFSET_END

# native
from app.domain.events import Event
from app.domain.event_handlers import EventHandler
from app.domain.event_subscribers import EventSubscriber

from app.infrastructure.message_brokers import KafkaBroker
from app.infrastructure.util.config import AppConfig
from app.infrastructure.events import RawAPXTransactionEvent

class KafkaEventConsumer(EventSubscriber):
    def __init__(self, topics, event_handler):
        super().__init__(message_broker=KafkaBroker(), topics=topics, event_handler=event_handler)
        self.config = dict(self.message_broker.config)
        self.config.update(AppConfig().parser['kafka_consumer'])
        print(f'Creating KafkaEventConsumer with config: {self.config}')
        self.consumer = Consumer(self.config)

    def consume(self, reset_offset: bool=False):
        
        try:
            sleep_secs = int(AppConfig().get('kafka_consumer_lw', 'sleep_seconds', fallback=0))
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting...")
                elif msg.error():
                    print(f"ERROR: {msg.error()}")
                elif msg.value() is not None:
                    print(f"Consuming message: {msg.value()}")

        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            print(f'Committing offset and closing {self.__class__.__name__}...')
            self.consumer.close()

    def on_assign(self, consumer, partitions):
        if self.reset_offset:
            for p in partitions:
                print(f"Resetting offset for {p}")
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    @abstractmethod
    def deserialize(self, message_value: bytes) -> Union[Event, None]:
        """ 
        Subclasses of KafkaEventConsumer must implement a deserialize method.
        Returning None (rather than an Event) signifies that there is no Event to handle.
        This makes sense when the consumer is looking for specific criteria to represent 
        the desired Event, but that criteria is not necessarily met in every message from the topic(s).
        """
        
    def __del__(self):
        self.consumer.close()


class KafkaRawAPXTransactionEventConsumer(KafkaEventConsumer):
    def __init__(self, event_handler: EventHandler):
        """ Creates a KafkaEventConsumer to consume new/changed coredb securities with the provided event handler """
        super().__init__(event_handler=event_handler, topics=[AppConfig().get('kafka_topics', 'apxdb_transaction')])

    def deserialize(self, message_value: bytes) -> RawAPXTransactionEvent:
        event_dict = json.loads(message_value.decode('utf-8'))
        return RawAPXTransactionEvent(event_dict)

