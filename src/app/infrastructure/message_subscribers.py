
# core python
import json
from abc import ABC, abstractmethod
import datetime
from typing import List, Type, Union

# pypi
from confluent_kafka import Consumer, OFFSET_BEGINNING, OFFSET_END

# native
from domain.events import Event, TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent
from domain.event_handlers import EventHandler
from domain.message_brokers import MessageBroker
from domain.message_subscribers import MessageSubscriber
from domain.models import Transaction

from infrastructure.message_brokers import KafkaBroker
from infrastructure.util.config import AppConfig


class DeserializationError(Exception):
    pass



class KafkaMessageConsumer(MessageSubscriber):
    def __init__(self, topics, event_handler):
        super().__init__(message_broker=KafkaBroker(), topics=topics, event_handler=event_handler)
        self.config = dict(self.message_broker.config)
        self.config.update(AppConfig().parser['kafka_consumer'])
        print(f'Creating KafkaMessageConsumer with config: {self.config}')
        self.consumer = Consumer(self.config)

    def consume(self, reset_offset: bool=False):
        
        print(f'Consuming from topics: {self.topics}')

        self.reset_offset = reset_offset
        self.consumer.subscribe(self.topics, on_assign=self.on_assign)

        try:
            sleep_secs = int(AppConfig().get('kafka_consumer_lw', 'sleep_seconds', fallback=0))
            while True:
                msg = self.consumer.poll(5.0)
                if msg is None:
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting...")
                elif msg.error():
                    print(f"ERROR: {msg.error()}")
                elif msg.value() is not None:
                    # print(f"Consuming message: {msg.value()}")
                    should_commit = True  # commit at the end, unless this gets overridden below
                    try:
                        event = self.deserialize(msg.value())

                        if event is None:
                            # A deserialize method returning None means the kafka message
                            # does not meet criteria for representing an Event that needs handling.
                            # Therefore if reaching here we should simply commit offset.
                            self.consumer.commit(message=msg)
                            continue
                        
                        # If reaching here, we have an Event that should be handled:
                        # print(f"Handling {event}")
                        should_commit = self.event_handler.handle(event)
                        # print(f"Done handling {event}")
                
                    except Exception as e:
                        if isinstance(e, DeserializationError):
                            print(f'Exception while deserializing: {e}')
                            should_commit = self.event_handler.handle_deserialization_error(e)
                        else:
                            print(e)  # TODO: any more valuable logging?
                    
                    # Commit, unless we should not based on above results
                    if should_commit:
                        self.consumer.commit(message=msg)
                        print("Done committing offset")
                    else:
                        print("Not committing offset, likely due to the most recent exception")


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
        Subclasses of KafkaMessageConsumer must implement a deserialize method.
        Returning None (rather than an Event) signifies that there is no Event to handle.
        This makes sense when the consumer is looking for specific criteria to represent 
        the desired Event, but that criteria is not necessarily met in every message from the topic(s).
        """
        
    def __del__(self):
        self.consumer.close()


class KafkaAPXTransactionMessageConsumer(KafkaMessageConsumer):
    def __init__(self, event_handler: EventHandler):
        """ Creates a KafkaMessageConsumer to consume new/changed coredb securities with the provided event handler """
        super().__init__(event_handler=event_handler, topics=[AppConfig().get('kafka_topics', 'apxdb_transaction')])

    def deserialize(self, message_value: bytes) -> Union[TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent]:
        msg_dict = json.loads(message_value.decode('utf-8'))
        payload = msg_dict['payload']
        before = payload['before']
        after = payload['after']

        # Dates will be in days since 1/1/1970 ... make them datetime dates:
        if isinstance(before, dict):
            for k, v in before.items():
                if 'Date' in k and isinstance(v, int):
                    before[k] = (datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=v))
        if isinstance(after, dict):
            for k, v in after.items():
                if 'Date' in k and isinstance(v, int):
                    after[k] = (datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=v))

        if payload['op'] == 'c':
            return TransactionCreatedEvent(Transaction(**after))

        elif payload['op'] == 'u':
            return TransactionUpdatedEvent(Transaction(**before), Transaction(**after))

        elif payload['op'] == 'd':
            return TransactionDeletedEvent(Transaction(**before))

        else:
            return None  # No event


