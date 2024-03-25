
# core python
import json
from abc import ABC, abstractmethod
import datetime
import logging
import os
from typing import List, Type, Union

# pypi
from confluent_kafka import Consumer, OFFSET_BEGINNING, OFFSET_END

# native
from domain.events import (Event, TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent
    , TransactionCommentCreatedEvent, TransactionCommentUpdatedEvent, TransactionCommentDeletedEvent
)
from domain.event_handlers import EventHandler
from domain.message_brokers import MessageBroker
from domain.message_subscribers import MessageSubscriber
from domain.models import Transaction, TransactionComment
from domain.repositories import HeartbeatRepository

from infrastructure.message_brokers import KafkaBroker
from infrastructure.util.config import AppConfig
from infrastructure.util.logging import get_log_file_name


class DeserializationError(Exception):
    pass



class KafkaMessageConsumer(MessageSubscriber):
    def __init__(self, topics, event_handler, heartbeat_repo: Union[HeartbeatRepository,None]=None):
        super().__init__(message_broker=KafkaBroker(), topics=topics, event_handler=event_handler)
        self.config = dict(self.message_broker.config)
        self.config.update(AppConfig().parser['kafka_consumer'])
        logging.info(f'Creating KafkaMessageConsumer with config: {self.config}')
        self.consumer = Consumer(self.config)
        self.heartbeat_repo = heartbeat_repo

    def consume(self, reset_offset: bool=False):
        
        logging.info(f'Consuming from topics: {self.topics}')

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
                    logging.info("Waiting...")
                    
                    # Save heartbeat
                    if self.heartbeat_repo:
                        # Log file name provides a meaningful name, if app_name is not found
                        app_name = os.environ.get('APP_NAME') or get_log_file_name()
                        if not app_name:
                            # Still not found? Default to class name:
                            app_name = self.cn

                        # Create heartbeat 
                        hb = self.heartbeat_repo.heartbeat_class(group='LW-FA-BLOTTER-TXN-VAL', name=app_name)

                        # If it has a log attribute, populate it with something more meaningful:
                        if hasattr(hb, 'log'):
                            hb.log = f"HEARTBEAT => {self.cn} consuming {', '.join(self.topics)} messages from {self.config['bootstrap.servers']}; using event handler {self.event_handler}"

                        # Now we have the heartbeat ready to save. Save it: 
                        logging.debug(f'About to save heartbeat to {self.heartbeat_repo.cn}: {hb}')
                        res = self.heartbeat_repo.create(hb)

                elif msg.error():
                    logging.info(f"ERROR: {msg.error()}")
                elif msg.value() is not None:
                    # logging.info(f"Consuming message: {msg.value()}")
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
                        # logging.info(f"Handling {event}")
                        should_commit = self.event_handler.handle(event)
                        # logging.info(f"Done handling {event}")
                
                    except Exception as e:
                        if isinstance(e, DeserializationError):
                            logging.info(f'Exception while deserializing: {e}')
                            should_commit = self.event_handler.handle_deserialization_error(e)
                        else:
                            logging.info(e)  # TODO: any more valuable logging?
                    
                    # Commit, unless we should not based on above results
                    if should_commit:
                        self.consumer.commit(message=msg)
                        logging.info("Done committing offset")
                    else:
                        logging.info("Not committing offset, likely due to the most recent exception")


        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            logging.info(f'Committing offset and closing {self.cn}...\n\n\n')
            self.consumer.close()

    def on_assign(self, consumer, partitions):
        if self.reset_offset:
            for p in partitions:
                logging.info(f"Resetting offset for {p}")
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
    def __init__(self, event_handler: EventHandler, heartbeat_repo: Union[HeartbeatRepository,None]=None):
        """ Creates a KafkaMessageConsumer to consume new/changed apxdb transactions/comments with the provided event handler """
        super().__init__(event_handler=event_handler, heartbeat_repo=heartbeat_repo, topics=[AppConfig().get('kafka_topics', 'apxdb_transaction')])

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
            return (
                TransactionCommentCreatedEvent(TransactionComment(**after)) 
                    if after.get('TransactionCode').strip() == ';' 
                    else TransactionCreatedEvent(Transaction(**after))
            )

        elif payload['op'] == 'u':
            return (
                TransactionCommentUpdatedEvent(TransactionComment(**before), TransactionComment(**after))
                    if after.get('TransactionCode').strip() == ';' 
                    else TransactionUpdatedEvent(Transaction(**before), Transaction(**after))
            )

        elif payload['op'] == 'd':
            return (
                TransactionCommentDeletedEvent(TransactionComment(**before)) 
                    if before.get('TransactionCode').strip() == ';' 
                    else TransactionDeletedEvent(Transaction(**before))
            )

        else:
            return None  # No event


