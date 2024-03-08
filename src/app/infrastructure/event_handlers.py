

from app.domain.event_handlers import EventHandler
from app.infrastructure.events import RawAPXTransactionEvent


class RawAPXTransactionEventHandler(EventHandler):
    def handle(self, event: RawAPXTransactionEvent):
        print(f'{self.__class__.__name__} consuming transaction message:')
        print(f'{event.payload}')

