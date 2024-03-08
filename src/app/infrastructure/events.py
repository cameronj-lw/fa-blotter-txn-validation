
# core python
from dataclasses import dataclass

# native
from app.domain.events import Event


@dataclass
class RawAPXTransactionEvent(Event):
    payload: dict





