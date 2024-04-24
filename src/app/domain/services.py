
from abc import ABC, abstractmethod

from domain.models import Alert

class AlertService(ABC):

    @abstractmethod
    def send_alert(self, alert: Alert) -> int:
        pass
    