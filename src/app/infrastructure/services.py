
from dataclasses import dataclass
import logging

import requests

from domain.models import Alert
from domain.services import AlertService


@dataclass
class MSTeamsAlertService(AlertService):
    webhook_url: str
    
    def send_alert(self, alert: Alert) -> int:
        message = {
            'title': alert.title.replace('\\','\\\\'),
            'text': alert.body.replace('\\','\\\\')
        }
        logging.info(f'Sending Teams alert to {self.webhook_url}:'+'\n\n'+alert.title+'\n\n'+alert.body+'\n')
        response = requests.post(self.webhook_url, json=message)
        
        # Returning 1 means 1 row was "saved", i.e. success
        if response.ok:
            return 1
        else:
            return 0  # TODO_EH: logging? throw exception? 

