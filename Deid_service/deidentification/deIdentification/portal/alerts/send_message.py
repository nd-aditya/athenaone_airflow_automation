from django.conf import settings
from typing import Union
from .gcpsender import GCPSender

SenderType = Union[GCPSender]

class SendMessage:
    def __init__(self):
        self.sender: SenderType = GCPSender()
    
    def send_message(self, message):
        if settings.ENABLE_FAILURE_ALERT:
            self.sender.send_message(message)
        else:
            pass
    


alert_sender = SendMessage()