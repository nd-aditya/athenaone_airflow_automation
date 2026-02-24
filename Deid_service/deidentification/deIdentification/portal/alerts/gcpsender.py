import requests
import json
from django.conf import settings

from .sender import Sender, AlertMessage
from deIdentification.nd_logger import nd_logger

class GCPSender(Sender):
    def __init__(self):
        self.webhook_url = settings.GCP_ALERT_WEBHOOK

    def send_message(self, alert_message: AlertMessage):
        if ("alert_type" not in alert_message):
            raise Exception("alert_type key not present in alert_message")
        headers = {
            'Content-Type': 'application/json; charset=UTF-8',
        }
        alert_message.update({"system_identifier": settings.SYSTEM_IDENTIFIER})
        formatted_text = ""
        for key, value in alert_message.items():
            formatted_text += f"*{key.replace('_', ' ').title()}*:\n{value}\n\n"
        payload = {
            "text": formatted_text
        }
        
        response = requests.post(self.webhook_url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            nd_logger.info("Message sent successfully.")
        else:
            nd_logger.info(f"Failed to send message. Status: {response.status_code}, Response: {response.text}")
        
