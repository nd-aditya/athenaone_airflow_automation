from typing import TypedDict

class AlertMessage(TypedDict, total=False):
    alert_type: str
    system_identifier: str
    traceback: str
    dump_identifier: str
    client_identifier: str


class Sender:
    def __init__(self):
        pass

    def send_message(self, message):
        raise NotImplementedError()
        
