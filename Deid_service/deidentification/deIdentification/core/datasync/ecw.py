from .base import ECWSynceConfig

class ECWSyncer:
    def __init__(self, client_connection_string: str, dump_connection_string: str):
        pass

    def start_sync(self, config: ECWSynceConfig, start_date: str, end_date : str):
        raise Exception("Method not implemented")
    