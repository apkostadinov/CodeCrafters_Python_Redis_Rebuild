from typing import Any

class ClientState:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

        self.is_multi = False
        self.tx_queue = []

        # future per-client stuff
        self.blocked_future = None
        self.replication_offset = 0

class ServerState:

    def __init__(self, host=None, port=None, info: dict[dict[str, Any], Any] = None):
        self.strings = {}
        self.expirations = {}
        self.lists = {}
        self.streams = {}

        self.waiters = {
            "list": {},
            "stream": {},
        }

        self.host = host if host else "localhost"
        self.port = port if port else "6379"

        self.info = info

    @property
    def info(self):
        return self._info


    @info.setter
    def info(self, value):
        if not value:
            self._info = {"replication": {"role": "master"}}
        else:
            self._info = value