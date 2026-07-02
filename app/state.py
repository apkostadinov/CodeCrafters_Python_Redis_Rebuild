from typing import Any
from app.services.utils import generate_master_id

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

    def __init__(self, host=None, port=None, role=None, master_id=None, offset=None):
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

        self.role = role
        self.master_id = master_id if master_id else generate_master_id()
        self.offset = offset
        self.info = {}



    @property
    def role(self):
        return self._role

    @role.setter
    def role(self, value):
        if value in ["master", "slave"]:
            self._role = value
        elif value is None:
            self._role = "master"
        else:
            raise ValueError(f"Invalid role {value}")

    @property
    def master_id(self):
        return self._master_id


    @master_id.setter
    def master_id(self, value):
        if self.role == "master":
            self._master_id = generate_master_id()
        else:
            self._master_id = value

    @property
    def offset(self):
        return self._offset
    @offset.setter
    def offset(self, value=None):
        if value and not isinstance(value, int):
            raise ValueError("Invalid offset value")

        if value:
            self._offset = value
        else:
            self._offset = 0

    @property
    def info(self):
        return self._info

    @info.setter
    def info(self, value):
        self._info = {"replication": {
                        "role": f"{self.role}",
                        "master_replid" : f"{self.master_id}",
                        "master_repl_offset": f"{self.offset}"}}