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

class Server:
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
        return {"replication": {
            "role": f"{self.role}",
            "master_replid": f"{self.master_id}",
            "master_repl_offset": f"{self.offset}"}}

class ServerState(Server):

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
        self.repl_id = generate_master_id()
        self.offset = offset

        self.slaves = []
        self.master_id = master_id if master_id else None

        self.master_host = None
        self.master_port = None
        self.master_reader = None
        self.master_writer = None

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
        if self.role == "slave":
            self._master_id = value
        else:
            self._master_id = None

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

class SlavedServer(Server):
    def __init__(self, host, port, reader, writer):
        self.host= host
        self.port = port
        self.reader = reader
        self.writer = writer

    def __repr__(self):
        return f"SlavedServer(address={self.address}, port={self.port})"