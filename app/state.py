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

    def __init__(self):
        self.strings = {}
        self.expirations = {}
        self.lists = {}
        self.streams = {}

        self.waiters = {
            "list": {},
            "stream": {},
        }