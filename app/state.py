class ClientState:
    in_multi = False
    tx_queue = []

class ServerState:
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