import socket

HOST = 'localhost'  # server address
PORT = 6379         # server port

def encode_command(*parts: str) -> bytes:
    out = f"*{len(parts)}\r\n".encode("utf-8")
    for part in parts:
        if isinstance(part, str):
            payload = part.encode("utf-8")
            out += f"${len(payload)}\r\n".encode("utf-8")
            out += payload + b"\r\n"
            continue
        else:
            payload = part
            out += f':{payload}\r\n'.encode("utf-8")
    return out

def send_redis_command(*parts: str):
    resp = encode_command(*parts)
    with socket.create_connection((HOST, PORT)) as sock:
        print(f"Sending: {resp}")
        sock.sendall(resp)
        data = sock.recv(1024)
        print(f"Received raw: {data}")
        #print(f"Received: {data} -> {parser.parser_first(data)}")