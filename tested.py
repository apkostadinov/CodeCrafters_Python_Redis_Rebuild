import socket

HOST = '127.0.0.1'  # server address
PORT = 6379         # server port

def send_redis_command(command: str):
    # Convert a simple string command into RESP bulk string
    # Example: "PING" → "$4\r\nPING\r\n"
    resp = f"${len(command)}\r\n{command}\r\n".encode("utf-8")

    with socket.create_connection((HOST, PORT)) as sock:
        print(f"Sending: {resp}")
        sock.sendall(resp)

        # Receive response
        data = sock.recv(1024)
        print(f"Received: {data.decode('utf-8')}")

if __name__ == "__main__":
    send_redis_command("PING")
    send_redis_command("PING")
    send_redis_command("ECHO hello")