import socket
import app.parser as parser

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
        print(f"Received: "
              f"{data} -> "
              f"{parser.parser_first(data)}")


if __name__ == "__main__":
    print(parser.parser_first(b'*3\r\n$3\r\nSET\r\n$9\r\nblueberry\r\n$9\r\nraspberry\r\n'))
    send_redis_command("PING")
    send_redis_command("PING")
    send_redis_command("ECHO hello")
    send_redis_command("SET fruit apple")
    send_redis_command("GET fruit")
    send_redis_command("GET vegetable")