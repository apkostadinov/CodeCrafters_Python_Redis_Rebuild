import socket  # noqa: F401
import threading
import asyncio
from . import parser

# def main():
#     # You can use print statements as follows for debugging, they'll be visible when running tests.
#     print("Logs from your program will appear here!")
#
#     # Uncomment this to pass the first stage
#
#     #server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
#     server_socket = socket.create_server(("localhost", 6379))
#     print("Server created")
#     server_socket.listen()
#     print("Server listening on 127.0.0.1:6379")
#     current_threads = []
#     counter = 0
#
#     while True:
#         connection, address = server_socket.accept()  # wait for client
#         print(f"Connected by {address}")
#         t = threading.Thread(target=connection_thread, args=(connection, address), daemon=True, name=f"Thread {counter}")
#         t.start()
#         current_threads = [t for t in current_threads if t.is_alive()]
#         counter += 1
#         print(f"{t.name} started ({len(current_threads)} active)\n")
#
# def connection_thread(connection, address):
#     with connection:
#         while True:
#             raw_message = connection.recv(1024)
#             if not raw_message:
#                 print(f"Connection closed by client on address {address}")
#                 break
#             message = raw_message.decode("utf-8").strip()
#             print("---------")
#             print(f"Received: {message}\nFrom: {address}\n")
#             for _ in range(message.count("PING")):
#                 response = b"+PONG\r\n"
#                 try:
#                     connection.sendall(response)
#                     print(f'Sent:{response.decode("utf-8")}')
#                     print("---------")
#                 except BrokenPipeError:
#                     print('Connection closed by user')
#                     break

def resp_bulk_string(message: str) -> bytes:
    """Convert a string to RESP bulk string format."""
    return f"${len(message)}\r\n{message}\r\n".encode("utf-8")


async def handle_client(reader, writer):
    address = writer.get_extra_info("peername")
    print(f'Connected by {address}')

    try:
        while True:
            data = await reader.read(1024)
            if data == b'':
                # True disconnect — still break
                print(f"Connection closed by client on address {address}")
                break

            try:
                # parse the RESP message
                message = parser.parser_first(data, 0)
            except RespError as e:
                print(f"Parser error from {address}: {e}")
                # Optionally send an error to the client
                writer.write(b"-ERR invalid message\r\n")
                await writer.drain()
                continue  # keep connection alive

            print('----------------')
            print(f'Received: {data} from {address}\n'
                  f'Decoded: {message}')
            if "PING" in message:
                for _ in range(message.count("PING")):
                    response = b'+PONG\r\n'
                    writer.write(response)
                    await writer.drain()
                    print(f'Sent: {response.decode("utf-8")}')
            if "ECHO" in message:
                # Extract the message to echo
                for i in range(len(message)):
                    if message[i].upper() == "ECHO" and message[i+1]:
                        writer.write(resp_bulk_string(to_echo))
                        await writer.drain()

            print('----------------')

    except ConnectionResetError:
        print(f'Connection forcibly closed by {address}')

    finally:
        writer.close()
        await writer.wait_closed()
        print(f'Connection closed by client on address {address}')

async def main():
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    print("Server created")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
    #main()
