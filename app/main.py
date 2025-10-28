import socket  # noqa: F401
import threading
import asyncio
from datetime import datetime, time, timedelta
from . import parser

def resp_bulk_string(message: str) -> bytes:
    """Convert a string to RESP bulk string format."""
    return f"${len(message)}\r\n{message}\r\n".encode("utf-8")

class RespError(Exception):
    pass

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
                        writer.write(resp_bulk_string(message[i+1]))
                        await writer.drain()
                        print(f'Sent: {message[i+1].decode("utf-8")}')

            if "SET" in message:
                # Extract the key and value to set
                working_dict[message[1]] = message[2]
                if len(message)>3:
                    add_time = None
                    if message[3] == "EX":
                        add_time = datetime.now() + timedelta(seconds=int(message[4]))
                    elif message[3] == "PX":
                        add_time = datetime.now() + timedelta(milliseconds=int(message[4]))
                    if add_time:
                        timing_dict[message[1]] = add_time
                    else:
                        raise RespError("Invalid time format for SET command")
                response = b'+OK\r\n'
                writer.write(response)
                await writer.drain()
                print(f'SET - Key: {message[1]} Value: {message[2]}\n'
                      f'Sent: {response}')

            if "GET" in message:
                value = b'$-1\r\n'
                key = message[1]
                if key in working_dict.keys() and key in timing_dict.keys():
                    if timing_dict[key] > datetime.now():
                        value = working_dict[key]
                        print(f'Value found: {value}')
                        value = resp_bulk_string(value)
                elif key in working_dict.keys():
                    value = working_dict[key]
                    print(f'Value found: {value}')
                    value = resp_bulk_string(value)
                writer.write(value)
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
    working_dict = dict()
    timing_dict = dict()
    asyncio.run(main())
    #main()
