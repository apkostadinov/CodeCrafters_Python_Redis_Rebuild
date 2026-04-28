import socket  # noqa: F401
import threading
import asyncio
from datetime import datetime, time, timedelta
from . import parser

def resp_bulk_string(message: str) -> bytes:
    """Convert a string to RESP bulk string format."""
    if len(message) >= 1:
        return f"${len(message)}\r\n{message}\r\n".encode("utf-8")
    else:
        return f'$""\r\n'.encode("utf-8")

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

            command = message[0]

            match command:

                case "PING":
                    for _ in range(message.count("PING")):
                        response = b'+PONG\r\n'
                        writer.write(response)
                        await writer.drain()
                        print(f'Sent: {response.decode("utf-8")}')

                case "ECHO":
                    # Extract the message to echo
                    print (message)
                    for i in range(len(message)):
                        if message[i].upper() == "ECHO" and message[i+1]:
                            print(message[i+1])
                            writer.write(resp_bulk_string(message[i+1]))
                            await writer.drain()
                            print(f'Sent: {message[i+1]}')
                        else:
                            break
                        # if message[i+1] == '':
                        #     writer.write(b"+''\r\n")
                        #     await writer.drain()
                    else:
                        writer.write(b'+""\r\n')
                        await writer.drain()

                case "SET":
                    # Extract the key and value to set
                    if message[1] and message[2]:
                        working_dict[message[1]] = message[2]
                    else:
                        raise RespError("Invalid format for SET command")
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
                    print(f'SET - Key: {message[1]} Value: {working_dict[message[1]]}\n'
                          f'Sent: {response}')

                case "GET":
                    value = b'$-1\r\n'
                    if message[1]:
                        key = message[1]
                    else:
                        key = None
                        print(f'Value not in dictionary')
                    if key and key in timing_dict.keys() and timing_dict[key] < datetime.now():
                        working_dict.pop(key)
                        timing_dict.pop(key)
                        print(f'Value not found')

                    if key in working_dict.keys():
                        value = working_dict[key]
                        print(f'Value found: {value}')
                        if isinstance(value, list) and len(value)>1:
                            value = parser.encode_array(value)
                        else:
                            value = resp_bulk_string(value)
                    writer.write(value)
                    await writer.drain()

                case "RPUSH":
                    key = message[1]
                    if key in working_dict:
                        if not isinstance(working_dict[key], list):
                            working_dict[key] = [working_dict[key]]
                        for i in message[2:]:
                            working_dict[key].append(i)
                    else:
                        working_dict[key] = message[2:]
                    writer.write(parser.encode_integer(len(working_dict[key])))
                    await writer.drain()

                case "LRANGE":
                    key = message[1]
                    try:
                        start = int(message[2])
                    except ValueError:
                        raise ValueError("Start index is not an integer")

                    try:
                        stop = int(message[3])
                    except:
                        raise ValueError("Stop index is not an integer")

                    try:
                        working_list = working_dict[key]
                    except KeyError:
                        print("LRANGE: Key not found")
                        writer.write(b"*0\r\n")
                        continue

                    if start < 0:
                        start = len(working_list) + start
                        if start < 0:
                            start = 0

                    if stop < 0:
                        stop = len(working_list) + stop

                    if stop > len(working_list):
                        stop = len(working_list)-1

                    if not any([start >= len(working_list), start > stop]):
                        returnable = []
                        # check if value is list, if it's not it should a one word string
                        if isinstance(working_list, list):
                            for i in range(start, stop+1):
                                returnable.append(working_list[i])
                            writer.write(parser.encode_array(returnable))
                        else:
                            writer.write(parser.encode_bulk_string(working_list))
                    else:
                        writer.write(b'*0\r\n')
                    await writer.drain()

                case "LPUSH":
                    key = message[1]
                    if key in working_dict:
                        if not isinstance(working_dict[key], list):
                            working_dict[key] = [working_dict[key]]
                        for i in message[2:]:
                            working_dict[key].insert(0,i)
                    else:
                        working_dict[key] = message[len(message):1:-1]
                    print(working_dict[key])
                    writer.write(parser.encode_integer(len(working_dict[key])))
                    await writer.drain()

                case "LLEN":
                    if message[1]:
                        key = message[1]
                    else:
                        key=None
                        writer.write(b":0\r\n")

                    try:
                        writer.write(parser.encode_integer(len(working_dict[key])))
                        print("LLEN: Key not found")
                    except KeyError:
                        writer.write(b":0\r\n")
                    await writer.drain()

                case "LPOP":
                    if message[1]:
                        key = message[1]
                    try:
                        writer.write(working_dict[key].pop(0))
                    except KeyError:
                        writer.write(b"$-1\r\n")
                    await writer.drain()

                case _:
                    raise RespError("Unknown command returns -ERR")
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
