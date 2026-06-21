import socket  # noqa: F401
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta
from operator import neg

from .services import parser
from .services.streams import *
from .services.exceptions import *


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

    buffer = b""

    try:
        while True:
            data = await reader.read(1024)
            buffer += data
            while True:
                try:
                    message, consumed = parser.parser(buffer, 0)
                except parser.IncompleteMessage:
                    break

                buffer = buffer[consumed:]
                await handle_command(message, writer)

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


    except ConnectionResetError:
        print(f'Connection forcibly closed by {address}')

    finally:
        writer.close()
        await writer.wait_closed()
        print(f'Connection closed by client on address {address}')

async def handle_command(message, writer):
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
            print(message)
            for i in range(len(message)):
                if message[i].upper() == "ECHO" and message[i + 1]:
                    print(message[i + 1])
                    writer.write(resp_bulk_string(message[i + 1]))
                    await writer.drain()
                    print(f'Sent: {message[i + 1]}')
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
                str_dict[message[1]] = message[2]
            else:
                raise RespError("Invalid format for SET command")
            if len(message) > 3:
                add_time = None
                if message[3] == "EX":
                    add_time = datetime.now() + timedelta(seconds=int(message[4]))
                elif message[3] == "PX":
                    add_time = datetime.now() + timedelta(milliseconds=int(message[4]))
                if add_time:
                    str_timing_dict[message[1]] = add_time
                else:
                    raise RespError("Invalid time format for SET command")

            response = b'+OK\r\n'
            writer.write(response)
            await writer.drain()
            print(f'SET - Key: {message[1]} Value: {str_dict[message[1]]}\n'
                  f'Sent: {response}')

        case "GET":
            value = b'$-1\r\n'
            if message[1]:
                key = message[1]
            else:
                key = None
                print(f'Value not in dictionary')
            if key and key in str_timing_dict.keys() and str_timing_dict[key] < datetime.now():
                str_dict.pop(key)
                str_timing_dict.pop(key)
                print(f'Value not found')

            if key in str_dict.keys():
                value = str_dict[key]
                print(f'Value found: {value}')
                if isinstance(value, list) and len(value) > 1:
                    value = parser.encode_array(value)
                else:
                    value = resp_bulk_string(value)
            else:
                value = b'$-1\r\n'
            writer.write(value)
            await writer.drain()

        case "RPUSH":
            key = message[1]
            values = message[2:]

            count_added = 0

            for value in values:
                if key in waiting_clients and waiting_clients[key]:
                    future = waiting_clients[key].pop(0)
                    if not future.done():
                        future.set_result(value)
                    count_added += 1
                else:
                    working_dict.setdefault(key, []).append(value)
                    count_added += 1

            current_len = len(working_dict.get(key, [])) + (
                0 if key not in waiting_clients else 0
            )

            writer.write(parser.encode_integer(current_len if current_len else count_added))
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

            if start < 0:
                start = len(working_list) + start
                if start < 0:
                    start = 0

            if stop < 0:
                stop = len(working_list) + stop

            if stop > len(working_list):
                stop = len(working_list) - 1

            if not any([start >= len(working_list), start > stop]):
                returnable = []
                # check if value is list, if it's not it should a one word string
                if isinstance(working_list, list):
                    for i in range(start, stop + 1):
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
                    working_dict[key].insert(0, i)
            else:
                working_dict[key] = message[len(message):1:-1]

            if key in waiting_clients and waiting_clients[key]:
                future = waiting_clients[key].pop(0)
                if not future.done():
                    value = working_dict[key].pop(0)
                    future.set_result(value)

            print(working_dict[key])
            writer.write(parser.encode_integer(len(working_dict[key])))
            await writer.drain()

        case "LLEN":
            if message[1]:
                key = message[1]
            else:
                key = None
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
                print(working_dict[key])
            if len(message) > 2:
                try:
                    count = int(message[2])
                except ValueError:
                    writer.write(b"$-1\r\n")
                    await writer.drain()

            else:
                count = 1

            if key not in working_dict or len(working_dict[key]) == 0:
                writer.write(b"$-1\r\n")
                await writer.drain()

            if count > 1:
                returnable = []
                for _ in range(count):
                    value = working_dict[key].pop(0)
                    print(value)
                    print(working_dict[key])
                    returnable.append(value)
            elif count == 1:
                returnable = working_dict[key].pop(0)

            writer.write(parser.encode(returnable))
            await writer.drain()

        case "BLPOP":
            key = message[1]
            timeout = float(message[2]) if len(message) > 2 else 0

            if key in working_dict and len(working_dict[key]) > 0:
                value = working_dict[key].pop(0)
                response = parser.encode_array([key, value])
                writer.write(response)
                await writer.drain()
                return

            loop = asyncio.get_event_loop()
            future = loop.create_future()

            waiting_clients.setdefault(key, []).append(future)

            try:
                if timeout == 0:
                    value = await future
                else:
                    value = await asyncio.wait_for(future, timeout)
                writer.write(parser.encode_array([key, value]))
                await writer.drain()

            except asyncio.TimeoutError:
                waiting_clients[key].remove(future)
                writer.write(b"*-1\r\n")
                await writer.drain()

        case "TYPE":
            key = message[1]
            if key in str_dict.keys():
                response = parser.encode_simple_string("string")
            elif key in working_dict.keys():
                response = parser.encode_simple_string("list")
            elif key in streams.keys():
                response = parser.encode_simple_string("stream")
            else:
                response = parser.encode_simple_string("none")

            writer.write(response)
            await writer.drain()

        case "XADD":
            key = message[1]
            main_id = message[2]
            temp_dict = dict()
            response = None

            for i in range(3, len(message), 2):
                temp_dict[message[i]] = message[i + 1]

            main_id_ms, main_id_sq = deconstruct_stream_id(main_id)

            if (main_id_ms, main_id_sq) == ('0', '0'):
                response = parser.encode_simple_error("ERR The ID specified in XADD must be greater than 0-0")

            if key in streams.keys():
                stream = streams[key][-1]
            else:
                stream = None

            if "*" in main_id_ms or "*" in main_id_sq:
                main_id_ms, main_id_sq = generate_id_sq(main_id_ms, main_id_sq, stream)
                main_id = "-".join([str(main_id_ms), str(main_id_sq)])

            if stream and response is None:

                if validate_stream_id(main_id_ms, main_id_sq, stream):
                    streams[key].append({"xid": main_id} | temp_dict)
                    response = parser.encode_bulk_string(main_id)
                else:
                    response = parser.encode_simple_error(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item")

            # if "*" in main_id_ms or "*" in main_id_sq:
            #     main_id_ms, main_id_sq = generate_id_sq(main_id_ms, main_id_sq)
            #     main_id = "-".join([str(main_id_ms), str(main_id_sq)])

            if validate_stream_id(main_id_ms, main_id_sq) and response is None:
                streams[key] = [{"xid":main_id} | temp_dict]
                response = parser.encode_bulk_string(main_id)

            print(streams)

            if key in waiting_clients and waiting_clients[key]:
                future = waiting_clients[key].pop(0)
                if not future.done():
                    value = streams[key]
                    future.set_result(value)

            writer.write(response)
            await writer.drain()

        case "XRANGE":
            key = message[1] if message[1] else None
            start = message[2]
            end = message[3]
            collection = list()

            stream = deepcopy(streams.get(key, None))

            if not stream:
                raise StreamNotFound("")

            if "-" in start and start != "-":
                start_ms, start_sq = (int(x) for x in deconstruct_stream_id(start))
            else:
                if start == "-":
                    start_ms, start_sq = 0, 0
                else:
                    start_ms, start_sq = int(start), None


            if "-" in end:
                end_ms, end_sq = (int(x) for x in deconstruct_stream_id(end))
            elif end == "+":
                end_ms, end_sq = (int(x) for x in deconstruct_stream_id(stream[-1]["xid"]))
            else:
                end_ms, end_sq = int(end), None


            for item in stream:
                item_ms, item_sq = (int(x) for x in deconstruct_stream_id(item["xid"]))

                if item_ms < start_ms or item_ms > end_ms:
                    continue

                if start_sq and item_sq < start_sq:
                    continue

                if end_sq and item_sq > end_sq:
                    continue

                item_id = item.pop("xid")
                temp_list = list()

                for key in item:
                    temp_list.append(key)
                    temp_list.append(str(item[key]))

                collection.append([item_id, temp_list])

            print(collection)

            writer.write(parser.encode_array(collection))
            await writer.drain()

        case "XREAD":
            pairs = dict()

            if message[1].upper() == "STREAMS":
                unprocessed = deepcopy(message[2:])
                for i in range(len(unprocessed) // 2):
                    value_index = (len(unprocessed) // 2) + i
                    pairs[unprocessed[i]] = unprocessed[value_index]

                collection = xread_extraction(streams, pairs)

            elif message[1].upper() == "BLOCK":

                key = message[4]
                pairs[key] = message[5]
                timeout_ms = int(message[2])

                collection = xread_extraction(streams, pairs)

                if not collection:

                    loop = asyncio.get_event_loop()
                    future = loop.create_future()

                    waiting_clients.setdefault(key, []).append(future)

                    try:
                        if timeout_ms == 0:
                            value = await future
                        else:
                            value = await asyncio.wait_for(future, timeout_ms)
                        streams[key] = value

                    except asyncio.TimeoutError:
                        waiting_clients[key].remove(future)
                        writer.write(b"*-1\r\n")
                        await writer.drain()
                        return

            else:
                raise RespError("Malformed command.")


            # collection = list()
            #
            # for key in pairs.keys():
            #     stream = deepcopy(streams.get(key, None))
            #     if not streams:
            #         raise StreamNotFound()
            #
            #     start = pairs[key]
            #     if "-" in start:
            #         start_ms, start_sq = (int(x) for x in deconstruct_stream_id(start))
            #     else:
            #         start_ms, start_sq = int(start), None
            #
            #     for item in stream:
            #         item_ms, item_sq = (int(x) for x in deconstruct_stream_id(item["xid"]))
            #
            #         if item_ms < start_ms:
            #             continue
            #
            #         if start_sq and item_sq <= start_sq:
            #             continue
            #
            #         item_id = item.pop("xid")
            #         temp_list = list()
            #
            #         for i in item:
            #             temp_list.append(i)
            #             temp_list.append(str(item[i]))
            #
            #         collection.append([key,[[item_id, temp_list]]])


            response = parser.encode_array(collection)
            print(response)

            writer.write(response)
            await writer.drain()
            return

        case _:
            raise RespError("Unknown command returns -ERR")
    print('----------------')

async def main():
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    print("Server created")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    str_dict = dict()
    str_timing_dict = dict()
    working_dict = dict()
    waiting_clients = dict()
    streams = dict()
    asyncio.run(main())
    #main()
