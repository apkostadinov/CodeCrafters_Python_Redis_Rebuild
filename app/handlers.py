import asyncio
from services import parser
from services.exceptions import *
from services.streams import *
from datetime import datetime, timedelta

async def handle_ping(message,state):
    for _ in range(message.count("PING")):
        response = b'+PONG\r\n'
        state.writer.write(response)
        await state.writer.drain()
        print(f'Sent: {response.decode("utf-8")}')

async def handle_echo(message, state):
    # Extract the message to echo
    print(message)
    for i in range(len(message)):
        if message[i].upper() == "ECHO" and message[i + 1]:
            print(message[i + 1])
            state.writer.write(parser.encode_bulk_string(message[i + 1]))
            await state.writer.drain()
            print(f'Sent: {message[i + 1]}')
        else:
            break

    else:
        state.writer.write(b'+""\r\n')
        await state.writer.drain()

async def handle_set(message, state, server):
    # Extract the key and value to set
    if message[1] and message[2]:
        server.strings[message[1]] = message[2]
    else:
        raise RespError("Invalid format for SET command")
    if len(message) > 3:
        add_time = None
        if message[3] == "EX":
            add_time = datetime.now() + timedelta(seconds=int(message[4]))
        elif message[3] == "PX":
            add_time = datetime.now() + timedelta(milliseconds=int(message[4]))
        if add_time:
            server.expirations[message[1]] = add_time
        else:
            raise RespError("Invalid time format for SET command")

    response = b'+OK\r\n'
    state.writer.write(response)
    await state.writer.drain()
    print(f'SET - Key: {message[1]} Value: {server.strings[message[1]]}\n'
          f'Sent: {response}')

async def handle_incr(message, state, server):
    key = message[1]
    if key in server.strings.keys():
        try:
            value = int(server.strings[key]) + 1
            server.strings[key] = str(value)
        except Exception as e:
            response = parser.encode_simple_error("ERR value is not an integer or out of range")
            state.writer.write(response)
            await state.writer.drain()
            return
    else:
        value = 1
        server.strings[key] = "1"

    response = parser.encode_integer(value) if value else None

    if response:
        state.writer.write(response)
    else:
        pass

    await state.writer.drain()

async def handle_multi(message, state, server):
    state.is_multi = True
    state.writer.write(parser.encode_bulk_string("OK"))
    await state.writer.drain()

async def handle_exec(message, state, server):
    #TODO
    pass

async def handle_get(message, state, server):
    value = b'$-1\r\n'
    if message[1]:
        key = message[1]
    else:
        key = None
        print(f'Value not in dictionary')
    if key and key in server.expirations.keys() and server.expirations[key] < datetime.now():
        server.strings.pop(key)
        server.expirations.pop(key)
        print(f'Value not found')

    if key in server.strings.keys():
        value = server.strings[key]
        print(f'Value found: {value}')
        if isinstance(value, list) and len(value) > 1:
            value = parser.encode_array(value)
        else:
            value = parser.encode_bulk_string(value)
    else:
        value = b'$-1\r\n'
    state.writer.write(value)
    await state.writer.drain()

async def handle_rpush(message, state, server):
    key = message[1]
    values = message[2:]

    count_added = 0

    for value in values:
        if key in server.waiters["list"] and server.waiters["list"][key]:
            future = server.waiters["list"][key].pop(0)
            if not future.done():
                future.set_result(value)
            count_added += 1
        else:
            server.lists.setdefault(key, []).append(value)
            count_added += 1

    current_len = len(server.lists.get(key, [])) + (
        0 if key not in server.waiters["list"] else 0
    )

    state.writer.write(parser.encode_integer(current_len if current_len else count_added))
    await state.writer.drain()

async def handle_lrange(message, state, server):
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
        working_list = server.lists[key]
    except KeyError:
        print("LRANGE: Key not found")
        server.writer.write(b"*0\r\n")
        return

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
            server.writer.write(parser.encode_array(returnable))
        else:
            server.writer.write(parser.encode_bulk_string(working_list))
    else:
        state.writer.write(b'*0\r\n')
    await state.writer.drain()

async def handle_lpush(message, state, server):
    key = message[1]

    if key in server.lists.keys():
        if not isinstance(server.lists[key], list):
            server.lists[key] = [server.lists[key]]
        for i in message[2:]:
            server.lists[key].insert(0, i)
    else:
        server.lists[key] = message[len(message):1:-1]

    if key in server.waiters["list"] and server.waiters["list"][key]:
        future = server.waiters["list"][key].pop(0)
        if not future.done():
            value = server.lists[key].pop(0)
            future.set_result(value)

    print(server.lists[key])
    state.writer.write(parser.encode_integer(len(server.lists[key])))
    await state.writer.drain()

async def handle_llen(message, state, server):
    if message[1]:
        key = message[1]
    else:
        key = None
        state.writer.write(b":0\r\n")

    try:
        state.writer.write(parser.encode_integer(len(server.lists[key])))
        print("LLEN: Key not found")
    except KeyError:
        state.writer.write(b":0\r\n")
    await state.writer.drain()

async def handle_lpop(message, state, server):
    if message[1]:
        key = message[1]
        print(server.lists[key])
    if len(message) > 2:
        try:
            count = int(message[2])
        except ValueError:
            state.writer.write(b"$-1\r\n")
            await state.writer.drain()

    else:
        count = 1

    if key not in server.lists or len(server.lists[key]) == 0:
        state.writer.write(b"$-1\r\n")
        await state.writer.drain()

    if count > 1:
        returnable = []
        for _ in range(count):
            value = server.lists[key].pop(0)
            print(value)
            print(server.lists[key])
            returnable.append(value)
    elif count == 1:
        returnable = server.lists[key].pop(0)

    state.writer.write(parser.encode(returnable))
    await state.writer.drain()

async def handle_blpop(message, state, server):
    key = message[1]
    timeout = float(message[2]) if len(message) > 2 else 0

    if key in server.lists and len(server.lists[key]) > 0:
        value = server.lists[key].pop(0)
        response = parser.encode_array([key, value])
        state.writer.write(response)
        await state.writer.drain()
        return

    loop = asyncio.get_event_loop()
    future = loop.create_future()

    server.waiters["list"].setdefault(key, []).append(future)

    try:
        if timeout == 0:
            value = await future
        else:
            value = await asyncio.wait_for(future, timeout)
        state.writer.write(parser.encode_array([key, value]))
        await state.writer.drain()

    except asyncio.TimeoutError:
        server.waiters["list"][key].remove(future)
        state.writer.write(b"*-1\r\n")
        await state.writer.drain()

async def handle_type(message, state, server):
    key = message[1]
    if key in server.strings.keys():
        response = parser.encode_simple_string("string")
    elif key in server.lists.keys():
        response = parser.encode_simple_string("list")
    elif key in server.streams.keys():
        response = parser.encode_simple_string("stream")
    else:
        response = parser.encode_simple_string("none")

    state.writer.write(response)
    await state.writer.drain()

async def handle_xadd(message, state, server):
    key = message[1]
    main_id = message[2]
    temp_dict = dict()
    response = None

    for i in range(3, len(message), 2):
        temp_dict[message[i]] = message[i + 1]

    main_id_ms, main_id_sq = deconstruct_stream_id(main_id)

    if (main_id_ms, main_id_sq) == ('0', '0'):
        response = parser.encode_simple_error("ERR The ID specified in XADD must be greater than 0-0")

    if key in server.streams.keys():
        stream = server.streams[key][-1]
    else:
        stream = None

    if "*" in main_id_ms or "*" in main_id_sq:
        main_id_ms, main_id_sq = generate_id_sq(main_id_ms, main_id_sq, stream)
        main_id = "-".join([str(main_id_ms), str(main_id_sq)])

    if stream and response is None:

        if validate_stream_id(main_id_ms, main_id_sq, stream):
            server.streams[key].append({"xid": main_id} | temp_dict)
            response = parser.encode_bulk_string(main_id)
        else:
            response = parser.encode_simple_error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item")

    if validate_stream_id(main_id_ms, main_id_sq) and response is None:
        server.streams[key] = [{"xid": main_id} | temp_dict]
        response = parser.encode_bulk_string(main_id)

    if key in server.waiters["stream"] and server.waiters["stream"][key]:
        future = server.waiters["stream"][key].pop(0)
        if not future.done():
            value = server.streams[key]
            future.set_result(value)

    state.writer.write(response)
    await state.writer.drain()

async def handle_xrange(message, state, server):
    key = message[1] if message[1] else None
    start = message[2]
    end = message[3]
    collection = list()

    stream = deepcopy(server.streams.get(key, None))

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

    state.writer.write(parser.encode_array(collection))
    await state.writer.drain()

async def handle_xread(message, state, server):
    pairs = dict()

    if message[1].upper() == "STREAMS":
        unprocessed = deepcopy(message[2:])
        for i in range(len(unprocessed) // 2):
            value_index = (len(unprocessed) // 2) + i
            pairs[unprocessed[i]] = unprocessed[value_index]

        collection = xread_extraction(server.streams, pairs)

    elif message[1].upper() == "BLOCK":

        key = message[4]
        if message[5] and message[5] == "$":
            item = server.streams.get(key, None)
            if item:
                xid = item[-1]['xid']
            else:
                xid = "0-0"
        else:
            xid = message[5]
        pairs[key] = xid
        timeout_ms = int(message[2]) / 1000

        collection = xread_extraction(server.streams, pairs)

        if not collection:

            loop = asyncio.get_event_loop()
            future = loop.create_future()

            server.waiters["stream"].setdefault(key, []).append(future)

            try:
                if timeout_ms == 0:
                    await future
                else:
                    await asyncio.wait_for(future, timeout_ms)
                # streams[key] = value
                collection = xread_extraction(server.streams, pairs)

            except asyncio.TimeoutError:
                server.waiters["stream"][key].remove(future)
                state.writer.write(b"*-1\r\n")
                await state.writer.drain()
                return

    else:
        raise RespError("Malformed command.")

    if collection:
        response = parser.encode_array(collection)
    else:
        response = b"*-1\r\n"

    print(response)

    state.writer.write(response)
    await state.writer.drain()