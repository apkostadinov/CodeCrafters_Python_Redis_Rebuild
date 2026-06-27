import socket  # noqa: F401
import asyncio

from app.handlers import *
from app.state import *
from app.services import parser
from app.services.exceptions import *

async def handle_client(reader, writer):
    state = ClientState(reader, writer)
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
                    print(f'Received: {data} from {address}\n'
                          f'Decoded: {message}')
                except parser.IncompleteMessage:
                    break

                buffer = buffer[consumed:]
                await handle_command(message, state)

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

    except ConnectionResetError:
        print(f'Connection forcibly closed by {address}')

    finally:
        writer.close()
        await writer.wait_closed()
        print(f'Connection closed by client on address {address}')


async def handle_command(message, state):
    command = message[0]

    if state.is_multi:
        if command != "EXEC":
            state.tx_queue.append(message)
            print('Sent: "QUEUED"')
            state.writer.write(parser.encode_simple_string("QUEUED"))
            await state.writer.drain()
            return

        elif command == "EXEC":
            state.is_multi = False
            if len(state.tx_queue) > 0:
                while len(state.tx_queue)>0:
                    mssg = state.tx_queue.pop(0)
                    await handle_command(mssg, state)
                return

            elif len(state.tx_queue) == 0:
                state.writer.write(b"*0\r\n")
                await state.writer.drain()
                return

    if command == "EXEC":
        state.writer.write(b"-ERR EXEC without MULTI\r\n")
        await state.writer.drain()
        return

    match command:

        case "PING":
            await handle_ping(message,state)

        case "ECHO":
            await handle_echo(message, state)

        case "SET":
            await handle_set(message, state, server)

        case "INCR":
            await handle_incr(message, state, server)

        case "MULTI":
            await handle_multi(message, state, server)
        case "EXEC":
            await handle_exec(message, state, server)

        case "GET":
            await handle_get(message, state, server)

        case "RPUSH":
            await handle_rpush(message,state, server)

        case "LRANGE":
            await handle_lrange(message, state, server)

        case "LPUSH":
            await handle_lpush(message, state, server)

        case "LLEN":
            await handle_llen(message, state, server)

        case "LPOP":
            await handle_lpop(message, state, server)

        case "BLPOP":
            await handle_blpop(message, state, server)

        case "TYPE":
            await handle_type(message, state, server)

        case "XADD":
            await handle_xadd(message, state, server)

        case "XRANGE":
            await handle_xrange(message, state, server)

        case "XREAD":
            await handle_xread(message, state, server)

        case _:
            raise RespError("Unknown command returns -ERR")
    print('----------------')

async def main():

    server_process = await asyncio.start_server(handle_client, "localhost", 6379)
    print("Server created at localhost:6379")

    async with server_process:
        await server_process.serve_forever()


if __name__ == "__main__":
    server = ServerState()
    asyncio.run(main())
    #main()
