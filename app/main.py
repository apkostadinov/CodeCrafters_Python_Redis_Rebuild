import socket  # noqa: F401
import asyncio

from app.handlers import *
from app.state import *
from app.services import parser
from app.services.exceptions import *
from app.services.classes import RedisResponse
from app.services.encoder import encode

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
                response = await handle_command(message, state)
                print(response)
                response = encode(response)
                print(response)
                state.writer.write(response)
                await writer.drain()

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
            return RedisResponse("QUEUED", "+")

        elif command == "EXEC":
            state.is_multi = False
            if len(state.tx_queue) > 0:
                response = []
                while len(state.tx_queue)>0:
                    mssg = state.tx_queue.pop(0)
                    rsp = await handle_command(mssg, state)
                    response.append(rsp)
                response = RedisResponse(response, "*")
                return response

            elif len(state.tx_queue) == 0:
                return RedisResponse([], "*")


    if command == "EXEC":
        return RedisResponse("ERR EXEC without MULTI", "-")

    match command:

        case "PING":
            response = await handle_ping(message,state)

        case "ECHO":
            response = await handle_echo(message, state)

        case "SET":
            response = await handle_set(message, state, server)

        case "INCR":
            response = await handle_incr(message, state, server)

        case "MULTI":
            response = await handle_multi(message, state, server)

        case "EXEC":
            response = await handle_exec(message, state, server)

        case "GET":
            response = await handle_get(message, state, server)

        case "RPUSH":
            response = await handle_rpush(message,state, server)

        case "LRANGE":
            response = await handle_lrange(message, state, server)

        case "LPUSH":
            response = await handle_lpush(message, state, server)

        case "LLEN":
            response = await handle_llen(message, state, server)

        case "LPOP":
            response = await handle_lpop(message, state, server)

        case "BLPOP":
            response = await handle_blpop(message, state, server)

        case "TYPE":
            response = await handle_type(message, state, server)

        case "XADD":
            response = await handle_xadd(message, state, server)

        case "XRANGE":
            response = await handle_xrange(message, state, server)

        case "XREAD":
            response = await handle_xread(message, state, server)

        case _:
            raise RespError("Unknown command returns -ERR")

    return response
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
