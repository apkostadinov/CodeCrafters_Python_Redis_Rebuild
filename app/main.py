import socket  # noqa: F401
import asyncio
import sys

from app.handlers import *
from app.state import *
from app.services import parser
from app.services.exceptions import *

async def handle_client(reader, writer):
    client = ClientState(reader, writer)
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
                response = await handle_command(message, client)
                print(response)
                client.writer.write(response)
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


async def handle_command(message, client):
    command = message[0]

    if client.is_multi:

        if command == "EXEC":
            client.is_multi = False
            if len(client.tx_queue) > 0:
                response = []
                while len(client.tx_queue)>0:
                    mssg = client.tx_queue.pop(0)
                    rsp = await handle_command(mssg, client)
                    response.append(rsp)
                return parser.encode(response)

            elif len(client.tx_queue) == 0:
                return (b"*0\r\n")

        elif command == "DISCARD":
            client.is_multi = False
            return parser.encode_simple_string("OK")

        else:
            client.tx_queue.append(message)
            print('Sent: "QUEUED"')
            return parser.encode_simple_string("QUEUED")

    if command == "EXEC":
        client.writer.write(b"-ERR EXEC without MULTI\r\n")
        await client.writer.drain()
        return

    elif command == "DISCARD":
        client.writer.write(b"-ERR DISCARD without MULTI\r\n")
        await client.writer.drain()
        return

    match command:

        case "PING":
            response = await handle_ping(message, client)

        case "ECHO":
            response = await handle_echo(message, client)

        case "SET":
            response = await handle_set(message, client, server)

        case "INCR":
            response = await handle_incr(message, client, server)

        case "MULTI":
            response = await handle_multi(message, client, server)

        case "EXEC":
            response = await handle_exec(message, client, server)

        case "GET":
            response = await handle_get(message, client, server)

        case "RPUSH":
            response = await handle_rpush(message, client, server)

        case "LRANGE":
            response = await handle_lrange(message, client, server)

        case "LPUSH":
            response = await handle_lpush(message, client, server)

        case "LLEN":
            response = await handle_llen(message, client, server)

        case "LPOP":
            response = await handle_lpop(message, client, server)

        case "BLPOP":
            response = await handle_blpop(message, client, server)

        case "TYPE":
            response = await handle_type(message, client, server)

        case "XADD":
            response = await handle_xadd(message, client, server)

        case "XRANGE":
            response = await handle_xrange(message, client, server)

        case "XREAD":
            response = await handle_xread(message, client, server)

        case "INFO":
            response = await handle_info(message, server)

        case _:
            raise RespError("Unknown command returns -ERR")

    return response
    print('----------------')

async def main():

    server_process = await asyncio.start_server(handle_client, HOST, PORT)
    print(f"Server created at {HOST}:{PORT}")

    async with server_process:
        await server_process.serve_forever()


if __name__ == "__main__":
    HOST = "localhost"
    PORT = 6379
    INFO = None
    print(sys.argv)
    if "--port" in sys.argv:
        if sys.argv.index("--port") + 1:
            PORT = sys.argv[sys.argv.index("--port") + 1]
        else:
            print(f'Port not specified, defaulting to {PORT}')
    if "--replicaof" in sys.argv:
        INFO = {"replication": {"role": "slave"}}
        master_address = sys.argv[sys.argv.index("--replicaof") + 1]



    server = ServerState(host=HOST, port=PORT, info=INFO)
    asyncio.run(main())
    #main()
