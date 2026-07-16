import socket
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

                if message[0] in ["SET", "INCR"]:
                    for slave in server.slaves:
                        print(f"Sending {data} to {slave.host}:{slave.port}")
                        slave.writer.write(data)
                        await slave.writer.drain()


                buffer = buffer[consumed:]
                response = await handle_command(message, client)
                print(response)
                if response is not None:
                    client.writer.write(response)
                    await writer.drain()
                else:
                    pass

            if data == b'':
                # True disconnect — still break
                #print(f"Connection closed by client on address {address}")
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

        case "REPLCONF":
            response = await handle_replconf(message, server, client)

        case "FULLRESYNC":
            response = await handle_fullresync(message, server, client)

        case _:
            raise RespError("Unknown command returns -ERR")

    return response
    print('----------------')

async def replica_loop():
    master_address = sys.argv[sys.argv.index("--replicaof") + 1]
    master_host, master_port = master_address.split(' ')
    reader, writer = await asyncio.open_connection(
        master_host,
        master_port,
    )

    writer.write(parser.encode(["PING"]))
    await writer.drain()

    data = await reader.read(1024)
    print(data,"decoded: " ,parser.parse_simple_string(data, 0))

    if parser.parse_simple_string(data, 0)[0] == "PONG":
        writer.write(parser.encode(["REPLCONF", "listening-port",server.port]))
    else:
        print(f'Server did not respond with PONG')
        return

    data = await reader.read(1024)
    print(data)

    if parser.parse_simple_string(data, 0)[0] == "OK":
        writer.write(parser.encode(["REPLCONF", "capa", "psync2"]))

    else:
        raise RespError(f"Master Server responded {data} instead of OK")

    data = await reader.read(1024)
    print(data)

    if parser.parse_simple_string(data, 0)[0] == "OK":
        writer.write(parser.encode(["PSYNC","?","-1"]))

    else:
        raise RespError(f"Master Server responded {data} instead of OK")

    data = await reader.read(1024)
    print(data)

    new_message = parser.parse_simple_string(data, 0)[0].split(" ")
    print(new_message)
    if  new_message[0]== "FULLRESYNC":
        server.master_id = new_message[1]
        server.offset = int(new_message[2])
        server.master_host = master_host
        server.master_port = master_port
        server.master_reader = reader
        server.master_writer = writer
        rdb_file = await reader.read(1024)
        print(f"Handshake with master complete.")
        print(f"Slaved to {master_host}:{master_port}\n"
              f"with master_id {server.master_id}")
        print(rdb_file)
    else:
        raise RespError(f"Master Server responded {data}")

    buffer = b''

    master = ClientState(reader, writer)

    try:
        while True:
            data = await master.reader.read(1024)
            buffer += data

            if data == b'':
                # True disconnect — still break
                # print(f"Connection closed by client on address {address}")
                break

            while True:
                try:
                    message, consumed = parser.parser(buffer, 0)
                    print(f'Received: {data} from {master_host}:{master_port}\n'
                          f'Decoded: {message}')
                except parser.IncompleteMessage:
                    break

                # if message[0] in ["SET", "INCR"]:
                #     for slave in server.slaves:
                #         print(f"Sending {data} to {slave.host}:{slave.port}")
                #         slave.writer.write(data)
                #         await slave.writer.drain()

                buffer = buffer[consumed:]
                response = await handle_command(message, master)
                print(response)

            try:
                # parse the RESP message
                message = parser.parser_first(data, 0)
            except RespError as e:
                print(f"Parser error from {address}: {e}")
                # Optionally send an error to the client
                master.writer.write(b"-ERR invalid message\r\n")
                await master.writer.drain()
                continue  # keep connection alive

    except ConnectionResetError:
        print(f'Connection forcibly closed by {address}')

async def main(server):

    if server.role == "slave":
        await replica_loop()

    server_process = await asyncio.start_server(handle_client, server.host, server.port)
    print(f"Server created at {server.host}:{server.port}")

    async with server_process:
        await server_process.serve_forever()


def server_setup(host, port, sys_vars):
    print(sys_vars)
    if "--port" in sys_vars:
        if sys_vars.index("--port") + 1:
            port = sys_vars[sys_vars.index("--port") + 1]
        else:
            print(f'Port not specified, defaulting to {port}')

    if "--replicaof" in sys_vars:
        role = "slave"
        master_id = sys.argv[sys.argv.index("--replicaof") + 1]

    else:
        role = None
        master_id = None

    server = ServerState(
        host=host,
        port=port,
        role=role,
        master_id=master_id)

    return server

if __name__ == "__main__":
    HOST = "localhost"
    PORT = 6379

    server = server_setup(HOST, PORT, sys.argv)
    asyncio.run(main(server))
    #main()
