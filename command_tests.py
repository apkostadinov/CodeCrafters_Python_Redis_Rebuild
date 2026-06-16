import socket
import app.services.parser as parser

HOST = 'localhost'  # server address
PORT = 6379         # server port

def encode_command(*parts: str) -> bytes:
    out = f"*{len(parts)}\r\n".encode("utf-8")
    for part in parts:
        if isinstance(part, str):
            payload = part.encode("utf-8")
            out += f"${len(payload)}\r\n".encode("utf-8")
            out += payload + b"\r\n"
            continue
        else:
            payload = part
            out += f':{payload}\r\n'.encode("utf-8")
    return out

def send_redis_command(*parts: str):
    resp = encode_command(*parts)
    with socket.create_connection((HOST, PORT)) as sock:
        print(f"Sending: {resp}")
        sock.sendall(resp)
        data = sock.recv(1024)
        print(f"Received raw: {data}")
        print(f"Received: {data} -> {parser.parser_first(data)}")


if __name__ == "__main__":
    # print(parser.parser_first(b'*3\r\n$3\r\nSET\r\n$9\r\nblueberry\r\n$9\r\nraspberry\r\n'))
    # send_redis_command("PING")
    # send_redis_command("PING")
    # send_redis_command("ECHO", "hello")
    # send_redis_command("SET", "fruit", "apple")
    # send_redis_command("GET", "fruit")
    # send_redis_command("GET", "vegetable")
    # send_redis_command("RPUSH", "vegetable", "tomato", "cucumber", "potato")
    # send_redis_command("RPUSH", "fruit", "cherry", "pineapple", "strawberry")
    # send_redis_command("LRANGE","vegetable", 1, 2)
    # send_redis_command("GET", "fruit")
    # send_redis_command("SET", "fruit", "apple")
    # send_redis_command("RPUSH", "fruit", "cherry", "pineapple", "strawberry", "orange")
    # send_redis_command("LRANGE","fruit", 0, -3)
    # send_redis_command("LRANGE", "fruit", 0, 14)
    # send_redis_command("RPUSH", "list_key", "a","b","c","d","e")
    # send_redis_command("LRANGE", "list_key", -2, -1)
    # send_redis_command("LPUSH", "list_key", "c")
    # send_redis_command("LPUSH", "list_key", "b", "a")
    # send_redis_command("LRANGE","list_key",0, - 1)
    # send_redis_command("LLEN", "list_key")
    # send_redis_command('RPUSH', 'mango', 'apple', 'orange', 'mango', 'strawberry', 'blueberry', 'banana', 'raspberry')
    # send_redis_command("LPOP", "mango", 2)
    # send_redis_command('BLPOP', 'pineapple', '0')
    # send_redis_command('BLPOP', 'pineapple', '0')
    # send_redis_command('RPUSH', 'pineapple', 'apple')
    # send_redis_command("TYPE", "fruit")

    # send_redis_command("XRANGE", "stream_key", "1526919030474", "1526919030474")
    # send_redis_command("XRANGE", "stream_key", "1526919030474", "+")    # send_redis_command("XADD", "stream_key", "0-0", "foo", "bar")
#     # send_redis_command("XADD", "stream_key", "0-*", "foo", "bar")
#     # send_redis_command("XADD", "stream_key", "0-2", "foo", "boo")
#     # send_redis_command("XADD", "stream_key", "0-*", "foo", "baz")
#     # send_redis_command("XADD", "stream_key", "0-0", "foo", "baz")
#     # send_redis_command("XADD", "stream_key", "1526919030474-*", "temperature", 36, "humidity", 95)
#     # send_redis_command("XADD", "stream_key", "1526919030474-*", "temperature", 36, "humidity", 95)
#     # send_redis_command("XADD", "stream_key", "1526919030496-*", "temperature", 36, "humidity", 95)
#     # send_redis_command("XADD", "stream_key", "*", "temperature", 36, "humidity", 95)
#     # send_redis_command("XREAD", "STREAMS", "stream_key", "1526919030474")


    #
    # send_redis_command("XADD","some_key", "1 - 1", "foo", "bar")
    # send_redis_command("XADD","some_key", "0 - 1", "foo", "baz")
    # send_redis_command("XADD","some_key", "0 - 0", "foo", "baz")
    send_redis_command('XADD', 'grape', '0-1', 'temperature', '52')
    send_redis_command('XREAD', 'streams', 'grape', '0-0')
