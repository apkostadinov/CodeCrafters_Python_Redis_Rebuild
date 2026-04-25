from typing import Any, Tuple, Optional

CRLF = b"\r\n"

class RespError(Exception):
    pass

def read_line(buf: bytes, i: int) -> Tuple[bytes, int]:
    """
    Read a single RESP line terminated by CRLF starting at index i.
    Returns (line_without_crlf, next_index_after_crlf).
    Raises RespError if no full line available.
    """
    j = buf.find(CRLF, i)
    if j == -1:
        raise RespError("Incomplete frame: missing CRLF")
    return buf[i:j], j + 2

def parse_simple_string(buf: bytes, i: int) -> Tuple[str, int]:
    # +OK\r\n
    line, i = read_line(buf, i)
    if not line.startswith(b"+"):
        raise RespError("Invalid simple string")
    return line[1:].decode("utf-8"), i

def parse_simple_error(buf: bytes, i: int) -> Tuple[str, int]:
    # -ERR something\r\n
    line, i = read_line(buf, i)
    if not line.startswith(b"-"):
        raise RespError("Invalid error")
    return line[1:].decode("utf-8"), i

def parse_integer(buf: bytes, i: int) -> Tuple[int, int]:
    # :123\r\n
    line, i = read_line(buf, i)
    if not line.startswith(b":"):
        raise RespError("Invalid integer")
    try:
        return int(line[1:]), i
    except ValueError as e:
        raise RespError(f"Invalid integer payload: {line!r}") from e

def parse_bulk_string(buf: bytes, i: int) -> Tuple[Optional[bytes], int]:
    # $<len>\r\n<data>\r\n or $-1\r\n (null)
    line, i = read_line(buf, i)
    if not line.startswith(b"$"):
        raise RespError("Invalid bulk string header")
    try:
        length = int(line[1:])
    except ValueError as e:
        raise RespError(f"Invalid bulk string length: {line!r}") from e

    if length == -1:
        return None, i  # Null Bulk String

    end = i + length
    if end + 2 > len(buf):
        raise RespError("Incomplete bulk string data")
    data = buf[i:end]
    if buf[end:end+2] != CRLF:
        raise RespError("Bulk string missing terminating CRLF")
    return data, end + 2

def parse_array(buf: bytes, i: int) -> Tuple[Optional[list], int]:
    # *<count>\r\n<elem1>... or *-1\r\n (null)
    line, i = read_line(buf, i)
    if not line.startswith(b"*"):
        raise RespError("Invalid array header")
    try:
        count = int(line[1:])
    except ValueError as e:
        raise RespError(f"Invalid array length: {line!r}") from e

    if count == -1:
        return None, i  # Null Array

    items = []
    for _ in range(count):
        val, i = parse(buf, i)
        items.append(val)
    return items, i

# Optional RESP3 boolean placeholder, not used in RESP2 Codecrafters stage
def parse_boolean(buf: bytes, i: int) -> Tuple[bool, int]:
    # #t\r\n or #f\r\n (RESP3)
    line, i = read_line(buf, i)
    if not line.startswith(b"#") or len(line) != 2 or line[1:2] not in (b"t", b"f"):
        raise RespError("Invalid boolean")
    return (line[1:2] == b"t"), i

def parse(buf: bytes, i: int = 0) -> Tuple[Any, int]:
    """
    Dispatcher that peeks at the first byte and routes to the right parser.
    Returns (value, next_index).
    """
    if i >= len(buf):
        raise RespError("Empty/incomplete buffer")
    t = buf[i:i+1]
    if t == b"+":
        return parse_simple_string(buf, i)
    if t == b"-":
        return parse_simple_error(buf, i)
    if t == b":":
        return parse_integer(buf, i)
    if t == b"$":
        return parse_bulk_string(buf, i)
    if t == b"*":
        return parse_array(buf, i)
    # RESP3 types can be added here:
    if t == b"#":
        return parse_boolean(buf, i)

    raise RespError(f"Unsupported type byte: {t!r}")

# Convenience function: parse a single frame and return only the value.
def parse_one(buf: bytes) -> Any:
    val, next_i = parse(buf, 0)
    # If you want to enforce complete consumption, you can check next_i == len(buf)
    return val

def encode_integer(n: int):
    if isinstance(n, int):
        return f":{n}\r\n".encode("utf-8")
    else:
        raise ValueError("Passed value is not an integer.")

if __name__ == "__main__":
    # Demo cases:
    print(parse_one(b"+OK\r\n"))  # 'OK'
    print(parse_one(b":123\r\n"))  # 123
    print(parse_one(b"$-1\r\n"))  # None
    print(parse_one(b"$4\r\nPING\r\n"))  # b'PING'
    arr = b"*3\r\n$4\r\nPING\r\n$4\r\nPONG\r\n$4\r\nPING\r\n"
    print(parse_one(arr))  # [b'PING', b'PONG', b'PING']
    print(encode_integer(2))