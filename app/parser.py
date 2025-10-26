#         b"*": array(data),
#         b"+": simple_string(data),
#         b"-": simple_errors(data),
#         b":": integer(data),
#         b"$": bulk_string(data),
#         b"_": None,
#         b"#": boolean(data),
#         b",": double(data),
#         b"(": big_numbers(data),
#         b"!": bulk_errors(data),
#         b"=": verbatim(data),
#         b"%": maps(data),
#         b"|": attribute(data),
#         b"~": sets(data),
#         b">": push(data)
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

def parse_array(buf, i):
    line, i = read_line(buf, i)
    print(line)
    rng = int(line[1:])

    values = []

    for _ in range(rng):
        element, i = parser(buf, i)
        values.append(element)

    return values, i+2

def parse_bulk_string(buf, i):
    line,i = read_line(buf,i)
    length = int(line[1:])

    if length == -1:
        raise RespError("Incomplete frame: missing CRLF")

    end = i + length
    if end + 2 > len(str(buf)):
        raise RespError("Incomplete bulk string data")
    data = buf[i:end]

    if buf[end:end + 2] != CRLF:
        raise RespError("Bulk string missing terminating CRLF")

    return data.decode('utf-8'), end + 2

def parse_simple_string(buf, i):
    line, i = read_line(buf, i)
    if not line.startswith(b"+"):
        raise RespError("Invalid simple string")
    return line[1:].decode("utf-8"), i+2

def parse_simple_error(buf, i):
    line, i = read_line(buf, i)
    if not line.startswith(b"-"):
        raise RespError("Invalid error")
    return line[1:].decode("utf-8"), i+2

def parse_integer(buf, i):
    line, i = read_line(buf, i)
    if not line.startswith(b":"):
        raise RespError("Invalid integer")
    try:
        return int(line[1:]), i
    except ValueError as e:
        raise RespError(f"Invalid integer payload: {line!r}") from e

def parse_boolean(buf, i):
    line, i = read_line(buf, i)
    if not line.startswith(b"#"):
        raise RespError("Invalid boolean")
    return (line[1:2] == b"t"), i

def parser(buf, i):
    if i >= len(buf):
        raise RespError("Empty/incomplete buffer")
    t = buf[i:i + 1]
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
    if t == b'_':
        return None

    raise RespError(f"Unsupported type byte: {t!r}")
def parser_first(buf: bytes, i=0) -> Any:
    """
    Dispatcher that peeks at the first byte and routes to the right parser.
    Returns (value, next_index).
    """
    val, next_i = parser(buf, i)
    # If you want to enforce complete consumption, you can check next_i == len(buf)
    return val

print('parser version: 0.3, created 26.10.2025')
# print(parser_first(b"$4\r\nPING\r\n"))
# print(parser_first(b'+OK\r\n'))
# print(parser_first(b"*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n"))
