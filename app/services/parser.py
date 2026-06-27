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
from .exceptions import RespError, IncompleteMessage
CRLF = b"\r\n"

print(f"Importing {__name__}")

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

    try:
        length = int(line[1:])
    except ValueError:
        raise RespError("Invalid array length")

    # Null array
    if length == -1:
        return None, i

    values = []

    for _ in range(length):
        element, i = parser(buf, i)  # may raise IncompleteMessage
        values.append(element)

    return values, i

def parse_bulk_string(buf, i):
    # Read "$<length>\r\n"
    line, i = read_line(buf, i)

    try:
        length = int(line[1:])
    except ValueError:
        raise RespError("Invalid bulk string length")

    # NULL bulk string
    if length == -1:
        return None, i

    # Check if we have enough data
    end = i + length
    if end + 2 > len(buf):
        raise IncompleteMessage()

    data = buf[i:end]

    # Validate trailing CRLF
    if buf[end:end + 2] != CRLF:
        raise RespError("Bulk string missing terminating CRLF")

    return data.decode("utf-8"), end + 2

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
        raise IncompleteMessage()

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
    return val, next_i



def encode_stream(values):
    response = f"*{len(values)}\r\n".encode("utf-8")

    for key in values.keys():
        response += f"*2\r\n".encode("utf-8")
        response += encode_bulk_string(key)
        response += encode_array()




def encode_array(values: list[str|int|list]):
    returnable = f'*{len(values)}\r\n'.encode("utf-8")
    for var in values:
        if isinstance(var, int):
            returnable += encode_integer(var)
        elif isinstance(var, str):
            returnable += encode_bulk_string(var)
        elif isinstance(var, list):
            returnable += encode_array(var)
    # array_len = len(returnable)
    # returnable.insert(0, f'*{array_len}\r\n')
    return returnable

def encode_integer(n: int):
    if isinstance(n, int):
        return f":{n}\r\n".encode("utf-8")
    else:
        raise ValueError("Passed value is not an integer.")

def encode_bulk_string(val: str):
    if isinstance(val, str) and len(val) >= 1:
        length = len(val)
        return f"${length}\r\n{val}\r\n".encode("utf-8")
    else:
        raise ValueError("Passed value is not a string.")

def encode_simple_string(val:str):
    if isinstance(val, str):
        return f"+{val}\r\n".encode("utf-8")
    else:
        raise ValueError("Passed value is not a string.")

def encode_simple_error(val:str):
    if isinstance(val, str):
        return f"-{val}\r\n".encode("utf-8")
    else:
        raise ValueError("Passed value is not a string.")

def encode(val: [int, str, list]):
    if isinstance(val, list):
        return encode_array(val)
    elif isinstance(val, str):
        if " " in val:
            return encode_bulk_string(val)
        else:
            return encode_simple_string(val)
    elif isinstance(val, int):
        return encode_integer(val)
    else:
        raise RespError("Passed value is not a list, str or int.")

print('parser version: 0.3, created 26.10.2025')
# print(parser_first(b"$4\r\nPING\r\n"))
# print(parser_first(b'+OK\r\n'))
# print(parser_first(b"*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n"))
