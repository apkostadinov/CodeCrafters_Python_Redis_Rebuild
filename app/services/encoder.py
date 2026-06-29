from .exceptions import *

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
        elif isinstance(var, bytes):
            returnable += var
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

def encode(val: "RedisResponse"):
    if val.encoding == "*":
        if len(val.value) > 1:
            for i in range(len(val.value)):
                val.value[i] = encode(val.value[i])
        return encode_array(val.value)

    elif val.encoding == "+":
        return encode_simple_string(val.value)

    elif val.encoding == "$":
        return encode_bulk_string(val.value)

    elif val.encoding == ":":
        return encode_integer(val.value)

    elif val.encoding == "-":
        return encode_simple_error(val.value)

    else:
        raise RespError("Passed value has no assigned encoding.")