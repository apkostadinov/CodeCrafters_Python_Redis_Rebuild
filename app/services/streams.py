print(f"Importing {__name__}")

from .exceptions import RespError, IncompleteMessage
import time

def deconstruct_stream_id(stream_id: str) -> tuple[str | None,str | None]:
    if stream_id == "*":
        return "*","*"
    try:
        id_ms, id_sq = [str.strip(x) for x in stream_id.split("-")]
    except (ValueError, KeyError):
        print("Proposed ID is invalid")
        return None, None
    return id_ms, id_sq

def validate_stream_id(main_id_ms, main_id_sq, stream = None) -> bool:

    if main_id_ms is None or main_id_sq is None:
        return False

    main_id_ms, main_id_sq = int(main_id_ms), int(main_id_sq)

    if main_id_ms == 0 and main_id_sq == 0:
        return False

    if stream:
        last_id_ms, last_id_sq = (int(x) for x in deconstruct_stream_id(stream["xid"]))

        if last_id_ms is None or last_id_sq is None:
            return False
    else:
        return True

    if last_id_ms > main_id_ms:
        print("Last MS ID larger than Proposed MS ID")
        return False
    elif last_id_ms == main_id_ms:
        print("Last MS ID equals to Proposed MS ID")
        if last_id_sq < main_id_sq:
            print("Last MS ID is smaller than Proposed MS ID")
            return True
        else:
            print("Last MS ID is equals or larger than Proposed MS ID")
            return False
    else:
        return True

def generate_id_sq(main_id_ms, main_id_sq, stream = None) -> tuple:
    if stream:
        last_id_ms, last_id_sq = deconstruct_stream_id(stream["xid"])
    else:
        last_id_ms, last_id_sq = None, None

    if main_id_ms == "*":
        main_id_ms = round(time.time() * 1000)

    if main_id_sq == "*":
        if last_id_sq:
            if last_id_ms == main_id_ms:
                main_id_sq = str(int(last_id_sq) + 1)
            elif main_id_ms != "0":
                main_id_sq = "0"
            else:
                main_id_sq = "1"
        elif main_id_ms == '0':
            main_id_sq = "1"
        elif main_id_ms != "0":
            main_id_sq = "0"
        else:
            raise RespError("generate_id_sq: main_id is invalid. -ERR")

    return main_id_ms,main_id_sq
