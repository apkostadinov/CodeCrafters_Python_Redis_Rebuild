from copy import deepcopy

print(f"Importing {__name__}")

from .exceptions import RespError, IncompleteMessage, StreamNotFound
import time
from copy import deepcopy


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


# def flatten_stream_item(stream, start, end):
#     collection = list()
#
#     for item in stream:
#         item_ms, item_sq = (int(x) for x in deconstruct_stream_id(item["xid"]))
#
#         if item_ms < start_ms or item_ms > end_ms:
#             continue
#
#         if start_sq and item_sq < start_sq:
#             continue
#
#         if end_sq and item_sq > end_sq:
#             continue
#
#         item_id = item.pop("xid")
#         temp_list = list()
#
#         for key in item:
#             temp_list.append(key)
#             temp_list.append(str(item[key]))
#
#         collection.append([item_id, temp_list])
#
#     return collection

def xread_extraction(streams, pairs):
    collection = list()

    for key in pairs.keys():
        stream = deepcopy(streams.get(key, None))
        if not stream:
            return None

        start = pairs[key]
        if "-" in start:
            start_ms, start_sq = (int(x) for x in deconstruct_stream_id(start))
        else:
            start_ms, start_sq = int(start), None

        for item in stream:
            item_ms, item_sq = (int(x) for x in deconstruct_stream_id(item["xid"]))

            if item_ms < start_ms:
                continue

            if start_sq and item_sq <= start_sq:
                continue

            item_id = item.pop("xid")
            temp_list = list()

            for i in item:
                temp_list.append(i)
                temp_list.append(str(item[i]))

            collection.append([key, [[item_id, temp_list]]])

    return collection if len(collection)>0 else None