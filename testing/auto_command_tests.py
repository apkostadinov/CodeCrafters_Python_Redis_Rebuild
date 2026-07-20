import re
import socket
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

import pytest

from .testing_utils import encode_command


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_server(port, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("localhost", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"Redis test server did not start on port {port}")


@pytest.fixture(scope="module")
def redis_server():
    port = _free_port()
    process = subprocess.Popen(
        [sys.executable, "-m", "app.main", "--port", str(port)],
        cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_server(port)
        yield port
    finally:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)


def _send_redis_command(port, *parts):
    with socket.create_connection(("localhost", port), timeout=1) as sock:
        sock.settimeout(0.05)
        sock.sendall(encode_command(*parts))
        chunks = []
        while True:
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                break
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)


def _bulk(value):
    payload = str(value).encode("utf-8")
    return b"$%d\r\n%s\r\n" % (len(payload), payload)


def _array(*items):
    return b"*%d\r\n" % len(items) + b"".join(items)


def _integer(value):
    return b":%d\r\n" % value


def _simple(value):
    return b"+%s\r\n" % value.encode("utf-8")


def _error(value):
    return b"-%s\r\n" % value.encode("utf-8")


INFO_RESPONSE = _bulk("# Replication role:master master_replid:None master_repl_offset:0")
INFO_REPLICATION_RESPONSE = _bulk("role:master master_replid:None master_repl_offset:0")

FRUIT_7 = _array(
    _bulk("cherry"),
    _bulk("pineapple"),
    _bulk("strawberry"),
    _bulk("cherry"),
    _bulk("pineapple"),
    _bulk("strawberry"),
    _bulk("orange"),
)
FRUIT_FIRST_5 = _array(
    _bulk("cherry"),
    _bulk("pineapple"),
    _bulk("strawberry"),
    _bulk("cherry"),
    _bulk("pineapple"),
)
FRUIT_FIRST_12 = _array(
    _bulk("cherry"),
    _bulk("pineapple"),
    _bulk("strawberry"),
    _bulk("cherry"),
    _bulk("pineapple"),
    _bulk("strawberry"),
    _bulk("orange"),
    _bulk("cherry"),
    _bulk("pineapple"),
    _bulk("strawberry"),
    _bulk("cherry"),
    _bulk("pineapple"),
)
LIST_KEY_14 = _array(
    _bulk("a"),
    _bulk("b"),
    _bulk("c"),
    _bulk("c"),
    _bulk("a"),
    _bulk("b"),
    _bulk("c"),
    _bulk("d"),
    _bulk("e"),
    _bulk("a"),
    _bulk("b"),
    _bulk("c"),
    _bulk("d"),
    _bulk("e"),
)
VEGETABLE_SLICE = _array(_bulk("cucumber"), _bulk("potato"))
LIST_KEY_TAIL = _array(_bulk("d"), _bulk("e"))
MANGO_FIRST_POP = _array(_bulk("apple"), _bulk("orange"))
MANGO_SECOND_POP = _array(_bulk("mango"), _bulk("strawberry"))
GRAPE_XREAD = _array(
    _array(
        _bulk("grape"),
        _array(
            _array(
                _bulk("0-1"),
                _array(_bulk("temperature"), _bulk("52")),
            )
        ),
    )
)
OTHER_STREAM_XREAD = _array(
    _array(
        _bulk("stream_key"),
        _array(
            _array(_bulk("0-2"), _array(_bulk("foo"), _bulk("boo"))),
            _array(_bulk("0-3"), _array(_bulk("foo"), _bulk("baz"))),
        ),
    )
)
ORANGE_XREAD = _array(
    _array(
        _bulk("orange"),
        _array(
            _array(
                _bulk("0-1"),
                _array(_bulk("temperature"), _bulk("52")),
            )
        ),
    )
)


COMMAND_CASES = [
    ("01_ping", ("PING",), _simple("PONG")),
    ("02_ping_ping", ("PING", "PING"), _array(_bulk("PONG"), _bulk("PONG"))),
    ("03_info_default_port", ("INFO",), INFO_RESPONSE),
    ("04_echo", ("ECHO", "hello"), _bulk("hello")),
    ("05_set_fruit_apple", ("SET", "fruit", "apple"), _simple("OK")),
    ("06_set_fruit_grape", ("SET", "fruit", "grape"), _simple("OK")),
    ("07_set_fruit_orange", ("SET", "fruit", "orange"), _simple("OK")),
    ("08_get_fruit", ("GET", "fruit"), _bulk("orange")),
    ("09_get_missing_vegetable", ("GET", "vegetable"), b"$-1\r\n"),
    ("10_rpush_vegetable", ("RPUSH", "vegetable", "tomato", "cucumber", "potato"), _integer(3)),
    ("11_rpush_fruit", ("RPUSH", "fruit", "cherry", "pineapple", "strawberry"), _integer(3)),
    ("12_lrange_vegetable", ("LRANGE", "vegetable", 1, 2), VEGETABLE_SLICE),
    ("13_get_fruit", ("GET", "fruit"), _bulk("orange")),
    ("14_set_fruit_apple", ("SET", "fruit", "apple"), _simple("OK")),
    ("15_rpush_fruit_four", ("RPUSH", "fruit", "cherry", "pineapple", "strawberry", "orange"), _integer(7)),
    ("16_lrange_fruit_to_negative_3", ("LRANGE", "fruit", 0, -3), FRUIT_FIRST_5),
    ("17_lrange_fruit_0_14", ("LRANGE", "fruit", 0, 14), FRUIT_7),
    ("18_rpush_list_key", ("RPUSH", "list_key", "a", "b", "c", "d", "e"), _integer(5)),
    ("19_lrange_list_key_tail", ("LRANGE", "list_key", -2, -1), LIST_KEY_TAIL),
    ("20_lpush_list_key_c", ("LPUSH", "list_key", "c"), _integer(6)),
    ("21_set_fruit_apple", ("SET", "fruit", "apple"), _simple("OK")),
    ("22_get_fruit", ("GET", "fruit"), _bulk("apple")),
    ("23_get_missing_vegetable", ("GET", "vegetable"), b"$-1\r\n"),
    ("24_rpush_vegetable", ("RPUSH", "vegetable", "tomato", "cucumber", "potato"), _integer(6)),
    ("25_rpush_fruit", ("RPUSH", "fruit", "cherry", "pineapple", "strawberry"), _integer(10)),
    ("26_lrange_vegetable", ("LRANGE", "vegetable", 1, 2), VEGETABLE_SLICE),
    ("27_get_fruit", ("GET", "fruit"), _bulk("apple")),
    ("28_set_fruit_apple", ("SET", "fruit", "apple"), _simple("OK")),
    ("29_rpush_fruit_four", ("RPUSH", "fruit", "cherry", "pineapple", "strawberry", "orange"), _integer(14)),
    ("30_lrange_fruit_to_negative_3", ("LRANGE", "fruit", 0, -3), FRUIT_FIRST_12),
    ("31_lrange_fruit_0_14_closes", ("LRANGE", "fruit", 0, 14), b""),
    ("32_rpush_list_key", ("RPUSH", "list_key", "a", "b", "c", "d", "e"), _integer(11)),
    ("33_lrange_list_key_tail", ("LRANGE", "list_key", -2, -1), LIST_KEY_TAIL),
    ("34_lpush_list_key_c", ("LPUSH", "list_key", "c"), _integer(12)),
    ("35_lpush_list_key_b_a", ("LPUSH", "list_key", "b", "a"), _integer(14)),
    ("36_lrange_list_key_all", ("LRANGE", "list_key", 0, -1), LIST_KEY_14),
    ("37_llen_list_key", ("LLEN", "list_key"), _integer(14)),
    ("38_rpush_mango", ("RPUSH", "mango", "apple", "orange", "mango", "strawberry", "blueberry", "banana", "raspberry"), _integer(7)),
    ("39_lpop_mango_two", ("LPOP", "mango", 2), MANGO_FIRST_POP),
    ("40_rpush_pineapple", ("RPUSH", "pineapple", "apple"), _integer(1)),
    ("41_type_fruit", ("TYPE", "fruit"), _simple("string")),
    ("42_lrange_list_key_all", ("LRANGE", "list_key", 0, -1), LIST_KEY_14),
    ("43_llen_list_key", ("LLEN", "list_key"), _integer(14)),
    ("44_rpush_mango", ("RPUSH", "mango", "apple", "orange", "mango", "strawberry", "blueberry", "banana", "raspberry"), _integer(12)),
    ("45_lpop_mango_two", ("LPOP", "mango", 2), MANGO_SECOND_POP),
    ("46_rpush_pineapple", ("RPUSH", "pineapple", "apple"), _integer(2)),
    ("47_type_fruit", ("TYPE", "fruit"), _simple("string")),
    ("48_xrange_missing_stream_exact_id_closes", ("XRANGE", "stream_key", "1526919030474", "1526919030474"), b""),
    ("49_xrange_missing_stream_to_plus_closes", ("XRANGE", "stream_key", "1526919030474", "+"), b""),
    ("50_xadd_stream_0_star", ("XADD", "stream_key", "0-*", "foo", "bar"), _bulk("0-1")),
    ("51_xadd_stream_0_2", ("XADD", "stream_key", "0-2", "foo", "boo"), _bulk("0-2")),
    ("52_xadd_stream_0_star", ("XADD", "stream_key", "0-*", "foo", "baz"), _bulk("0-3")),
    ("53_xadd_stream_0_0", ("XADD", "stream_key", "0-0", "foo", "baz"), _error("ERR The ID specified in XADD must be greater than 0-0")),
    ("54_xadd_stream_timestamp_star", ("XADD", "stream_key", "1526919030474-*", "temperature", 36, "humidity", 95), _bulk("1526919030474-0")),
    ("55_xadd_stream_timestamp_star", ("XADD", "stream_key", "1526919030474-*", "temperature", 36, "humidity", 95), _bulk("1526919030474-1")),
    ("56_xadd_stream_later_timestamp_star", ("XADD", "stream_key", "1526919030496-*", "temperature", 36, "humidity", 95), _bulk("1526919030496-0")),
    ("57_xadd_stream_auto_id", ("XADD", "stream_key", "*", "temperature", 36, "humidity", 95), None),
    ("58_xread_stream_key", ("XREAD", "STREAMS", "stream_key", "1526919030474"), None),
    ("59_xadd_some_key_spaced_id", ("XADD", "some_key", "1 - 1", "foo", "bar"), _bulk("1 - 1")),
    ("60_xadd_some_key_lower_spaced_id", ("XADD", "some_key", "0 - 1", "foo", "baz"), _error("ERR The ID specified in XADD is equal or smaller than the target stream top item")),
    ("61_xadd_some_key_zero_spaced_id", ("XADD", "some_key", "0 - 0", "foo", "baz"), _error("ERR The ID specified in XADD must be greater than 0-0")),
    ("62_xadd_grape", ("XADD", "grape", "0-1", "temperature", "52"), _bulk("0-1")),
    ("63_xread_grape", ("XREAD", "streams", "grape", "0-0"), GRAPE_XREAD),
    ("64_xadd_stream_lower_id", ("XADD", "stream_key", "0-1", "temperature", "95"), _error("ERR The ID specified in XADD is equal or smaller than the target stream top item")),
    ("65_xadd_other_stream_key", ("XADD", "other_stream_key", "0-2", "humidity", "97"), _bulk("0-2")),
    ("66_xread_two_streams", ("XREAD", "streams", "stream_key", "other_stream_key", "0-1", "0-2"), OTHER_STREAM_XREAD),
    ("67_xadd_orange", ("XADD", "orange", "0-1", "temperature", "52"), _bulk("0-1")),
    ("68_xread_orange", ("XREAD", "streams", "orange", "0-0"), ORANGE_XREAD),
    ("69_xadd_orange_second", ("XADD", "orange", "0-2", "humidity", "73"), _bulk("0-2")),
    ("70_set_fruit_xyz", ("SET", "fruit", "xyz"), _simple("OK")),
    ("71_incr_fruit_non_integer", ("INCR", "fruit"), _error("ERR value is not an integer or out of range")),
    ("72_get_fruit", ("GET", "fruit"), _bulk("xyz")),
    ("73_info_replica_port_command", ("INFO",), INFO_RESPONSE),
    ("74_get_fruit_replica_port_command", ("GET", "fruit"), _bulk("xyz")),
    ("75_info_replication", ("INFO", "replication"), INFO_REPLICATION_RESPONSE),
]


def _xread_with_generated_id(generated_id):
    generated_id = generated_id.decode("utf-8")
    return _array(
        _array(
            _bulk("stream_key"),
            _array(
                _array(_bulk("1526919030474-0"), _array(_bulk("temperature"), _bulk("36"), _bulk("humidity"), _bulk("95"))),
                _array(_bulk("1526919030474-1"), _array(_bulk("temperature"), _bulk("36"), _bulk("humidity"), _bulk("95"))),
                _array(_bulk("1526919030496-0"), _array(_bulk("temperature"), _bulk("36"), _bulk("humidity"), _bulk("95"))),
                _array(_bulk(generated_id), _array(_bulk("temperature"), _bulk("36"), _bulk("humidity"), _bulk("95"))),
            ),
        )
    )


def test_command_tests_send_redis_command_sequence(redis_server):
    generated_stream_id = None

    for case_id, command, expected in COMMAND_CASES:
        response = _send_redis_command(redis_server, *command)

        if case_id == "57_xadd_stream_auto_id":
            match = re.fullmatch(rb"\$15\r\n(\d{13}-0)\r\n", response)
            assert match, f"{case_id} returned {response!r}"
            generated_stream_id = match.group(1)
            continue

        if case_id == "58_xread_stream_key":
            assert generated_stream_id is not None
            expected = _xread_with_generated_id(generated_stream_id)

        assert response == expected, f"{case_id} returned {response!r}"
