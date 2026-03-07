#!/usr/bin/env python3
"""
Simple end-to-end RESP test runner for a Redis-like server.

Usage:
  python resp_smoke_test.py
  python resp_smoke_test.py --host 127.0.0.1 --port 6379
"""

from __future__ import annotations

import argparse
import socket
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Union

CRLF = b"\r\n"


# -----------------------------
# RESP encode/decode
# -----------------------------
def encode_command(*parts: Union[str, bytes, int]) -> bytes:
    """
    Encode a Redis command as a RESP Array of Bulk Strings.
    """
    bparts: List[bytes] = []
    for p in parts:
        if isinstance(p, bytes):
            bparts.append(p)
        elif isinstance(p, int):
            bparts.append(str(p).encode("utf-8"))
        else:
            bparts.append(p.encode("utf-8"))

    out = bytearray()
    out += b"*" + str(len(bparts)).encode("utf-8") + CRLF
    for bp in bparts:
        out += b"$" + str(len(bp)).encode("utf-8") + CRLF
        out += bp + CRLF
    return bytes(out)


@dataclass
class RespError(Exception):
    message: str

    def __str__(self) -> str:
        return self.message


class RespReader:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buf = bytearray()

    def _recv_more(self) -> None:
        chunk = self.sock.recv(4096)
        if not chunk:
            raise ConnectionError("Socket closed by peer")
        self.buf += chunk

    def _read_line(self) -> bytes:
        while True:
            idx = self.buf.find(CRLF)
            if idx != -1:
                line = bytes(self.buf[:idx])
                del self.buf[: idx + 2]
                return line
            self._recv_more()

    def _read_exact(self, n: int) -> bytes:
        while len(self.buf) < n:
            self._recv_more()
        data = bytes(self.buf[:n])
        del self.buf[:n]
        return data

    def read_resp(self) -> Any:
        # Ensure at least 1 byte
        while not self.buf:
            self._recv_more()

        prefix = bytes(self.buf[:1])
        del self.buf[:1]

        if prefix == b"+":
            return self._read_line().decode("utf-8", errors="replace")
        if prefix == b"-":
            msg = self._read_line().decode("utf-8", errors="replace")
            raise RespError(msg)
        if prefix == b":":
            return int(self._read_line().decode("utf-8", errors="strict"))
        if prefix == b"$":
            length = int(self._read_line().decode("utf-8", errors="strict"))
            if length == -1:
                return None
            data = self._read_exact(length)
            # consume CRLF
            crlf = self._read_exact(2)
            if crlf != CRLF:
                raise ValueError("Invalid bulk string terminator")
            return data.decode("utf-8", errors="replace")
        if prefix == b"*":
            length = int(self._read_line().decode("utf-8", errors="strict"))
            if length == -1:
                return None
            arr = []
            for _ in range(length):
                arr.append(self.read_resp())
            return arr

        raise ValueError(f"Unknown RESP prefix: {prefix!r}")


# -----------------------------
# Test runner
# -----------------------------
def assert_equal(got: Any, expected: Any, label: str) -> None:
    if got != expected:
        raise AssertionError(f"{label} FAILED:\n  got:      {got!r}\n  expected: {expected!r}")
    print(f"✅ {label}")


def assert_raises_resp_error(fn, label: str, contains: Optional[str] = None) -> None:
    try:
        fn()
    except RespError as e:
        if contains is not None and contains.lower() not in str(e).lower():
            raise AssertionError(f"{label} FAILED:\n  error: {e}\n  expected to contain: {contains!r}")
        print(f"✅ {label} (error: {e})")
        return
    raise AssertionError(f"{label} FAILED: expected RespError but no error was raised")


def run_tests(host: str, port: int, timeout: float = 2.0) -> None:
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.settimeout(timeout)
        rr = RespReader(s)

        def send_and_read(*cmd: Union[str, bytes, int]) -> Any:
            payload = encode_command(*cmd)
            s.sendall(payload)
            return rr.read_resp()

        # 1) PING
        assert_equal(send_and_read("PING"), "PONG", "PING -> PONG")

        # 2) ECHO
        assert_equal(send_and_read("ECHO", "hello"), "hello", "ECHO hello")

        # 3) SET key value
        # Some implementations return +OK, some may return OK as simple string -> our decoder returns "OK"
        assert_equal(send_and_read("SET", "key", "value"), "OK", "SET key value")

        # 4) GET key
        assert_equal(send_and_read("GET", "key"), "value", "GET key")

        # 5) GET missing -> null bulk string => None
        assert_equal(send_and_read("GET", "missing"), None, "GET missing -> nil")

        # 6) A couple more useful edge cases
        assert_equal(send_and_read("ECHO", ""), "", "ECHO empty string")

        # Unknown command should return an error
        def unknown():
            send_and_read("MADEUPCMD")

        assert_raises_resp_error(unknown, "Unknown command returns -ERR", contains="unknown")

    print("\nAll tests passed ✅")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--timeout", type=float, default=2.0)
    args = ap.parse_args()
    run_tests(args.host, args.port, args.timeout)


if __name__ == "__main__":
    main()