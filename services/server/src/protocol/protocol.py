from safe_socket import recv_all, send_all

_FIELD_SEP = ","
_RECORD_SEP = "\n"

HELLO = b"H"
BET_BATCH = b"B"
DONE = b"D"
ACK = b"A"
WINNERS = b"W"

def send_message(sock, message_type: bytes, payload: bytes = b"") -> None:
    body = message_type + payload
    header = len(body).to_bytes(4, "big")
    send_all(sock, header + body)

def recv_message(sock) -> tuple[bytes, bytes]:
    header = recv_all(sock, 4)
    body_length = int.from_bytes(header, "big")
    body = recv_all(sock, body_length)
    return body[:1], body[1:]

def decode_hello(payload: bytes) -> int:
    return int(payload.decode("utf-8"))

def decode_bet(payload: bytes) -> list[list[str]]:
    if not payload:
        return []
    return [
        line.split(_FIELD_SEP)
        for line in payload.decode("utf-8").split(_RECORD_SEP)
        if line
    ]

def encode_ack(count: int) -> bytes:
    return str(count).encode("utf-8")

def encode_winners(rows: list[list[str]]) -> bytes:
    return _RECORD_SEP.join(_FIELD_SEP.join(row) for row in rows).encode("utf-8")