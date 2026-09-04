import safe_socket


def uint32_to_bytes(value: int) -> bytes:
    return bytes([
        (value >> 24) & 0xFF,
        (value >> 16) & 0xFF,
        (value >> 8) & 0xFF,
        value & 0xFF,
    ])


def bytes_to_uint32(data: bytes) -> int:
    return (int(data[0]) << 24) | (int(data[1]) << 16) | (int(data[2]) << 8) | int(data[3])


def receive_frame(sock):
    header = safe_socket.recv_all(sock, 4)
    if not header:
        return None
    payload_length = bytes_to_uint32(header)
    if payload_length == 0:
        return b""
    return safe_socket.recv_all(sock, payload_length)


def send_frame(sock, payload: bytes):
    header = uint32_to_bytes(len(payload))
    safe_socket.send_all(sock, header + payload)


def deserialize_bet(payload: bytes | str) -> tuple[str, str, str, str, str, str]:
    decoded = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    fields = decoded.split(",")
    if len(fields) != 6:
        raise ValueError(f"Invalid bet payload: {decoded!r}")
    return tuple(fields)


def deserialize_batch(payload: bytes) -> list[tuple[str, str, str, str, str, str]]:
    if not payload:
        return []

    bets = []
    for line in payload.decode("utf-8").splitlines():
        line = line.strip()
        if line:
            bets.append(deserialize_bet(line))
    return bets


def serialize_winner(fields: tuple[str, str, str, str, str, str]) -> str:
    _, first_name, last_name, document, birthdate, number = fields
    return f"{first_name},{last_name},{document},{birthdate},{number}"
