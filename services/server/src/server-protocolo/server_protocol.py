import safe_socket

#Codifica la longitud de una trama en cuatro bytes big-endian
def uint32_to_bytes(value: int) -> bytes:
    return bytes([
        (value >> 24) & 0xFF,
        (value >> 16) & 0xFF,
        (value >> 8) & 0xFF,
        value & 0xFF,
    ])

#Decodifica una longitud de trama desde cuatro bytes big-endian
def bytes_to_uint32(data: bytes) -> int:
    return (int(data[0]) << 24) | (int(data[1]) << 16) | (int(data[2]) << 8) | int(data[3])

#Lee desde un socket una trama cuyo tamanio esta prefijado
def receive_frame(sock):
    header = safe_socket.recv_all(sock, 4)
    if not header:
        return None
    payload_length = bytes_to_uint32(header)
    if payload_length == 0:
        return b""
    return safe_socket.recv_all(sock, payload_length)

#Antecede la longitud al payload y envaa la trama completa
def send_frame(sock, payload: bytes):
    header = uint32_to_bytes(len(payload))
    safe_socket.send_all(sock, header + payload)

#Valida y separa un registro CSV de apuesta en sus seis campos
def deserialize_bet(payload: bytes | str) -> tuple[str, str, str, str, str, str]:
    decoded = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    fields = decoded.split(",")
    if len(fields) != 6:
        raise ValueError(f"Invalid bet payload: {decoded!r}")
    return tuple(fields)

#Decodifica un payload separado por saltos de linea en registros individuales
def deserialize_batch(payload: bytes) -> list[tuple[str, str, str, str, str, str]]:
    if not payload:
        return []

    bets = []
    for line in payload.decode("utf-8").splitlines():
        line = line.strip()
        if line:
            bets.append(deserialize_bet(line))
    return bets

#Elimina el identificador de agencia al formar la respuesta de un ganador
def serialize_winner(fields: tuple[str, str, str, str, str, str]) -> str:
    _, first_name, last_name, document, birthdate, number = fields
    return f"{first_name},{last_name},{document},{birthdate},{number}"
