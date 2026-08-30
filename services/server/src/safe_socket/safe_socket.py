import socket

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(sock: socket.socket, size):
    buffer = b'' #buffer para guardar los bytes que leeremos y despues devolvemos
    while len(buffer) < size:
        chunk = sock.recv(size - len(buffer))
        if not chunk:
            raise ConnectionError(f"Conexion cerrada: recibidos {len(buffer)}/{size} bytes")
        buffer += chunk
    return buffer

def send_all(sock: socket.socket, data):
    total_sent = 0  #bytes enviados
    while total_sent < len(data):
        sent = sock.send(data[total_sent:])
        if sent == 0:
            raise ConnectionError(f"No se envio informacion: enviados {total_sent}/{len(data)} bytes")
        total_sent += sent
    return total_sent
