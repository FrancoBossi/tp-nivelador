import socket

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(socket: socket.socket, size):
    return socket.recv(size)


def send_all(sock: socket.socket, data):
    total_sent = 0  #bytes enviados
    while total_sent < len(data):
        sent = sock.send(data[total_sent:])
        if sent == 0:
            raise ConnectionError(f"No se envio informacion: enviados {total_sent}/{len(data)} bytes")
        total_sent += sent
    return total_sent
