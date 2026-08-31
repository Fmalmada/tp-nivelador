import socket

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(socket: socket.socket, size):
    buffer = bytearray()

    while len(buffer) < size:
        chunck = socket.recv(size - len(buffer))
        if not chunck:
            raise ConnectionError (
                "Conection closes by peer before all data was received"
            )
        buffer.extend(chunck)

    return bytes(buffer)



def send_all(socket: socket.socket, bytes):
    total_sent = 0
    while total_sent < len(bytes):
        sent = socket.send(bytes[total_sent:])
        total_sent += sent


