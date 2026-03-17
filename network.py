import socket
from protocol import encode_message

def send_message(sock, data):

    print("[NETWORK] Enviando dados...")

    sock.sendall(encode_message(data))


def receive_message(sock):

    buffer = ""

    while True:

        data = sock.recv(1024).decode()

        if not data:
            return None

        buffer += data

        if "\n" in buffer:

            message, buffer = buffer.split("\n", 1)

            print("[NETWORK] Mensagem recebida bruta:", message)

            return message