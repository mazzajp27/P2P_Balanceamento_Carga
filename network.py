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


def send_message_to(ip, port, data, timeout=3):
    """Abre uma conexão pontual, envia uma mensagem e fecha. Retorna a resposta ou None."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((ip, port))
        send_message(sock, data)
        response = receive_message(sock)
        sock.close()
        return response
    except Exception as e:
        print(f"[NETWORK] Falha ao enviar para {ip}:{port} — {e}")
        return None