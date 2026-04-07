# network.py
import socket
from protocol import encode_message
from config import BUFFER_SIZE  # Importamos o buffer novo

def send_message(sock, data):
    try:
        sock.sendall(encode_message(data))
        return True
    except Exception as e:
        # Removi o print de erro aqui para não poluir o terminal quando o Master cair
        return False

def receive_message(sock):
    buffer = ""
    while True:
        try:
            # Usando o novo BUFFER_SIZE
            data = sock.recv(BUFFER_SIZE).decode()
            
            if not data:
                return None
                
            buffer += data
            if "\n" in buffer:
                message, buffer = buffer.split("\n", 1)
                return message
        except Exception:
            return None