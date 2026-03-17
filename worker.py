import socket
import time
import uuid
import threading

from protocol import *
from network import *
from config import *

worker_id = str(uuid.uuid4())

master_ip = "10.62.206.53"
master_port = 5000


def process_task():

    print(LOG_SEPARATOR)
    print("[WORKER]", worker_id)
    print("Executando tarefa...")
    print("Simulando processamento...")

    time.sleep(TASK_PROCESS_TIME)

    print("Tarefa concluída")
    print(LOG_SEPARATOR)


def connect_to_master(ip, port):

    print(LOG_SEPARATOR)
    print("[WORKER] Conectando ao master", ip, port)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    sock.connect((ip, port))

    send_message(sock, register_worker(worker_id))

    print("[WORKER] Registrado no master")

    return sock


def send_heartbeat(sock):

    while True:

        time.sleep(4)

        print("[WORKER] Enviando HEARTBEAT")

        send_message(sock, heartbeat("MASTER"))


def listen_master(sock):

    while True:

        msg = receive_message(sock)

        if not msg:
            print("[WORKER] Conexão perdida com master")
            break

        data = decode_message(msg)

        if data.get("type") == "task":

            process_task()

        if data.get("TASK") == "HEARTBEAT":

            if data.get("RESPONSE") == "ALIVE":

                print("[WORKER] MASTER respondeu ALIVE")


print(LOG_SEPARATOR)
print("WORKER INICIADO:", worker_id)
print(LOG_SEPARATOR)

sock = connect_to_master(master_ip, master_port)

threading.Thread(target=send_heartbeat, args=(sock,), daemon=True).start()

listen_master(sock)