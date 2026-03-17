import socket
import threading
import time
import uuid

from protocol import *
from network import *
from config import *

workers = {}
task_queue = []

server_uuid = str(uuid.uuid4())


def handle_worker(conn):

    global workers

    worker_id = None

    while True:

        msg = receive_message(conn)

        if not msg:
            break

        data = decode_message(msg)

        # REGISTRO DO WORKER
        if data.get("type") == "register_worker":

            worker_id = data["worker_id"]

            workers[worker_id] = conn

            print(LOG_SEPARATOR)
            print("[MASTER] Worker registrado:", worker_id)
            print("Workers ativos:", len(workers))
            print(LOG_SEPARATOR)

        # HEARTBEAT
        elif data.get("TASK") == "HEARTBEAT":

            print("[MASTER] HEARTBEAT recebido")

            send_message(conn, heartbeat_alive(server_uuid))

        # SE EXISTIR TAREFA NA FILA
        if worker_id and len(task_queue) > 0:

            print(LOG_SEPARATOR)
            print("[MASTER] Enviando tarefa para worker:", worker_id)
            print("Fila antes:", len(task_queue))

            task_queue.pop(0)

            send_message(conn, {"type": "task"})

            print("Fila depois:", len(task_queue))
            print(LOG_SEPARATOR)


def monitor_load():

    while True:

        print(LOG_SEPARATOR)
        print("[MASTER] Monitorando carga")

        print("Tarefas pendentes:", len(task_queue))
        print("Workers disponíveis:", len(workers))

        if len(task_queue) > THRESHOLD:

            print("[MASTER] SATURAÇÃO DETECTADA")

        time.sleep(5)


def simulate_requests():

    while True:

        print(LOG_SEPARATOR)
        print("[SIMULADOR] Gerando nova tarefa")

        task_queue.append("task")

        print("Fila atual:", len(task_queue))

        time.sleep(REQUEST_INTERVAL)


def start_server(port):

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind(("10.62.206.53", port))

    server.listen()

    print(LOG_SEPARATOR)
    print("MASTER INICIADO")
    print("PORTA:", port)
    print("UUID:", server_uuid)
    print(LOG_SEPARATOR)

    while True:

        conn, addr = server.accept()

        print("[MASTER] Nova conexão:", addr)

        threading.Thread(target=handle_worker, args=(conn,)).start()


threading.Thread(target=monitor_load).start()
threading.Thread(target=simulate_requests).start()

start_server(5000)