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


def create_connections(server):
    while True:
        conn, addr = server.accept()
        print("[MASTER] Nova conexão:", addr)
        threading.Thread(target=handle_worker, args=(conn,), daemon=True).start()


# Dentro do master.py...

def start_server(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # 0.0.0.0 permite que o Master escute conexões de qualquer IP (da sua máquina ou do seu amigo)
    server.bind(("0.0.0.0", port))
    server.listen()

    print(LOG_SEPARATOR)
    print("MASTER INICIADO")
    print("PORTA:", port)
    print("IP LOCAL: 10.62.206.21")
    print("UUID:", server_uuid)
    print(LOG_SEPARATOR)

    while True:
        try:
            conn, addr = server.accept()
            print("[MASTER] Nova conexão:", addr)
            # Thread que lida com o worker também precisa ser daemon
            threading.Thread(target=handle_worker, args=(conn,), daemon=True).start()
        except Exception:
            break

def start_master_services(port=MASTER_PORT):
    # Definir daemon=True é o segredo para o Ctrl+C funcionar!
    threading.Thread(target=monitor_load, daemon=True).start()
    threading.Thread(target=simulate_requests, daemon=True).start()
    threading.Thread(target=start_server, args=(port,), daemon=True).start()

if __name__ == "__main__":
    start_master_services(MASTER_PORT)
    
    # Este try/except captura o seu Ctrl+C
    try:
        while True:
            time.sleep(1) # Mantém a thread principal viva esperando
    except KeyboardInterrupt:
        print("\n" + LOG_SEPARATOR)
        print("[MASTER] Encerrando com Ctrl+C...")
        print("[MASTER] Os workers vão perceber a queda em instantes!")
        print(LOG_SEPARATOR)
        import sys
        sys.exit(0) # Derruba o programa e quebra a conexão de rede