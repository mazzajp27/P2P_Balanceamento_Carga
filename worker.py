"""
worker.py
=========
Worker com suporte a eleição de master.

Fluxo normal:
  1. Conecta ao master e se registra.
  2. Envia heartbeat a cada HEARTBEAT_INTERVAL segundos.
  3. Ouve tarefas enviadas pelo master e as processa.

Fluxo de falha:
  4. Se o heartbeat falhar HEARTBEAT_FAIL_THRESHOLD vezes seguidas,
     inicia a eleição via election.py.
  5a. Se eleito master → sobe o servidor master (master.py) neste processo.
  5b. Se não eleito    → reconecta ao novo master e retoma o fluxo normal.
"""

import socket
import time
import uuid
import threading
import sys
import os

from protocol import *
from network import *
from config import *
from election import start_election

# ─────────────────────────────────────────────────────────────────────────────
# Configuração inicial
# ─────────────────────────────────────────────────────────────────────────────

worker_id = str(uuid.uuid4())

# IP deste worker (visível pelos peers). Ajuste conforme sua rede.
MY_IP = os.environ.get("WORKER_IP", "127.0.0.1")

# IP e porta do master atual (pode ser sobrescrito após eleição)
master_ip   = os.environ.get("MASTER_IP", "10.62.206.53")
master_port = int(os.environ.get("MASTER_PORT", str(MASTER_PORT)))

# IPs dos outros workers conhecidos (para broadcast da eleição).
# Preencha com os IPs reais ou passe via variável de ambiente separada por vírgula.
_peer_env   = os.environ.get("PEER_IPS", "")
PEER_IPS    = [ip.strip() for ip in _peer_env.split(",") if ip.strip()]

# Estado compartilhado entre threads
heartbeat_fail_count = 0
election_in_progress = False
current_sock         = None
lock                 = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# Processamento de tarefa
# ─────────────────────────────────────────────────────────────────────────────

def process_task():
    print(LOG_SEPARATOR)
    print("[WORKER]", worker_id)
    print("Executando tarefa...")
    print("Simulando processamento...")
    time.sleep(TASK_PROCESS_TIME)
    print("Tarefa concluída")
    print(LOG_SEPARATOR)

# ─────────────────────────────────────────────────────────────────────────────
# Conexão ao master
# ─────────────────────────────────────────────────────────────────────────────

def connect_to_master(ip, port):
    print(LOG_SEPARATOR)
    print(f"[WORKER] Conectando ao master {ip}:{port}")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    send_message(sock, register_worker(worker_id))
    print("[WORKER] Registrado no master")
    print(LOG_SEPARATOR)
    return sock

# ─────────────────────────────────────────────────────────────────────────────
# Heartbeat com detecção de falha
# ─────────────────────────────────────────────────────────────────────────────

def send_heartbeat(sock):
    """
    Envia heartbeat periodicamente.
    Conta falhas consecutivas e dispara eleição quando atinge o threshold.
    """
    global heartbeat_fail_count, election_in_progress, current_sock

    while True:
        time.sleep(HEARTBEAT_INTERVAL)

        with lock:
            if election_in_progress:
                # Aguarda eleição terminar antes de continuar
                continue

        print("[WORKER] Enviando HEARTBEAT")

        try:
            send_message(sock, heartbeat(worker_id))

            # Aguarda resposta com timeout
            sock.settimeout(HEARTBEAT_INTERVAL)
            response_raw = receive_message(sock)
            sock.settimeout(None)

            if response_raw:
                data = decode_message(response_raw)
                if data.get("RESPONSE") == "ALIVE":
                    print("[WORKER] MASTER respondeu ALIVE")
                    with lock:
                        heartbeat_fail_count = 0   # zera contador em caso de sucesso
            else:
                raise ConnectionError("Sem resposta do master")

        except Exception as e:
            with lock:
                heartbeat_fail_count += 1
                fails = heartbeat_fail_count

            print(f"[WORKER] Falha no heartbeat ({fails}/{HEARTBEAT_FAIL_THRESHOLD}): {e}")

            if fails >= HEARTBEAT_FAIL_THRESHOLD:
                print("[WORKER] Master considerado offline. Iniciando eleição...")
                _trigger_election()
                return   # Esta thread encerra; uma nova será criada após eleição

# ─────────────────────────────────────────────────────────────────────────────
# Escuta de mensagens do master
# ─────────────────────────────────────────────────────────────────────────────

def listen_master(sock):
    global election_in_progress

    while True:
        msg = receive_message(sock)

        if not msg:
            print("[WORKER] Conexão perdida com master")
            break

        data = decode_message(msg)

        # Tarefa recebida
        if data.get("type") == "task":
            process_task()

        # Confirmação de heartbeat (caso venha pelo canal de escuta e não pelo send_heartbeat)
        elif data.get("TASK") == "HEARTBEAT" and data.get("RESPONSE") == "ALIVE":
            print("[WORKER] MASTER respondeu ALIVE")

        # Redirecionamento para outro master (protocolo original do projeto)
        elif data.get("type") == "command_redirect":
            new_ip   = data["target_master_ip"]
            new_port = data["target_master_port"]
            print(f"[WORKER] Redirecionado para {new_ip}:{new_port}")
            _reconnect_to_master(new_ip, new_port)
            return

        # Novo master anunciado (pode vir de outro worker eleito)
        elif data.get("type") == "i_am_master":
            new_ip   = data["master_ip"]
            new_port = data["master_port"]
            print(f"[WORKER] Novo master anunciado: {new_ip}:{new_port}")
            _reconnect_to_master(new_ip, new_port)
            return

# ─────────────────────────────────────────────────────────────────────────────
# Eleição
# ─────────────────────────────────────────────────────────────────────────────

def _trigger_election():
    global election_in_progress

    with lock:
        if election_in_progress:
            return
        election_in_progress = True

    start_election(
        worker_id=worker_id,
        my_ip=MY_IP,
        peer_ips=PEER_IPS,
        on_elected_as_master=_become_master,
        on_new_master_found=_reconnect_to_master
    )


def _become_master():
    """
    Este worker foi eleito master.
    Importa e inicia o servidor master neste mesmo processo.
    """
    global election_in_progress

    print(LOG_SEPARATOR)
    print("[ELECTION] Tornando-se MASTER...")
    print(LOG_SEPARATOR)

    with lock:
        election_in_progress = False

    # Importa o módulo master e sobe o servidor.
    # O master.py foi escrito para rodar de forma bloqueante (start_server),
    # por isso o executamos em uma thread separada.
    import master
    threading.Thread(target=master.start_server, args=(MASTER_PORT,), daemon=False).start()

    # Reinicia as threads de monitoramento do master
    threading.Thread(target=master.monitor_load, daemon=True).start()
    threading.Thread(target=master.simulate_requests, daemon=True).start()

    print("[ELECTION] Servidor master iniciado neste worker.")


def _reconnect_to_master(new_ip, new_port):
    global master_ip, master_port, heartbeat_fail_count, election_in_progress, current_sock

    print(LOG_SEPARATOR)
    print(f"[WORKER] Reconectando ao novo master {new_ip}:{new_port}")
    print(LOG_SEPARATOR)

    master_ip   = new_ip
    master_port = new_port

    with lock:
        heartbeat_fail_count = 0
        election_in_progress = False

    # Tenta reconectar com retries
    for attempt in range(1, 6):
        try:
            sock = connect_to_master(new_ip, new_port)
            current_sock = sock
            # Reinicia threads
            threading.Thread(target=send_heartbeat, args=(sock,), daemon=True).start()
            threading.Thread(target=listen_master,  args=(sock,), daemon=False).start()
            return
        except Exception as e:
            print(f"[WORKER] Tentativa {attempt}/5 falhou: {e}")
            time.sleep(3)

    print("[WORKER] Não foi possível reconectar. Encerrando.")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# Entrada principal
# ─────────────────────────────────────────────────────────────────────────────

print(LOG_SEPARATOR)
print("WORKER INICIADO:", worker_id)
print("IP local:      ", MY_IP)
print("Master inicial:", master_ip, ":", master_port)
print("Peers:         ", PEER_IPS)
print(LOG_SEPARATOR)

current_sock = connect_to_master(master_ip, master_port)

threading.Thread(target=send_heartbeat, args=(current_sock,), daemon=True).start()
listen_master(current_sock)