import socket
import time
import uuid
import threading
import shutil

from protocol import *
from network import *
from config import *
import master  # Importamos para o worker poder virar master

worker_id = str(uuid.uuid4())

# --- CONFIGURAÇÃO DE IP NA REDE ---
my_ip = "10.62.206.210"            # IP deste Worker (PC do seu amigo)
current_master_ip = "10.62.206.21" # IP do Master inicial (Seu PC)
current_master_port = MASTER_PORT
sock = None

# --- VARIÁVEIS DE ESTADO ---
hosting_master = False  # Flag correta para manter o programa vivo
in_election = False
heartbeat_fails = 0
election_bids = {}


def get_free_disk_space():
    return shutil.disk_usage("/").free


def process_task():
    print(LOG_SEPARATOR)
    print(f"[WORKER] {worker_id} - Executando tarefa...")
    time.sleep(TASK_PROCESS_TIME)
    print("Tarefa concluída")
    print(LOG_SEPARATOR)


def connect_to_master(ip, port):
    global sock
    print(LOG_SEPARATOR)
    print(f"[WORKER] Conectando ao master {ip}:{port}")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip, port))
        send_message(sock, register_worker(worker_id))
        print("[WORKER] Registrado no master")
        return True
    except Exception as e:
        print(f"[WORKER] Falha ao conectar no Master: {e}")
        return False


def start_election():
    global in_election, election_bids, hosting_master, current_master_ip, heartbeat_fails
    
    in_election = True
    election_bids.clear()
    
    my_space = get_free_disk_space()
    print(LOG_SEPARATOR)
    print(f"👑 [ELEIÇÃO] Iniciada! Meu espaço em disco: {my_space / (1024**3):.2f} GB")
    
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    
    bid_msg = encode_message(election_bid(worker_id, my_space, my_ip, MASTER_PORT))
    
    for _ in range(3):
        udp_sock.sendto(bid_msg, ("<broadcast>", ELECTION_PORT))
        time.sleep(0.5)
        
    print(f"[ELEIÇÃO] Aguardando respostas por {ELECTION_TIMEOUT}s...")
    time.sleep(ELECTION_TIMEOUT)
    
    winner_id = worker_id
    max_space = my_space
    winner_ip = my_ip
    
    for pid, info in election_bids.items():
        if info["space"] > max_space:
            max_space = info["space"]
            winner_id = pid
            winner_ip = info["ip"]
        elif info["space"] == max_space and pid > winner_id:
            winner_id = pid
            winner_ip = info["ip"]

    if winner_id == worker_id:
        print(LOG_SEPARATOR)
        print("👑 [ELEIÇÃO] EU VENCI! ME TORNANDO O NOVO MASTER!")
        print(LOG_SEPARATOR)
        hosting_master = True
        
        # --- CORREÇÃO DO ERRO AQUI ---
        # A linha 'x = ...' foi removida. Usamos apenas a mensagem codificada correta.
        vic_msg = encode_message(election_victory(worker_id, my_ip, MASTER_PORT))
        udp_sock.sendto(vic_msg, ("<broadcast>", ELECTION_PORT))
        
        # Inicia os serviços do Master
        master.start_master_services(MASTER_PORT)
        
        # O novo master se conecta a si mesmo para virar worker
        time.sleep(1.5)
        print("[WORKER] Conectando a mim mesmo para continuar trabalhando...")
        current_master_ip = my_ip
        if connect_to_master(current_master_ip, MASTER_PORT):
            heartbeat_fails = 0
            
    else:
        print(LOG_SEPARATOR)
        print(f"[ELEIÇÃO] Vencedor foi {winner_id}. Conectando ao novo master...")
        print(LOG_SEPARATOR)
    
    in_election = False
    udp_sock.close()


def listen_election_messages():
    global current_master_ip, heartbeat_fails
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    if hasattr(socket, 'SO_REUSEPORT'):
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        
    udp_sock.bind(("", ELECTION_PORT))
    
    while True:
        data, addr = udp_sock.recvfrom(2048)
        if hosting_master: 
            continue
            
        try:
            msg = decode_message(data.decode())
            if msg.get("type") == "ELECTION_BID":
                election_bids[msg["worker_id"]] = {"space": msg["free_space"], "ip": msg["ip"]}
            elif msg.get("type") == "ELECTION_VICTORY":
                print(f"[REDE] Novo Master reconhecido no IP: {msg['ip']}")
                current_master_ip = msg["ip"]
                heartbeat_fails = 0
                if connect_to_master(current_master_ip, current_master_port):
                    print("[WORKER] Conexão com novo master estabelecida!")
        except Exception:
            pass


def send_heartbeat():
    global heartbeat_fails, in_election, sock
    while True:
        time.sleep(4)
        
        # Se estiver no meio de uma eleição, não fazemos nada
        if in_election:
            continue
            
        # Se o socket for None, consideramos que a conexão caiu (falha = True)
        if sock is None:
            success = False
        else:
            success = send_message(sock, heartbeat("MASTER"))
        
        if not success:
            heartbeat_fails += 1
            print(f"⚠️ [WORKER] Falha na conexão com Master ({heartbeat_fails}/{MAX_HEARTBEAT_FAILS})")
            
            if heartbeat_fails >= MAX_HEARTBEAT_FAILS:
                print("🚨 [WORKER] Limite de falhas atingido. Master caiu!")
                start_election()
        else:
            heartbeat_fails = 0


def listen_master():
    global sock, in_election
    while True:
        if in_election:
            time.sleep(1)
            continue
            
        if sock is None:
            time.sleep(1)
            continue
            
        msg = receive_message(sock)
        
        if not msg:
            # A conexão caiu! Anulamos o socket, mas agora o Heartbeat vai perceber e contar as falhas
            if sock is not None:
                print("[WORKER] Conexão TCP perdida. O Heartbeat irá confirmar a queda...")
                sock = None
            time.sleep(2)
            continue

        data = decode_message(msg)

        if data.get("type") == "task":
            process_task()

        elif data.get("TASK") == "HEARTBEAT" and data.get("RESPONSE") == "ALIVE":
            print("[WORKER] MASTER respondeu ALIVE")

# --- INICIALIZAÇÃO ---
print(LOG_SEPARATOR)
print("WORKER INICIADO:", worker_id)
print(LOG_SEPARATOR)

threading.Thread(target=listen_election_messages, daemon=True).start()
threading.Thread(target=send_heartbeat, daemon=True).start()
threading.Thread(target=listen_master, daemon=True).start()

if not connect_to_master(current_master_ip, current_master_port):
    print("[WORKER] Master não encontrado na inicialização. Iniciando eleição...")
    start_election()

# --- LOOP PRINCIPAL SALVA-VIDAS ---
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n" + LOG_SEPARATOR)
    print("[WORKER] Encerrando pelo usuário (Ctrl+C)...")
    print(LOG_SEPARATOR)