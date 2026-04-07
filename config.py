# config.py
MASTER_PORT = 5000
BUFFER_SIZE = 4096             # Atualizado conforme solicitado
THRESHOLD = 4
TASK_PROCESS_TIME = 6
REQUEST_INTERVAL = 4
LOG_SEPARATOR = "======================================="

# --- CONFIGURAÇÕES DE ELEIÇÃO ---
ELECTION_PORT = 5020           
MAX_HEARTBEAT_FAILS = 4        
ELECTION_TIMEOUT = 5