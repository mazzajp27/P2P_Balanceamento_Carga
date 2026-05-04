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

# Usados por election.py (eleição TCP/consenso — módulo preparado para evolução)
ELECTION_SERVER_PORT = 5030       # TCP: distinto do UDP em ELECTION_PORT
ELECTION_MASTER_PORT = MASTER_PORT
HEARTBEAT_FAIL_THRESHOLD = MAX_HEARTBEAT_FAILS