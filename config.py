MASTER_PORT = 5000
BUFFER_SIZE = 1024

THRESHOLD = 4

TASK_PROCESS_TIME = 6

REQUEST_INTERVAL = 4

LOG_SEPARATOR = "======================================="

# ── Eleição ───────────────────────────────────────────────────────────────────

# Quantas falhas consecutivas de heartbeat disparam a eleição
HEARTBEAT_FAIL_THRESHOLD = 4

# Intervalo do heartbeat (segundos)
HEARTBEAT_INTERVAL = 4

# Porta que o worker eleito usará como master
ELECTION_MASTER_PORT = 5000

# Porta usada entre workers durante a eleição (peer-to-peer)
ELECTION_PEER_PORT_BASE = 6000   # cada worker usa 6000 + índice; na prática
                                  # usamos a porta do servidor de eleição abaixo
ELECTION_SERVER_PORT = 6100      # porta fixa que todos os workers escutam
                                  # para mensagens de eleição