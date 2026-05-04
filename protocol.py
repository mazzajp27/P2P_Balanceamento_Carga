import json

def encode_message(data):
    return (json.dumps(data) + "\n").encode()

def decode_message(data):
    return json.loads(data)


def heartbeat(server_uuid):
    return {
        "SERVER_UUID": server_uuid,
        "TASK": "HEARTBEAT"
    }


def heartbeat_alive(server_uuid):
    return {
        "SERVER_UUID": server_uuid,
        "TASK": "HEARTBEAT",
        "RESPONSE": "ALIVE"
    }


def register_worker(worker_id):
    return {
        "type": "register_worker",
        "worker_id": worker_id
    }


def request_help():
    return {"type": "request_help"}

def response_accepted():
    return {"type": "response_accepted"}

def response_rejected():
    return {"type": "response_rejected"}

def command_redirect(ip, port):
    return {
        "type": "command_redirect",
        "target_master_ip": ip,
        "target_master_port": port
    }


# ── Mensagens de Eleição ──────────────────────────────────────────────────────

def election_announce(worker_id, free_disk_bytes, ip, port):
    """Worker anuncia candidatura com seu espaço livre em disco."""
    return {
        "type": "election_announce",
        "worker_id": worker_id,
        "free_disk": free_disk_bytes,
        "ip": ip,
        "port": port
    }


def election_vote(voter_id, chosen_id):
    """Worker vota em um candidato."""
    return {
        "type": "election_vote",
        "voter_id": voter_id,
        "chosen_id": chosen_id
    }


def election_result(new_master_id, new_master_ip, new_master_port):
    """Resultado da eleição: informa quem é o novo master."""
    return {
        "type": "election_result",
        "new_master_id": new_master_id,
        "new_master_ip": new_master_ip,
        "new_master_port": new_master_port
    }


def i_am_master(master_id, master_ip, master_port):
    """Novo master se anuncia para os outros workers."""
    return {
        "type": "i_am_master",
        "master_id": master_id,
        "master_ip": master_ip,
        "master_port": master_port
    }


def election_bid(worker_id, free_space, ip, port):
    """Broadcast UDP: candidatura na eleição legada (worker.py)."""
    return {
        "type": "ELECTION_BID",
        "worker_id": worker_id,
        "free_space": free_space,
        "ip": ip,
        "port": port,
    }


def election_victory(worker_id, ip, port):
    """Broadcast UDP: anúncio do vencedor da eleição legada."""
    return {
        "type": "ELECTION_VICTORY",
        "worker_id": worker_id,
        "ip": ip,
        "port": port,
    }