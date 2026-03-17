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

def command_redirect(ip,port):
    return {
        "type":"command_redirect",
        "target_master_ip":ip,
        "target_master_port":port
    }