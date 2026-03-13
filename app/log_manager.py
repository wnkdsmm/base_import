logs = []

def add_log(message: str):
    logs.append(message)

def get_logs():
    return logs

def clear_logs():
    global logs
    logs = []