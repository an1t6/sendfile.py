import random
import socket
import threading
import logging

HOST = 'localhost'
CACHE_PORT = [8001, 8002]
DATA_PORT = 8000
REQUESTS = 1000
CLIENTS = 4
SERVER_SHUTDOWN_MSG = "SERVER_SHUTDOWN"

def set_logging(file):
    logger = logging.getLogger(file)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(file, mode='w', encoding='utf-8')
    logger.addHandler(handler)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    return logger

def port_for_name(port):
    if port == 8000:
        return "dataserver"
    elif port == 8001:
        return "cache1"
    elif port == 8002:
        return "cache2"

def request_file_from_server(file_name, port, log):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((HOST, port))
            sock.sendall(file_name.encode('utf-8'))
            response = sock.recv(1024).decode('utf-8')
            server_name = port_for_name(port)

            if "전송 완료" in response:
                transfer_time = int(file_name) / 1000 if port != DATA_PORT else int(file_name) / 3000
                log.info(f"[{server_name}] - 파일 {file_name} {response} - 전송 시간 {int(transfer_time * 1000)}ms")
                return "전송 완료", response
            elif "not found" in response:
                log.info(f"[{server_name}] - 파일 {file_name} {response}")
                return "not found", response
            else:
                log.info(f"[{server_name}] - 파일 {file_name} 알 수 없는 응답")
                return None, response
    except socket.error:
        log.info(f"{port_for_name(port)}에서 {file_name} 수신 실패")
        return None, None

def simulate_client(client_id):
    log = set_logging(f'client{client_id}.txt')
    requested_files = set()
    for _ in range(REQUESTS):
        file_name = str(random.randint(1, 10000))
        if file_name in requested_files:
            continue
        requested_files.add(file_name)
        
        cache_hit = False
        for port in CACHE_PORT:
            status, response = request_file_from_server(file_name, port, log)
            if status == "전송 완료":
                cache_hit = True
                break
        if not cache_hit:
            request_file_from_server(file_name, DATA_PORT, log)

    # 모든 요청이 끝나면 서버 종료 요청 보내기
    shutdown_servers()

def shutdown_servers():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # 데이터 서버 종료 요청
        sock.connect((HOST, DATA_PORT))
        sock.sendall(SERVER_SHUTDOWN_MSG.encode('utf-8'))
    
    for port in CACHE_PORT:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # 캐시 서버 종료 요청
            sock.connect((HOST, port))
            sock.sendall(SERVER_SHUTDOWN_MSG.encode('utf-8'))

if __name__ == "__main__":
    # 클라이언트 실행 코드
    threads = []
    for i in range(1, CLIENTS + 1):
        client_thread = threading.Thread(target=simulate_client, args=(i,))
        threads.append(client_thread)
        client_thread.start()

    for thread in threads:
        thread.join()

    print("모든 클라이언트 작업 완료, 서버 종료 요청 중...")
