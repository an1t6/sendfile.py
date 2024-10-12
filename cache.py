import socket
import threading
import logging
import random

HOST = 'localhost'
CACHE_PORT = [8001, 8002]
data_server_port = 8000
cache1, cache2 = {}, {}
MAX_SIZE = 200 * 1024
SERVER_SHUTDOWN_MSG = "SERVER_SHUTDOWN"
is_running = True

def set_logging(file):
    logger = logging.getLogger(file)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(file, mode='w', encoding='utf-8')
    logger.addHandler(handler)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    return logger

def manage_client(client_socket, log, cache, cache_name, other_cache):
    global is_running
    try:
        while is_running:
            request = client_socket.recv(1024).decode('utf-8')
            if request == SERVER_SHUTDOWN_MSG:
                log.info(f"{cache_name} 서버 종료 요청 받음")
                is_running = False
                break

            if request in cache:
                transfer_time = int(request) / 1000
                client_socket.sendall(f"전송 완료".encode('utf-8'))
                log.info(f"[{cache_name}] - 파일 {request} 전송 완료 - 소요된 시간 {int(transfer_time * 1000)}ms")
            else:
                client_socket.sendall("not found".encode('utf-8'))
                log.info(f"[{cache_name}] - 파일 {request} 찾지 못함 - 데이터 서버로 요청")
    except socket.error:
        client_socket.close()

def run_cacheserver(port, log, cache, cache_name, other_cache):
    global is_running
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, port))
    server_socket.listen(5)
    print(f"{cache_name} 서버 포트 {port}에서 실행 중")

    try:
        while is_running:
            client_socket, _ = server_socket.accept()
            threading.Thread(target=manage_client, args=(client_socket, log, cache, cache_name, other_cache)).start()
    finally:
        print(f"{cache_name} 서버 종료됨")
        server_socket.close()

def main():
    log1 = set_logging('cache1.txt')
    log2 = set_logging('cache2.txt')
    threading.Thread(target=run_cacheserver, args=(8001, log1, cache1, 'Cache1', cache2)).start()
    threading.Thread(target=run_cacheserver, args=(8002, log2, cache2, 'Cache2', cache1)).start()

if __name__ == "__main__":
    main()
