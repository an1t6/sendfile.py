import socket
import threading
import logging

HOST = 'localhost'
PORT = 8000
FILES = {str(i): i for i in range(1, 10001)}
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

def log_print(message):
    logging.info(message)
    
def manage_request(client_socket, address):
    global is_running
    try:
        while is_running:
            request = client_socket.recv(1024).decode('utf-8')
            if request == SERVER_SHUTDOWN_MSG:
                log_print("서버 종료 요청 받음. 데이터 서버 종료 중...")
                is_running = False
                break

            if request in FILES:
                file_size = FILES[request]
                transfer_time = file_size / 3000
                client_socket.sendall(f"파일 {request}".encode('utf-8'))
                log_print(f"[dataserver] - 파일 {request} 전송 완료 - 소요된 시간 {int(transfer_time * 1000)}ms")
            else:
                client_socket.sendall("파일을 찾을 수 없음".encode('utf-8'))
    except socket.error:
        client_socket.close()

if __name__ == "__main__":
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(5)
    print("데이터 서버 실행 중...")
    
    try:
        while is_running:
            client_socket, address = server_socket.accept()
            threading.Thread(target=manage_request, args=(client_socket, address)).start()
    finally:
        print("데이터 서버 종료됨")
        server_socket.close()
