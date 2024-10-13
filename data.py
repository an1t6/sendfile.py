import logging
import socket
import threading
import os

HOST = 'localhost'
PORT = 8000
FILES = {str(i): i for i in range(1, 10001)}
CACHE_TOTAL_FILE = 0
CACHE_TOTAL_TIME = 0
CLIENT_TOTAL_FILE = 0
CLIENT_TOTAL_TIME = 0

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

def manage_cacheserver(file_num, log):
    global CACHE_TOTAL_FILE, CACHE_TOTAL_TIME
    if file_num in FILES:
        file_data = FILES[file_num]
        send_time = file_data / 2000  
        CACHE_TOTAL_FILE += 1
        CACHE_TOTAL_TIME += send_time
        
        log.info(f"[dataserver] ㅣ 파일 {file_num} ㅣ 크기 {file_data}KB ㅣ 캐시 서버로 전송됨 ㅣ 소요된 시간 {int(send_time * 1000)}ms ㅣ")
        return f"파일 {file_num} 전송 완료"
    else:
        log.info(f"{file_num}")
        return "not found"

def manage_clinet(file_num, log):
    global CLIENT_TOTAL_FILE, CLIENT_TOTAL_TIME
    if file_num in FILES:
        file_data = FILES[file_num]
        send_time = file_data / 3000  
        CLIENT_TOTAL_FILE += 1
        CLIENT_TOTAL_TIME += send_time
        
        log.info(f"[dataserver] ㅣ 파일 {file_num} ㅣ 크기 {file_data}KB ㅣ 클라이언트로 전송됨 ㅣ 소요된 시간 {int(send_time * 1000)}ms ㅣ")
        return f"파일 {file_num} 전송 완료"
    else:
        log.info(f"{file_num}")
        return "not found"

def run_dataserver(client_socket, address, log):
    try:
        while True:
            request = client_socket.recv(1024).decode('utf-8').strip()
            if not request:
                break  
            log.info(f"{request} 파일 요청")
            if request.startswith("Cache"):
                file_num = request.split(" - ")[1]  
                response = manage_cacheserver(file_num, log)
            else:
                response = manage_clinet(request, log)
            if request == "종료":
                log.info(f"캐시 서버로 전송된 파일 총 개수: {CACHE_TOTAL_FILE}, 총 전송 시간: {int(CACHE_TOTAL_TIME * 1000)}ms")
                log.info(f"클라이언트로 전송된 파일 총 개수: {CLIENT_TOTAL_FILE}, 총 전송 시간: {int(CLIENT_TOTAL_TIME * 1000)}ms")
                os._exit(0)
            client_socket.sendall(response.encode('utf-8'))
    except socket.error as e:
        log.error(f"Socket error: {str(e)}")
    finally:
        client_socket.close()  

if __name__ == "__main__":
    
    log = set_logging('data.txt')
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(5)
    print("데이터 서버 실행 중")
    
    while True:
        client_socket, address = server_socket.accept()
        threading.Thread(target=run_dataserver, args=(client_socket, address, log)).start()
