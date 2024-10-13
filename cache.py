import random
import logging
import socket
import threading
import os

HOST = 'localhost'
DATA_PORT = 8000
MAX_SIZE = 200 * 100
CACHE1, CACHE2 = {}, {}
RESULT1, RESULT2 = 0, 0
lock = threading.Lock()

def set_logging(file):
    logger = logging.getLogger(file)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(file, mode='w', encoding='utf-8')
    logger.addHandler(handler)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    return logger

def fetch_file(file_data, log, cache_name):
    global RESULT1, RESULT2
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((HOST, DATA_PORT))
            sock.sendall(f"Cache - {file_data}".encode('utf-8'))
            response = sock.recv(1024).decode('utf-8')

            if response.startswith("파일"):
                file_data = int(file_data)  
                send_time = file_data / 2000  
                with lock:
                    if cache_name == 'CACHE1':
                        RESULT1 += file_data
                    elif cache_name == 'CACHE2':
                        RESULT2 += file_data
                log.info(f"[{cache_name}] ㅣ 파일 {file_data} 저장 완료 ㅣ 크기 {file_data}KB ㅣ 소요된 시간 {int(send_time * 1000)}ms ㅣ")
            else:
                log.info(f"데이터 서버에서 {file_data} 가져오기 실패: {response}")
    except (ValueError, socket.error) as e:
        log.error(f"데이터 서버와의 통신 오류: {str(e)}")

def prefetch(log1, log2):
    global RESULT1, RESULT2
    file_num = list(range(1, 1000)) 
    random.shuffle(file_num) 

    while file_num:
        file_data = str(file_num.pop())
        if RESULT1 + int(file_data) <= MAX_SIZE:
            fetch_file(file_data, log1, 'CACHE1') # 데이터 서버로 파일 요청
            CACHE1[file_data] = True  # 프리패치된 파일을 CACHE1에 저장
        elif RESULT2 + int(file_data) <= MAX_SIZE:
            fetch_file(file_data, log2, 'CACHE2')  # 데이터 서버로 파일 요청
            CACHE2[file_data] = True  # 프리패치된 파일을 CACHE2에 저장
    log1.info(f"[CACHE1] 저장됨. 현재 크기: {RESULT1}KB")
    log2.info(f"[CACHE2] 저장됨. 현재 크기: {RESULT2}KB")


def manage_client(client_socket, log, cache, cache_name, other_cache):
    try:
        while True:
            request = client_socket.recv(1024).decode('utf-8').strip() 
            if not request:
                continue
            if request in cache:  
                send_time = int(request) / 1000
                client_socket.sendall(f"전송 완료".encode('utf-8'))
                log.info(f"[{cache_name}] ㅣ 파일 {request} 전송 완료     ㅣ      소요된 시간 {int(send_time * 1000)}ms     ㅣ")
            else:
                if request:
                    client_socket.sendall("not found".encode('utf-8'))
                    log.info(f"[{cache_name}] ㅣ 파일 {request} is not found ㅣ >> 요청을 Dataserver로 전송 ㅣ")
                else:
                    log.error(f"[{cache_name}] ㅣ 유효하지 않은 파일 요청 수신")
            if request == "종료":
                print(f"Cacheserver 종료")
                os._exit(0)
    except socket.error:
        client_socket.close()

def run_cacheserver(port, log, cache, cache_name, other_cache): 
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, port))
    server_socket.listen(5)
    print(f"{cache_name} 실행")
    try:
        while True:
            client_socket, _ = server_socket.accept()
            threading.Thread(target=manage_client, args=(client_socket, log, cache, cache_name, other_cache)).start()
    finally:
        server_socket.close()
        print(f"{cache_name} 서버가 종료되었습니다.")

def main():
    log1 = set_logging('CACHE1.txt')
    log2 = set_logging('CACHE2.txt')
    prefetch(log1, log2)
    threading.Thread(target=run_cacheserver, args=(8001, log1, CACHE1, 'CACHE1', CACHE2)).start()  # cache 1,2를 스레드로 실행
    threading.Thread(target=run_cacheserver, args=(8002, log2, CACHE2, 'CACHE2', CACHE1)).start()

if __name__ == "__main__":
    main()
