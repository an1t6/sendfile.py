import random
import time 
import logging
import socket
import threading
import os

HOST = 'ec2-43-201-72-135.ap-northeast-2.compute.amazonaws.com'
DATA_PORT = 8000
MAX_SIZE = 200 * 1024
CACHE1, CACHE2 = {}, {}
RESULT1, RESULT2 = 0, 0
TOTAL_FILES1, TOTAL_FIELS2 = 0, 0 
TOTAL_SIZE1, TOTAL_SIZE2 = 0, 0  
TOTAL_TIME1, TOTAL_TIME2 = 0, 0 
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
    file_num = list(range(1, 10001)) 
    random.shuffle(file_num) 

    while file_num:
        file_data = str(file_num.pop())
        if RESULT1 + int(file_data) <= MAX_SIZE:
            fetch_file(file_data, log1, 'CACHE1') 
            CACHE1[file_data] = True  
        elif RESULT2 + int(file_data) <= MAX_SIZE:
            fetch_file(file_data, log2, 'CACHE2')  
            CACHE2[file_data] = True 
    log1.info(f"[CACHE1] 저장됨. 현재 크기: {RESULT1}KB")
    log2.info(f"[CACHE2] 저장됨. 현재 크기: {RESULT2}KB")


def manage_client(client_socket, log, cache, cache_name, other_cache):
    global TOTAL_FILES1, TOTAL_FIELS2, TOTAL_SIZE1, TOTAL_SIZE2, TOTAL_TIME1, TOTAL_TIME2
    try:
        print(f"클라이언트가 {cache_name}에 연결되었습니다.")
        while True:
            request = client_socket.recv(1024).decode('utf-8').strip()
            if not request:
                continue
            if request in cache:
                start_time = time.time()
                send_time = int(request) / 1000
                client_socket.sendall(f"전송 완료".encode('utf-8'))
                end_time = time.time() 
                duration = end_time - start_time 
                
                with lock:
                    if cache_name == 'CACHE1':
                        TOTAL_FILES1 += 1
                        TOTAL_SIZE1 += int(request)
                        TOTAL_TIME1 += duration
                        avg_speed = TOTAL_SIZE1 / TOTAL_TIME1 if TOTAL_TIME1 > 0 else 0
                        log.info(f"[{cache_name}] ㅣ 파일 {request} 전송 완료 ㅣ 소요된 시간 {int(send_time * 1000)}ms ㅣ")
                        log.info(f"[{cache_name}] 총 전송한 파일 수: {TOTAL_FILES1}, 총 전송한 파일 크기: {TOTAL_SIZE1}KB, 평균 전송 속도: {avg_speed:.2f}KB/s")
                    elif cache_name == 'CACHE2':
                        TOTAL_FIELS2 += 1
                        TOTAL_SIZE2 += int(request)
                        TOTAL_TIME2 += duration
                        avg_speed = TOTAL_SIZE2 / TOTAL_TIME2 if TOTAL_TIME2 > 0 else 0
                        log.info(f"[{cache_name}] ㅣ 파일 {request} 전송 완료 ㅣ 소요된 시간 {int(send_time * 1000)}ms ㅣ")
                        log.info(f"[{cache_name}] 총 전송한 파일 수: {TOTAL_FIELS2}, 총 전송한 파일 크기: {TOTAL_SIZE2}KB, 평균 전송 속도: {avg_speed:.2f}KB/s")
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

def main():
    log1 = set_logging('Cache1.txt')
    log2 = set_logging('Cache2.txt')
    prefetch(log1, log2)

    server_socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket1.bind((HOST, 8001))
    server_socket1.listen(5)
    print("CACHE1 실행 중")
    
    server_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket2.bind((HOST, 8002))
    server_socket2.listen(5)
    print("CACHE2 실행 중")

    try:
        # 두 서버에 대해 각각 스레드를 시작하여 클라이언트 관리
        def accept_clients(server_socket, log, cache, cache_name, other_cache):
            while True:
                client_socket, _ = server_socket.accept()
                threading.Thread(target=manage_client, args=(client_socket, log, cache, cache_name, other_cache)).start()

        threading.Thread(target=accept_clients, args=(server_socket1, log1, CACHE1, 'CACHE1', CACHE2)).start()
        threading.Thread(target=accept_clients, args=(server_socket2, log2, CACHE2, 'CACHE2', CACHE1)).start()

        # 메인 스레드가 종료되지 않도록 대기
        while True:
            pass

    finally:
        server_socket1.close()
        server_socket2.close()

if __name__ == "__main__":
    main()