import random
import time 
import logging
import socket
import threading


HOST = 'ec2-43-201-72-135.ap-northeast-2.compute.amazonaws.com'
CACHE_PORT = [8001, 8002]
DATA_PORT = 8000
CLIENTS = 4
REQUEST_MAX_SIZE = 1000
TOTAL_RESULT = 0
lock = threading.Lock()

def set_channel(port):
    if port == 8000:
        return "dataserver"
    elif port == 8001:
        return "cache1"
    elif port == 8002:
        return "cache2"
    
def set_logging(file):
    logger = logging.getLogger(file)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(file, mode='w', encoding='utf-8')
    logger.addHandler(handler)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    return logger

def connect_server(file_num, port, log):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((HOST, port))
            sock.sendall(file_num.encode('utf-8'))
            response = sock.recv(1024).decode('utf-8')
            channel = set_channel(port)

            if "전송 완료" in response:
                if port == DATA_PORT:
                    transfer_time = int(file_num) / 3000  
                else:
                    transfer_time = int(file_num) / 1000 
                log.info(f"[{channel}] ㅣ 파일 {file_num} ㅣ 크기 {file_num}KB ㅣ {response} ㅣ 전송 시간 {int(transfer_time * 1000)}ms ㅣ")
                return "전송 완료", response, int(file_num), transfer_time
            elif "not found" in response:
                log.info(f"[{channel}] ㅣ 파일 {file_num} ㅣ 크기 {file_num}KB ㅣ {response} ㅣ")
                return "not found", response, 0, 0
            else:
                if port == DATA_PORT:
                    transfer_time = int(file_num) / 3000 
                else:
                    transfer_time = int(file_num) / 1000  
                log.info(f"[{channel}] ㅣ 파일 {file_num} ㅣ 크기 {file_num}KB ㅣ 전송 완료  ㅣ 전송 시간 {int(transfer_time * 1000)}ms ㅣ")
                return "전송 완료", response, int(file_num), transfer_time
    except socket.error:
        log.info(f"수신 실패")
        return None, None, 0, 0
    
def quit_server(log):
    for port in CACHE_PORT + [DATA_PORT]:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((HOST, port))
                sock.sendall("종료".encode('utf-8'))
        except socket.error:
            log.info(f"전송 실패")
            
def run_client(client_i):
    global TOTAL_RESULT
    log = set_logging(f'Client{client_i}.txt')
    overlap = set() 
    total_files = 0
    total_size = 0  
    total_time = 0 

    while total_files < REQUEST_MAX_SIZE:
        file_num = str(random.randint(1, 10001))
        if file_num in overlap: 
            continue 
        overlap.add(file_num)
        cache_hit = False
        for port in CACHE_PORT:
            status, response, file_size, transfer_time = connect_server(file_num, port, log)
            if status == "전송 완료":
                total_files += 1
                total_size += file_size
                total_time += transfer_time
                cache_hit = True
                break
        if not cache_hit:
            status, response, file_size, transfer_time = connect_server(file_num, DATA_PORT, log)
            if status == "전송 완료":
                total_files += 1
                total_size += file_size
                total_time += transfer_time

    avg_speed = total_size / total_time if total_time > 0 else 0  
    log.info(f"전송 성공한 파일 수 : {total_files}, 총 전송받은 파일 크기: {total_size}KB, 평균 전송 속도: {avg_speed:.2f}KB/s")
    
    with lock:
        TOTAL_RESULT += total_files
        if TOTAL_RESULT >= CLIENTS * REQUEST_MAX_SIZE: 
            print("모든 클라이언트가 파일을 전송받았음으로 각 서버에게 종료 신호를 전송")
            quit_server(log)

if __name__ == "__main__":
    threads = []
    
    for i in range(1, CLIENTS + 1):
        client_thread = threading.Thread(target=run_client, args=(i,))
        threads.append(client_thread)
        client_thread.start()
    for thread in threads:
        thread.join()
