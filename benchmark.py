import socket
import time

def run_benchmark():
    # 1. Connect to our RedisX server on port 6379
    server_address = ('127.0.0.1', 6379)
    print("Connecting to RedisX server at 127.0.0.1:6379...")
    
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(server_address)
    except Exception as e:
        print(f"Error: Could not connect to server. Make sure it is running! ({e})")
        return

    # Total requests we want to send
    num_requests = 10000
    print(f"Connected! Starting stress test with {num_requests} requests...\n")

    # ==========================================
    # TEST 1: SET COMMANDS
    # ==========================================
    start_time = time.time()
    for i in range(num_requests):
        # We send standard RESP array for: SET key<i> val<i>
        # format: *3\r\n$3\r\nSET\r\n$4\r\nkeyX\r\n$4\r\nvalX\r\n
        key = f"key{i}"
        val = f"val{i}"
        cmd = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
        s.sendall(cmd.encode())
        
        # Read the response from our server
        s.recv(1024)
        
    end_time = time.time()
    duration = end_time - start_time
    rps = num_requests / duration
    print(f"[SET Test Results]")
    print(f"  Total Time Taken : {duration:.4f} seconds")
    print(f"  Throughput       : {rps:.2f} requests/second\n")

    # ==========================================
    # TEST 2: GET COMMANDS
    # ==========================================
    start_time = time.time()
    for i in range(num_requests):
        # We send standard RESP array for: GET key<i>
        # format: *2\r\n$3\r\nGET\r\n$4\r\nkeyX\r\n
        key = f"key{i}"
        cmd = f"*2\r\n$3\r\nGET\r\n${len(key)}\r\n{key}\r\n"
        s.sendall(cmd.encode())
        
        # Read the response from our server
        s.recv(1024)
        
    end_time = time.time()
    duration = end_time - start_time
    rps = num_requests / duration
    print(f"[GET Test Results]")
    print(f"  Total Time Taken : {duration:.4f} seconds")
    print(f"  Throughput       : {rps:.2f} requests/second\n")

    s.close()
    print("Benchmark complete!")

if __name__ == "__main__":
    run_benchmark()
