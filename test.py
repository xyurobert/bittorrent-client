import socket

HOST = socket.gethostbyname(socket.gethostname())

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('10.104.65.156', 6885))  # Use the correct IP address of the server

print(HOST)
print("connected!")
data = sock.recv(1024)
print(f"Received: {data.decode('utf-8')}")
sock.close()