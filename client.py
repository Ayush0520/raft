import zmq
import sys


context = zmq.Context()
serverSocket = context.socket(zmq.DEALER)

def set(key, value):
    ip = input("Enter IP:")
    serverSocket.connect(f"tcp://{ip}:{9006}")
    message = {"type": "set", "msg": {key:value}}
    serverSocket.send_json(message)

    message = serverSocket.recv_json()
    print(f"Status: {message['status']}")


def get(key):
    ip = input("Enter IP:")
    serverSocket.connect(f"tcp://{ip}:{9006}")
    message = {"type": "get", "key": key}
    serverSocket.send_json(message)

    message = serverSocket.recv_json()
    print(message)

if __name__ == "__main__":
    while True:
        print("1. Set Key")
        print("2. Get Value")
        print("3. Exit Program")
        op = input("Enter the Operation you want to perform(1/2/3):")

        if op == "1":
            key = input("Enter Key:")
            value = input("Enter Value:")
            set(key, value)
        elif op == "2":
            key = input("Enter Key:")
            get(key)
        elif op == "3":
            sys.exit(0)
        else:
            print("Incorrect Choice. Try Again.")