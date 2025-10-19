import socket  # noqa: F401
import threading

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage

    #server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket = socket.create_server(("localhost", 6379))
    print("Server created")
    server_socket.listen()
    print("Server listening on 127.0.0.1:6379")

    current_threads = []
    for i in range(5):
        t = threading.Thread(target=run, args=(server_socket, ), daemon=True)
        t.start()
        print(f"Thread {i} started")
        current_threads.append(t)

    for thread in range(len(current_threads)):
        current_threads[thread].join()
        print(f"Thread {thread} joined")
        print(f"Thread {i} finished")


def run(server_socket):
    while True:
        connection, address = server_socket.accept()  # wait for client
        print(f"Connected by {address}")
        # connection.sendall(b"+PONG\r\n")
        with connection:
            while True:
                message = connection.recv(1024)
                message = message.decode("utf-8")
                if message == "":
                    print("Connection closed by client")
                    break
                print(f"Received: {message}")
                for _ in range(message.count("PING")):
                    try:
                        connection.sendall(b"+PONG\r\n")
                    except BrokenPipeError:
                        print('Connection closed by user')
                        break




if __name__ == "__main__":
    main()
