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
    while True:
        connection, address = server_socket.accept()  # wait for client
        print(f"Connected by {address}")
        current_threads = []
        t = threading.Thread(target=run, args=(connection, address), daemon=True)
        t.start()
        current_threads.append(t)
        print(f"Thread {len(current_threads)} started")

        # for thread in range(len(current_threads)):
        #     current_threads[thread].join()
        #     print(f"Thread {thread} joined")
        #     print(f"Thread {i} finished")


def run(connection, address):
    with connection:
        while True:
            message = connection.recv(1024)
            message = message.decode("utf-8")
            message.strip()
            if message == "":
                print("Connection closed by client")
                break
            print(f"Received: {message} From: {address}\n")
            for _ in range(message.count("PING")):
                response = b"+PONG\r\n"
                try:
                    connection.sendall(response)
                    print(f'Sent:{response}')
                    print("---------")
                except BrokenPipeError:
                    print('Connection closed by user')
                    break




if __name__ == "__main__":
    main()
