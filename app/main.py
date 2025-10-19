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
    counter = 0
    while True:
        connection, address = server_socket.accept()  # wait for client
        print(f"Connected by {address}")
        t = threading.Thread(target=run, args=(connection, address), daemon=True, name=f"Thread {counter}")
        t.start()
        current_threads.append(t)
        counter += 1
        print(f"{t.name} started", end="\n\n")


def run(connection, address):
    with connection:
        while True:
            message = connection.recv(1024)
            message = message.decode("utf-8")
            message.strip()
            if message == "":
                print(f"Connection closed by client on address {address}")
                break
            print("---------")
            print(f"Received: {message}From: {address}\n")
            for _ in range(message.count("PING")):
                response = b"+PONG\r\n"
                try:
                    connection.sendall(response)
                    print(f'Sent:{response.decode("utf-8")}')
                    print("---------")
                except BrokenPipeError:
                    print('Connection closed by user')
                    break




if __name__ == "__main__":
    main()
