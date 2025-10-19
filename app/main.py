import socket  # noqa: F401

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server created")
    server_socket.listen()
    print("Server listening on 127.0.0.1:6379")
    while True:
        connection, address = server_socket.accept()  # wait for client
        print(f"Connected by {address}")
        connection.sendall(b"+PONG\r\n")
        with connection:
            while True:
                message = connection.recv(1024)
                message = message.decode("utf-8")
                print(f"Received: {message}")
                for _ in range(message.count("PING")):
                    try:
                        connection.sendall(b"+PONG\r\n")
                    except BrokenPipeError:
                        print('Connection closed by user')
                        break




if __name__ == "__main__":
    main()
