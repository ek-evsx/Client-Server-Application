import socket
import time


class ClientError(Exception):
    pass


class ClientSocketError(ClientError):
    pass


class ClientProtocolError(ClientError):
    pass


class Client:

    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        try:
            self.sock = socket.create_connection((host, port), timeout)
        except socket.error as err:
            raise ClientSocketError("error create connection", err)

    def put(self, key, value, timestamp=int(time.time())):
        string = 'put' + ' ' + str(key) + ' ' + str(value) + ' ' + str(timestamp) + '\n'
        try:
            self.sock.sendall(string.encode('utf-8'))
            result = self.sock.recv(1024).decode('utf-8')
        except socket.error as err:
            raise ClientError("protocol error", err)

    def get(self, key):
        data = {}
        key = 'get' + ' ' + key + '\n'
        try:
            self.sock.sendall(key.encode('utf-8'))
        except socket.error as err:
            raise ClientSocketError("error send data", err)
        string = b''
        while not string.endswith(b"\n\n"):
            try:
                string += self.sock.recv(1024)
            except socket.error as err:
                raise ClientSocketError("error recv data", err)
        string = string.decode('utf-8')
        status, result = string.split("\n", 1)
        result = result.strip()
        if status == "error":
            raise ClientProtocolError(result)

        if result == '':
            return data
        for row in result.split('\n'):
            key, value, timestamp = row.split()
            if key not in data:
                data[key] = []
            data[key].append((int(timestamp), float(value)))

        return data

    def close(self):
        try:
            self.sock.close()
        except socket.error as err:
            raise ClientSocketError("error close connection", err)


def _main():
    client = Client("127.0.0.1", 8888, timeout=15)

    client.put("palm.cpu", 0.5, timestamp=1150864247)
    client.put("palm.cpu", 2.0, timestamp=1150864248)
    client.put("palm.cpu", 0.5, timestamp=1150864248)

    client.put("eardrum.cpu", 3, timestamp=1150864250)
    client.put("eardrum.cpu", 4, timestamp=1150864251)
    client.put("eardrum.memory", 4200000)

    print(client.get("*"))


if __name__ == "__main__":
    _main()
