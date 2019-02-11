import asyncio

DATA = {}


class ServerError(Exception):
    pass


class ServerSocketError(ServerError):
    pass


class ServerProtocolError(ServerError):
    pass


class ServerProtocol(asyncio.Protocol):

    def __init__(self):
        self.transport = None

    def connection_made(self, transport: asyncio.Transport):
        self.transport = transport

    def data_received(self, string: bytes):
        """This function read, processes and executes request from client"""
        data = string.decode()
        status, result = data.split(" ", 1)
        result = result.strip()
        if status == 'get':
            res = self._answer(result)
        elif status == 'put':
            res = self._put(result)
        else:
            res = 'error\nwrong command\n\n'
        self.transport.write(res.encode())

    @staticmethod
    def _put(put_string):
        """This function put data from client into the server"""
        for row in put_string.split('\n'):
            key, value, timestamp = row.split()
            if key not in DATA:
                DATA[key] = []
            DATA[key].append((int(timestamp), float(value)))
        return 'ok\n\n'

    @staticmethod
    def _answer(key):
        """This function give data from server to client"""
        if key == ' ' or key == '*':
            result = 'ok\n'
            for key in DATA:
                for tup in DATA[key]:
                    result = result + key + ' ' + str(tup[1]) + ' ' + str(tup[0]) + '\n'
            result += '\n'
            return result
        elif key not in DATA:
            return 'ok\n\n'
        else:
            result = 'ok\n'
            for tup in DATA[key]:
                result = result + key + ' ' + str(tup[1]) + ' ' + str(tup[0]) + '\n'
            result += '\n'
            return result


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ServerProtocol, host, int(port))
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()


def _main():
    host = '127.0.0.1'
    port = 8888
    run_server(host, port)


if __name__ == '__main__':
    _main()
