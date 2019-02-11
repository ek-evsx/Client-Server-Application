import asyncio

DATA = {}


class ServerError(Exception):
    pass


class ServerSocketError(ServerError):
    pass


class ServerProtocolError(ServerError):
    pass


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = asyncio.start_server(read, host, port)
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()


async def read(reader, writer):
    """This function read, processes and executes request from client"""
    while True:
        data = b''
        while not data.endswith(b"\n"):
            try:
                data += await reader.read(1024)
            except Exception as err:
                raise ServerSocketError("error recv data", err)
        data = data.decode()
        status, result = data.split(" ", 1)
        result = result.strip()
        if status == 'get':
            res = _answer(result)
        elif status == 'put':
            res = _put(result)
        else:
            res = 'error\nwrong command\n\n'
        writer.write(res.encode())


def _put(put_string):
    """This function put data from client into the server"""
    for row in put_string.split('\n'):
        key, value, timestamp = row.split()
        if key not in DATA:
            DATA[key] = []
        DATA[key].append((int(timestamp), float(value)))
    return 'ok\n\n'


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


def _main():
    host = '127.0.0.1'
    port = 8888
    run_server(host, port)


if __name__ == '__main__':
    _main()
