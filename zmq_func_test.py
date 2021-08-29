"""
This file is used for testing zmq queue basic functionality
"""
import time

import zmq
import multiprocessing as mp


def log2terminal(worker_type, worker_id, msg):
    print(f"[{worker_type} {worker_id}] {msg}")


def client_func(rank):

    #  Prepare our context and sockets
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5559")
    log2terminal(worker_type="Client", worker_id=rank, msg=f"Client {rank} starts.")

    #  Do 10 requests, waiting each time for a response
    for request in range(1, 5):
        socket.send(bytes(f"Hello from client {rank}", encoding='utf8'))
        message = socket.recv()
        log2terminal(worker_type="Client", worker_id=rank, msg=f"Received reply {request} [{message}]")


def server_func(rank):

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect("tcp://localhost:5560")
    log2terminal(worker_type="Server", worker_id=rank, msg=f"Server {rank} starts.")

    while True:
        message = socket.recv()
        log2terminal(worker_type="Server", worker_id=rank, msg=f"Received request: {message}")
        socket.send(bytes(f"World from server {rank}", encoding='utf8'))


def broker_func():
    context = zmq.Context()

    # Socket facing clients
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://*:5559")

    # Socket facing services
    backend = context.socket(zmq.DEALER)
    backend.bind("tcp://*:5560")

    log2terminal(worker_type="Broker", worker_id=0, msg="Broker starts.")
    zmq.proxy(frontend, backend)

    # We never get here...
    frontend.close()
    backend.close()
    context.term()


def main():

    ctx = mp.get_context('spawn')
    broker = ctx.Process(target=broker_func)
    broker.start()
    server_list = [ctx.Process(target=server_func, args=(i, )) for i in range(2)]
    [server.start() for server in server_list]
    time.sleep(1)
    client_list = [ctx.Process(target=client_func, args=(i, )) for i in range(3)]
    [client.start() for client in client_list]


if __name__ == '__main__':
    main()
