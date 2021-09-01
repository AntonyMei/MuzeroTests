"""
This file is used for testing zmq queue functionality
"""
import time

import numpy as np
import zmq
import multiprocessing as mp

entry_size = 400*1024*1024
client_count = 16
server_count = 16
package_count = 50


def log2terminal(worker_type, worker_id, msg):
    print(f"[{worker_type} {worker_id}] {msg}")


def client_func(rank):

    #  Prepare our context and sockets
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5559")
    workload = np.ones(entry_size)
    log2terminal(worker_type="Client", worker_id=rank, msg=f"Client {rank} starts.")

    #  Do 5 requests, waiting each time for a response
    for request in range(package_count):
        workload[0] = rank
        workload[1] = request
        socket.send(workload.tobytes())
        log2terminal(worker_type="Client", worker_id=rank, msg=f"Send request {request}")
        _ = socket.recv()
        time.sleep(0.1)


def server_func(rank):

    context = zmq.Context()
    log2terminal(worker_type="Server", worker_id=rank, msg=f"Server {rank} starts.")
    socket = context.socket(zmq.REP)
    socket.connect("tcp://localhost:5560")

    while True:
        message = socket.recv()
        # workload = np.ndarray(buffer=message)
        # log2terminal(worker_type="Server", worker_id=rank,
        #              msg=f"Received: client {workload[0]}, message {workload[1]}")
        log2terminal(worker_type="Server", worker_id=rank, msg=f"Received")
        socket.send(b"0")


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

    # broker
    broker = ctx.Process(target=broker_func)
    broker.start()

    # servers
    server_list = [ctx.Process(target=server_func, args=(i, )) for i in range(server_count)]
    [server.start() for server in server_list]

    # clients
    time.sleep(1)
    s_time = time.time()
    client_list = [ctx.Process(target=client_func, args=(i, )) for i in range(client_count)]
    [client.start() for client in client_list]

    # clean up
    [client.join() for client in client_list]
    e_time = time.time()
    [server.terminate() for server in server_list]
    broker.terminate()
    print(f"Total time: {e_time - s_time}s")
    print(f"avg speed: {(client_count * package_count * entry_size * 8 / 1024 / 1024) / (e_time - s_time)} MB/s")


if __name__ == '__main__':
    main()
