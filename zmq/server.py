import time
import numpy as np
import zmq
import pickle

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.bind("tcp://*:5001")

message = np.ones(10 * (1024 ** 2))
data_stream = pickle.dumps(message)

while True:
    socket.send(data_stream)
    print(f"sent {len(data_stream)}")
