import time
import numpy as np
import zmq
import pickle

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.connect("tcp://10.200.3.112:11111")

while True:
    message = input()
    socket.send_string(message)
