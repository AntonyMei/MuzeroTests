import time
import zmq
import pickle


class Node:
    def __init__(self):
        self.a = 1
        self.b = 2


context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:5000")

while True:
    #  Wait for next request from client
    message = input()
    socket.send_string(message)
