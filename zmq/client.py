#
#   Hello World client in Python
#   Connects REQ socket to tcp://localhost:5555
#   Sends "Hello" to server, expects "World" back
#

import zmq
import pickle


class Node:
    def __init__(self):
        self.a = 1
        self.b = 2


def main():
    context = zmq.Context()

    #  Socket to talk to server
    print("Connecting to hello world server")
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://10.200.13.18:10002")
    zip_filter = ""
    socket.setsockopt_string(zmq.SUBSCRIBE, zip_filter)

    #  Do 10 requests, waiting each time for a response
    while True:
        string = socket.recv_string()
        print(string)


if __name__ == '__main__':
    main()
