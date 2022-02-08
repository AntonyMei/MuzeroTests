import time
import zmq
import pickle


def main():
    context = zmq.Context()

    #  Socket to talk to server
    print("Connecting to hello world server")
    socket = context.socket(zmq.PULL)
    socket.bind("tcp://10.200.3.112:11111")

    while True:
        string = socket.recv_string()
        print(string)


if __name__ == '__main__':
    main()
