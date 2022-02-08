import time
import zmq
import pickle


def main():
    context = zmq.Context()

    #  Socket to talk to server
    print("Connecting to hello world server")
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://10.200.13.18:10010")

    start = time.time()
    counter = 0
    while True:
        counter += 1
        string = socket.recv()
        if counter == 50:
            break
        print(counter)
    end = time.time()
    print((50 * 80) / (end - start))


if __name__ == '__main__':
    main()
