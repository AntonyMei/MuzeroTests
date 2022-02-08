import time
import zmq
import pickle


def main():
    context = zmq.Context()

    #  Socket to talk to server
    print("Connecting to hello world server")
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://10.200.3.112:10003")

    start = time.time()
    counter = 0
    while True:
        counter += 1
        string = socket.recv()
        if counter == 1000:
            break
        print(counter)
    end = time.time()
    print((1000 * 80) / (end - start))


if __name__ == '__main__':
    main()
