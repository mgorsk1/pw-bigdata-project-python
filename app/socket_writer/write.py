from socket import *
import time
from config import BASE_PATH
from json import loads, dumps
import random

with open("{}/tmp/meetups.json".format(BASE_PATH), 'r') as r:
    meetups = loads(r.read())

HOST = 'localhost'
PORT = 9999
ADDR = (HOST, PORT)
tcpSock = socket(AF_INET, SOCK_STREAM)
tcpSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
tcpSock.bind(ADDR)
tcpSock.listen(5)


while True:
    error = False
    c, addr = tcpSock.accept()
    print('got connection')
    while not error:
        batch_size = random.randint(1, 5)
        wait_for = random.randint(1, 10)
        print("batch_size", batch_size, "wait_for", wait_for)
        for i in range(batch_size):
            try:
                number = random.randint(1, len(meetups['data']))
                message = (dumps(meetups['data'][number])+"\n").encode()

                c.send(message)
                time.sleep(wait_for)
            except Exception as e:
                print(e.args)
                error = True
                break
