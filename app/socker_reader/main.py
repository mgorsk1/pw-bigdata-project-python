import websocket
from app.gcp_pubsub.publish import publish_message
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR

try:
    import thread
except ImportError:
    import _thread as thread

HOST = 'localhost'
PORT = 9999
ADDR = (HOST, PORT)
tcpSock = socket(AF_INET, SOCK_STREAM)
tcpSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
tcpSock.bind(ADDR)
tcpSock.listen(5)


class MeetupSocketHandler:
    url = "ws://stream.meetup.com/2/rsvps"

    @staticmethod
    def on_message(ws, message):
        publish_message("pw-bigdata-final-project", "meetup-rawdata", message)

    @staticmethod
    def on_error(ws, error):
        print(error)

    @staticmethod
    def on_close(ws):
        print("### closed ###")

    @staticmethod
    def on_open(ws):
        def run(*args):
            pass
            # time.sleep(120)
            # ws.close()
            # print("thread terminating...")

        thread.start_new_thread(run, ())


websocket.enableTrace(True)
ws = websocket.WebSocketApp(MeetupSocketHandler.url,
                            on_message=MeetupSocketHandler.on_message,
                            on_error=MeetupSocketHandler.on_error,
                            on_close=MeetupSocketHandler.on_close)

ws.on_open = MeetupSocketHandler.on_open
# @todo run only in selected time period (for example: only through whole january)
ws.run_forever()
