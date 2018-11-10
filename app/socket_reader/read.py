import websocket
import click

from app.gcp_pubsub.publish import PubSubPublisher
from time import sleep

from app.logger import log


try:
    import thread
except ImportError:
    import _thread as thread


class WebsocketToPubSubEmitter:
    """
    Class responsible for reading data from any Websocket (ws_url_arg) and sending it to pub/sub topic using
    PubSubPublisher class.
    """
    def __init__(self, ws_url_arg, project_id_arg, topic_arg, seconds_arg=None):
        self.publisher = PubSubPublisher(project_id_arg, topic_arg)
        self.url = ws_url_arg
        self.seconds = seconds_arg

        self.ws = None

        self.run()

    def run(self):
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(self.url,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

        self.ws.counter = 0

        self.ws.on_open = self.on_open

        log.log_info("{} - Starting listening on: {} Messages will be published to: {}".format(
                self.__class__.__name__, self.url, str(self.publisher)))

        self.ws.run_forever()

    def on_message(self, message):
        self.publisher.publish_message(message)
        log.log_debug("{} - Acquired message: {}".format(self.__class__.__name__, message))

        self.ws.counter += 1

    def on_error(self, error):
        print(error)

    def on_close(self):
        log.log_info("{} - Websocket closed".format(self.__class__.__name__))

    def on_open(self):
        def run(*args):
            if self.seconds:
                log.log_info("{} - Running for {} seconds".format(self.__class__.__name__, self.seconds))
                sleep(self.seconds)
                self.ws.close()

                log.log_info("{} - Published {} messages in {} seconds. That is {:.2f} mps!".format(
                               self.__class__.__name__, self.ws.counter, self.seconds, self.ws.counter/self.seconds))

            else:
                log.log_info("{} - Running forever".format(self.__class__.__name__, self.seconds))
                while True:
                    pass

        thread.start_new_thread(run, ())

    def return_count(self):
        return self.ws.counter


@click.command()
@click.option('--url', required=True, type=str, help='WebSocket data source url')
@click.option('--project-id', required=True, type=str, help='Google Cloud Platform Project Id')
@click.option('--topic', required=True, type=str, help='Pub/Sub Topic to which messages will be published')
@click.option('--seconds', default=None, required=False, type=int, help='For how long to process messages. If not provided - run forever')
def run(url, project_id, topic, seconds):
    WebsocketToPubSubEmitter(url, project_id, topic, seconds)


if __name__ == '__main__':
    run()
