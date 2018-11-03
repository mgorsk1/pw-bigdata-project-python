import click

from google.cloud import pubsub_v1
from json import dumps
from os import environ
from threading import Thread
from queue import Queue

from config import BASE_PATH, PROJECT_ID
from logging import log

environ['GOOGLE_APPLICATION_CREDENTIALS'] = "{}/config/keys/gcp/key.json".format(BASE_PATH)


class ProcessFutures(Thread):
    def __init__(self, futures_queue):
        Thread.__init__(self)

        self.queue = futures_queue

        self.counter = 0

        self.results = list()

        self.daemon = True
        self.start()

    def run(self):
        while getattr(self, 'keep_going', True):
            future = self.queue.get()

            self.results.append(future.result())

            self.queue.task_done()


class PubSubPublisher:
    def __init__(self, project_id, topic_name):
        self.client = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

        self.keep_going = True
        self.futures_queue = Queue()
        self.future_process = ProcessFutures(self.futures_queue)

    def publish_message(self, message_body):
        """
        Publishes message to a Pub/Sub topic.

        future.result is verified in separate thread to avoid blocking of message publishing.
        """

        topic_path = self.client.topic_path(self.project_id, self.topic_name)

        if isinstance(message_body, dict):
            data = dumps(message_body)
        elif isinstance(message_body, str):
            data = message_body
        else:
            raise BaseException

        data = data.encode('utf-8')

        future = self.client.publish(topic_path, data=data)

        self.futures_queue.put(future)

    def finish(self):
        self.future_process.queue.join()

        print("Processed results: " + str(len(self.future_process.results)))


@click.command()
@click.option('--project-id', '-p', required=True, type=str, help='Google Cloud Platform Project Id')
@click.option('--topic', '-t', required=True, type=str, help='Pub/Sub Topic to which messages will be published')
@click.option('--message', '-m', required=True, type=str, help='Message body')
def run(project, topic, message):
    psp = PubSubPublisher(project, topic)
    psp.publish_message(message)
    psp.finish()


if __name__ == '__main__':
    run()
