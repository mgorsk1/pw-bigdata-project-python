import click

from os import environ
from json import dumps
from threading import Thread
from queue import Queue
from google.cloud import pubsub_v1
from message_proto import MSG

from config import BASE_PATH

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
            message_str = dumps(message_body)
        elif isinstance(message_body, str):
            message_str = message_body
        else:
            raise Exception

        try:
            pb_message = MSG()
            pb_message.ParseFromJson(message_str)
        except Exception as e:
            print("Message didn't pass schema validation !")
            print(message_str)
            print(e.args)

            return False

        pb_message_serialized = pb_message.SerializeToString()

        future = self.client.publish(topic_path, data=pb_message_serialized)

        self.futures_queue.put(future)

        return True

    def finish(self):
        self.future_process.queue.join()

        print("Processed results: " + str(len(self.future_process.results)))


@click.command()
@click.option('--project-id', required=True, type=str, help='Google Cloud Platform Project Id')
@click.option('--topic', required=True, type=str, help='Pub/Sub Topic to which messages will be published')
@click.option('--amount', required=True, type=int, help='How many messages to send')
def run(project_id, topic, amount):
    """
    Publishes batch of --amount test messages. --topic have corresponding json template in resources/mockups/templates

    :param project_id: Project on which topic is created
    :param topic: Name of topic to which publish messages
    :param amount: Number of messages to publish
    :return:
    """
    from time import time

    from app.tools.mockups import DictFromTemplate

    psp = PubSubPublisher(project_id, topic)

    mockup_data = DictFromTemplate(topic).generate()

    time_start = time()

    for i in range(amount):
        message_body = mockup_data
        psp.publish_message(message_body)

    psp.finish()

    time_stop = time()

    seconds = time_stop - time_start

    print("Published {} messages in {:.2f} seconds. That is {:.2f} mps!".format(amount, seconds,
                                                                                amount / seconds))


if __name__ == '__main__':
    run()
