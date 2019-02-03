import click

from os import environ
from json import dumps
from threading import Thread
from queue import Queue
from google.cloud import pubsub_v1
from message_proto import MSG

from config import BASE_PATH
from app.tools.logger import log


class PubSubPublisher:
    """Class responsible for publishing messages to GCP Pub/Sub topic.

    It processess future.result() in separate thread to increase throughput.

    Schema validation is performed using pyrobuf (implementation of proto buffers).
    Before running class make sure that proto file corresponding to shema of subscription data was compiled with
    pyrobuf and installed in Your venv (here we use message_proto.MSG() generalized library name).

    """
    def __init__(self, project_id, topic_name):
        environ['GOOGLE_APPLICATION_CREDENTIALS'] = "{}/config/keys/{}/key.json".format(BASE_PATH, project_id)

        self.client = pubsub_v1.PublisherClient()

        self.project_id = project_id
        self.topic_name = topic_name

        self.results_counter = 0

        self.topic_path = self.client.topic_path(self.project_id, self.topic_name)

    def publish_message(self, message_body):
        """
        Publishes message to a Pub/Sub topic.

        future.result is verified in separate thread to avoid blocking of message publishing.
        """

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
            log.log_warning("{} - Message didn't pass schema validation !\n{}".format(self.__class__.__name__, e.args))

            return False

        pb_message_serialized = pb_message.SerializeToString()

        future = self.client.publish(self.topic_path, data=pb_message_serialized)

        future.add_done_callback(PubSubPublisher.callback)

        self.results_counter += 1

        return True

    def finish(self):
        log.log_info("{} - Processed results: {}".format(self.__class__.__name__,
                                                         self.results_counter))

    @staticmethod
    def callback(message_future):
        if message_future.exception():
            log.log_warning('Publishing message on {} threw an Exception {}.'.format(topic_name,
                                                                                     message_future.exception()))
        else:
            pass

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

    msg_size = dumps(mockup_data).encode().__len__()

    time_start = time()

    for i in range(amount):
        psp.publish_message(mockup_data)

    psp.finish()

    time_stop = time()

    seconds = time_stop - time_start

    log.log_info("Published {} messages in {:.2f} seconds. That is {:.2f} m/s & {:.2f} MB/s!".format(amount,
                                                                                          seconds,
                                                                                          amount / seconds,
                                                                                          amount * msg_size / (seconds * 1024 * 1024)))


if __name__ == '__main__':
    run()
