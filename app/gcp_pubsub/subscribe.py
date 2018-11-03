import click

from google.cloud import pubsub_v1
from time import sleep, strftime, localtime
from os import environ
from json import loads, JSONDecodeError
from time import time

from app.elasticsearch.client import ElasticDailyIndexManager
from config import BASE_PATH

environ['GOOGLE_APPLICATION_CREDENTIALS'] = "{}/config/keys/gcp/key.json".format(BASE_PATH)


class PubSubSubscriber:
    def __init__(self, project_id_arg, topic_name_arg, seconds_arg=None):
        self.elasticsearch_index_manager = ElasticDailyIndexManager(topic_name_arg)

        self.project_id = project_id_arg
        self.topic_name = topic_name_arg

        self.client = pubsub_v1.SubscriberClient()

        self.counter = 0

        self.seconds = seconds_arg

    def receive_and_index(self):
        subscription_path = self.client.subscription_path(self.project_id,
            "{}-subscription-elastic".format(self.topic_name))

        def callback(message):
            document = PubSubSubscriber.struct_message(message.data)

            self.elasticsearch_index_manager.queue.put((document, message.message_id))

            message.ack()

            self.counter += 1

        self.client.subscribe(subscription_path, callback=callback)

        print('Listening for messages on {}'.format(subscription_path))

        # if seconds specified, run only for given time. if not - run indefinitely
        if self.seconds:
            print("Running for {} seconds...".format(self.seconds))
            sleep(self.seconds)

            time_queue_join_start = time()
            self.elasticsearch_index_manager.queue.join()
            time_queue_join_stop = time()

            self.seconds = self.seconds + time_queue_join_stop - time_queue_join_start

            print("Read {} messages in {:.2f} seconds. That is {:.2f} mps!".format(self.counter, self.seconds,
                                                                                   self.counter / self.seconds))
        else:
            print("Running forever...")
            while True:
                sleep(60)

    @staticmethod
    def struct_message(message_arg, encoding='utf-8'):
        if isinstance(message_arg, dict):
            message = message_arg
        elif isinstance(message_arg, bytes):
            message = PubSubSubscriber.message_to_dict(message_arg.decode(encoding))
        elif isinstance(message_arg, str):
            message = PubSubSubscriber.message_to_dict(message_arg)
        else:
            message = None

        group_topics = message.get("group", dict()).get("group_topics", dict())

        if group_topics:
            message['group']['group_topics'] = [d['topic_name'] for d in message['group']['group_topics']]

        # time handling
        event_time = PubSubSubscriber.epoch_to_strtime(message.get("event", dict()).get("time", None))
        if event_time:
            message['event']['time'] = event_time

        mtime = PubSubSubscriber.epoch_to_strtime(message.get("mtime", None))
        if mtime:
            message['mtime'] = mtime

        # geo handling
        group_geo_lat = message.get("group", dict()).get("group_lat", None)
        group_geo_lon = message.get("group", dict()).get("group_lon", None)

        if group_geo_lon and group_geo_lat:
            message['group']['group_geo'] = PubSubSubscriber.create_geo_object(group_geo_lat, group_geo_lon)

        venue_geo_lat = message.get("venue", dict()).get("lat", None)
        venue_geo_lon = message.get("venue", dict()).get("lon", None)

        if venue_geo_lon and venue_geo_lat:
            message['venue']['venue_geo'] = PubSubSubscriber.create_geo_object(venue_geo_lat, venue_geo_lon)

        return message

    @staticmethod
    def epoch_to_strtime(epoch_time):
        try:
            result = strftime('%Y-%m-%dT%H:%M:%S', localtime(epoch_time / 1000))
        except:
            result = epoch_time

        return result

    @staticmethod
    def create_geo_object(lat, lon):
        return "{}, {}".format(str(lat), str(lon))

    @staticmethod
    def message_to_dict(message_arg):
        keep_going = True
        result = message_arg

        while keep_going and (not isinstance(result, dict)):
            try:
                result = loads(result)
            except JSONDecodeError:
                result = None
                keep_going = False

        return result


@click.command()
@click.option('--project-id', '-p', required=True, type=str, help='Google Cloud Platform Project Id')
@click.option('--topic', '-t', required=True, type=str, help='Pub/Sub Topic from which messages will be read')
@click.option('--seconds', '-s', default=None, required=False, type=int, help='For how long to read messages. If not provided - run forever')
def run(project_id, topic, seconds):
    pss = PubSubSubscriber(project_id, topic, seconds)
    pss.receive_and_index()


if __name__ == '__main__':
    run()
