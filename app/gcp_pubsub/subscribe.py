import click

from google.cloud import pubsub_v1
from time import sleep, strftime, localtime
from os import environ
from json import loads, JSONDecodeError
from time import time
from threading import Thread

from app.elasticsearch.client import ElasticDailyIndexManager
from config import BASE_PATH

environ['GOOGLE_APPLICATION_CREDENTIALS'] = "{}/config/keys/gcp/key.json".format(BASE_PATH)


class PubSubSubscriber(Thread):
    def __init__(self, project_id_arg, topic_name_arg, seconds_arg=None):
        Thread.__init__(self)

        self.elastic_managers = environ.get("ElASTIC_MANAGERS", 1)
        self.elasticsearch_index_managers = list()

        for _ in range(self.elastic_managers):
            self.elasticsearch_index_managers.append(ElasticDailyIndexManager(topic_name_arg))

        self.project_id = project_id_arg
        self.topic_name = topic_name_arg

        self.client = pubsub_v1.SubscriberClient()

        self.counter = 0

        self.latencies = list()

        self.seconds = seconds_arg

        self.start()

    def run(self):
        self.receive_and_index()

    def receive_and_index(self):
        subscription_path = self.client.subscription_path(self.project_id,
            "{}-subscription-elastic".format(self.topic_name))

        def callback(message):
            latency = 1000 * (message._received_timestamp - message.publish_time.timestamp())

            message_id = message.message_id
            document = PubSubSubscriber.struct_message(message.data)

            self.elasticsearch_index_managers[self.counter % self.elastic_managers].queue.put((document, message_id))

            message.ack()

            if self.seconds:
                self.latencies.append(latency)

            self.counter += 1

        self.client.subscribe(subscription_path, callback=callback)

        # if seconds specified, run only for given time. if not - run indefinitely
        if self.seconds:
            sleep(self.seconds)

            time_queue_join_start = time()

            for manager in self.elasticsearch_index_managers:
                manager.queue.join()

            time_queue_join_stop = time()

            self.seconds = self.seconds + time_queue_join_stop - time_queue_join_start

            print("Read {} messages in {:.2f} seconds. That is {:.2f} mps!".format(self.counter, self.seconds,
                                                                                   self.counter / self.seconds))

            if self.latencies:
                avg_latency = float(sum(self.latencies))/float(len(self.latencies))

                print("Average latency was {:.2f} ms.".format(avg_latency))

        else:
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


class PubSubSubscribersManager:
    def __init__(self, project_id_arg, topic_name_arg, seconds_arg=None):
        self.threads = list()
        self.counter = None
        self.threads_seconds = None

        self.project = project_id_arg
        self.topic = topic_name_arg
        self.seconds = seconds_arg

    def run(self, threads_number):

        if self.seconds:
            print("Running for {} seconds...".format(self.seconds))
        else:
            print("Running forever")

        for i in range(threads_number):
            self.threads.append(PubSubSubscriber(self.project, self.topic, self.seconds))

        for thread in self.threads:
            thread.join()

        self.counter = sum([t.counter for t in self.threads])
        self.time_taken = max([t.seconds for t in self.threads])

        print("Summary: Read {} messages in {:.2f} seconds. That is {:.2f} mps!".format(self.counter, self.time_taken,
                                                                                        self.counter / self.time_taken))

        if self.seconds:
            try:
                avg_latency = float(sum([sum(t.latencies) for t in self.threads])) / float(sum([len(t.latencies) for t in self.threads]))
            except ZeroDivisionError:
                avg_latency = 0

        print("Summary: Average latency was {:.2f} ms.".format(avg_latency))


@click.command()
@click.option('--project-id', required=True, type=str, help='Google Cloud Platform Project Id')
@click.option('--topic', required=True, type=str, help='Pub/Sub Topic from which messages will be read')
@click.option('--seconds', default=None, required=False, type=int, help='For how long to read messages. If not provided - run forever')
@click.option('--subscribers', default=5, type=int, help="Number of concurrent threads to read from subscription")
def run(project_id, topic, seconds, subscribers):
    manager = PubSubSubscribersManager(project_id, topic, seconds)
    manager.run(subscribers)


if __name__ == '__main__':
    run()
