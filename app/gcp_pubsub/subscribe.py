import click

from os import environ
from json import loads, JSONDecodeError
from time import sleep, strftime, localtime, time
from threading import Thread
from google.cloud import pubsub_v1
from message_proto import MSG

from config import BASE_PATH
from app.elasticsearch.client import ElasticDailyIndexManager

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
            document = PubSubSubscriber.protobuf_to_json(message.data)

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
    def protobuf_to_json(self, message_arg):
        try:
            pb_message = MSG()
            pb_message.ParseFromString(message_arg)
        except Exception as e:
            print(e.arsg)

        # if received message matches proto schema - process
        if pb_message.IsInitialized():
            try:
                message = pb_message.SerializeToDict()
            except Exception as e:
                print(e.args)

            return message
        else:
            return None

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
