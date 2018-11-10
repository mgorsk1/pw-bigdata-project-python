import click

from os import environ
from time import sleep, strftime, localtime, time
from threading import Thread
from google.cloud import pubsub_v1
from message_proto import MSG

from config import BASE_PATH
from app.logger import log
from app.elasticsearch.client import ElasticDailyIndexManager

environ['GOOGLE_APPLICATION_CREDENTIALS'] = "{}/config/keys/gcp/key.json".format(BASE_PATH)


class BaseSubscriber(Thread):
    """Base class responsible for reading raw data from pub/sub subscription and indexing it into Elasticsearch.

    It should be used with ThreadsManager, where number of concurrent threads is selected.

    It requires index template to be present in config/index_templates. Index name must be named the same way as topic
    from which data is subscribed.

    Schema validation is performed using pyrobuf (implementation of proto buffers).
    Before running class make sure that proto file corresponding to shema of subscription data was compiled with
    pyrobuf and installed in Your venv (here we use message_proto.MSG() generalized library name).

    Can be used directly, or, if we want to perform some data enrichment, subclassed with overriding enrich function,
    where custom data enrichment logic can be implemented.
    """

    def __init__(self, project_id_arg, topic_name_arg, seconds_arg=None, **kwargs):
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
            document = self.enrich(BaseSubscriber.protobuf_to_json(message.data))

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

            log.log_info("{} - Read {} messages in {:.2f} seconds. That is {:.2f} mps!".format(
                                   self.__class__.__name__, self.counter, self.seconds, self.counter / self.seconds))

            if self.latencies:
                avg_latency = float(sum(self.latencies))/float(len(self.latencies))

                log.log_info("{} - Average latency was {:.2f} ms.".format(self.__class__.__name__, avg_latency))

        else:
            while True:
                sleep(60)

    def enrich(self, dict_arg):
        return dict_arg

    @staticmethod
    def protobuf_to_json(message_arg):
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



