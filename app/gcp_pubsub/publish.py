from google.cloud import pubsub_v1
from json import dumps
from os import environ

from config import BASE_PATH

environ['GOOGLE_APPLICATION_CREDENTIALS'] = "{}/config/keys/gcp/key.json".format(BASE_PATH)


def publish_message(project_id, topic_name, message_body):
    """Publishes message to a Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()

    # `projects/{project_id}/topics/{topic_name}`
    topic_path = publisher.topic_path(project_id, topic_name)

    data = dumps(message_body).encode('utf-8')

    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data=data, origin='meetup-websocket', username='mariusz-gorski')
    print('Published {} of message ID {}.'.format(data, future.result()))
