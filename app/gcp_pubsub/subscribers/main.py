import click

from app.tools.threads_manager import ThreadsManager
from app.gcp_pubsub.subscribers.base import BaseSubscriber


@click.command()
@click.option('--project-id', required=True, type=str, help='Google Cloud Platform Project Id')
@click.option('--topic', required=True, type=str, help='Pub/Sub Topic from which messages will be read')
@click.option('--seconds', default=None, required=False, type=int, help='For how long to read messages. If not provided - run forever')
@click.option('--subscribers', default=1, type=int, help="Number of concurrent threads to read from subscription")
def run(project_id, topic, seconds, subscribers):
    manager = ThreadsManager(BaseSubscriber, project_id, topic, seconds)
    manager.run(subscribers)


if __name__ == '__main__':
    run()