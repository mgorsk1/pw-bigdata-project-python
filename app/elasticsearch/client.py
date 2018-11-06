from elasticsearch import Elasticsearch, ElasticsearchException, NotFoundError, helpers
from datetime import datetime
from json import load
from threading import Thread
from queue import Queue
from os import getenv

from config import BASE_PATH


class ElasticDailyIndexManager(Thread):
    def __init__(self, index_basename):
        Thread.__init__(self)

        es_port = 9200 if ElasticDailyIndexManager.str_to_bool(getenv("RUNNING_IN_CONTAINER", "False")) else 9201
        es_url = "elastic" if ElasticDailyIndexManager.str_to_bool(getenv("RUNNING_IN_CONTAINER", "False")) else "localhost"

        self.es = Elasticsearch(hosts=[es_url], port=es_port)

        self.index_template_name = index_basename
        self.index_name_mask = index_basename if index_basename.endswith("-") else index_basename + "-"

        while not self._template_exists():
            self._register_index_template()

        self.queue = Queue()
        self.daemon = True

        self.start()

    def run(self):
        def generator():
            while True:
                message_body, message_id = self.queue.get()

                metadata = dict(metadata=dict(_id=message_id))
                self.queue.task_done()

                yield self._prepare_bulk_doc(message_body, **metadata)

        bulk_load = helpers.streaming_bulk(self.es, generator(), 10, yield_ok=False)

        while True:
            for success, info in bulk_load:
                print(success, info)

    def index_document(self, document_body, id=None):
        document_body['@timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

        try:
            self.es.index(index=self.index_name_mask + datetime.utcnow().strftime('%Y.%m.%d'),
                          doc_type='default',
                          body=document_body,
                          id=id)
        except ElasticsearchException as e:
            print(document_body, id, e.args)

    def _register_index_template(self):
        template_body = self._get_json_file_content("{}/config/templates/{}.json".format(BASE_PATH,
                                                                                         self.index_template_name))

        try:
            if template_body is not None:
                self.es.indices.put_template(name=self.index_template_name,
                                             body=template_body,
                                             master_timeout="60s")

        except ElasticsearchException as e:
            print(e.args)

    def _template_exists(self):
        try:
            self.es.indices.get_template(self.index_template_name)
            return True
        except NotFoundError:
            return False

    @staticmethod
    def _get_json_file_content(file_dir_arg):
        """
        Wrapper on load function. Expects file with JSON inside.

        :param file_dir_arg: Path to file to be read.
        :return: Dictionary (Encoded JSON)
        """
        result = None

        try:
            with open(file_dir_arg, 'r', encoding='UTF-8-SIG') as f:
                result_tmp = f
                result = load(result_tmp)
        except Exception as e:
            print(e.args)

        return result

    def _prepare_bulk_doc(self, source_arg, **kwargs):
        """
        Function providing unified document structure for indexing in elasticsearch.
        The structure needs to be compliant with

        :param index_arg: index to which send data
        :param doc_type_arg: document type in index_arg
        :param source_arg: body of document
        :param kwargs: additional meta parameters (like doc _id)
        :return: Reformatted & enhanced source_arg
        """

        metadata = dict(**kwargs).get('metadata', dict())

        source_arg['@timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

        result = {
            '_index': self.index_name_mask + datetime.utcnow().strftime('%Y.%m.%d'),
            '_type': 'default',
            '_op_type': 'update',
            'doc': source_arg,
            'doc_as_upsert': True
        }

        result.update(metadata)

        return result

    @staticmethod
    def str_to_bool(str_arg):
        if str_arg.lower() == 'true':
            return True
        elif str_arg.lower() == 'false':
            return False
        else:
            return None