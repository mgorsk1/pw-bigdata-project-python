from elasticsearch import Elasticsearch, ElasticsearchException, NotFoundError
from datetime import datetime
from json import load

from config import BASE_PATH


class ElasticDailyIndexManager:
    def __init__(self, index_basename):
        self.es = Elasticsearch(port=9201)

        self.index_template_name = index_basename
        self.index_name_mask = index_basename if index_basename.endswith("-") else index_basename + "-"

        while not self._template_exists():
            self._register_index_template()

    def index_document(self, document_body, id=None):
        document_body['@timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        self.es.search()
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