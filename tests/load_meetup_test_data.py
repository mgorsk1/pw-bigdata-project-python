import datetime

from elasticsearch import Elasticsearch, ElasticsearchException
from json import load
from random import randint

from app.tools.mockups import DictFromTemplate
from config import BASE_PATH

es = Elasticsearch(hosts=["localhost:9203"])

template = DictFromTemplate('meetup-rawdata')


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


def _register_index_template(es, index_template_name):
    template_body = _get_json_file_content("{}/config/index_templates/{}.json".format(BASE_PATH,
                                                                                      index_template_name))

    try:
        if template_body is not None:
            es.indices.put_template(name=index_template_name,
                                    body=template_body,
                                    master_timeout="60s")

    except ElasticsearchException as e:
        print(e.args)


_register_index_template(es, 'meetup-rawdata')

date_tmp = datetime.date(2018, 12, 1)

for i in range(62):
    date_suffix = date_tmp.strftime('%Y.%m.%d')

    for i in range(randint(10, 25)):
        doc = template.generate()

        es.index('meetup-rawdata-{}'.format(date_suffix), 'default', doc)

    date_tmp = date_tmp + datetime.timedelta(days=1)