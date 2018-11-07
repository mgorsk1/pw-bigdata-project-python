from faker import Faker
from json import loads, JSONDecodeError
from datetime import datetime

from config import BASE_PATH


class DictFromTemplate:
    def __init__(self, template_name):

        with open('{}/resources/mockups/templates/{}.json'.format(BASE_PATH, template_name), 'r+') as f:
            template_raw = f.read()

        try:
            self.template = loads(template_raw)
        except JSONDecodeError as e:
            raise JSONDecodeError

    def generate(self):
        return DictFromTemplate.populate(self.template)

    @staticmethod
    def populate(dict_arg):
        chet = Faker()

        result = dict_arg

        for k, v in result.items():
            if isinstance(v, dict):
                [None for _, _ in DictFromTemplate.populate(v).items()]
            else:
                if 'time' in k:
                    value = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                else:
                    value = chet.word()

                result[k] = value

        return result


