from faker import Factory
from faker.providers import date_time
from json import loads, JSONDecodeError
from random import randint

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
        chet = Factory.create()
        chet.add_provider(date_time)

        result = dict_arg

        for k, v in result.items():
            if isinstance(v, dict):
                [None for _, _ in DictFromTemplate.populate(v).items()]
            else:
                if 'time' in k:
                    value = 1000 * chet.date_time_this_month().timestamp()
                elif 'lat' in k:
                    value = 52.2297700
                elif 'lon' in k:
                    value = 21.0117800
                elif isinstance(v, float):
                    value = chet.pyfloat(left_digits=None, right_digits=2, positive=True)
                elif isinstance(v, int):
                    value = chet.pyint()
                elif isinstance(v, list):
                    value = list()
                    list_length = randint(1, 10)

                    first_element = v[0]

                    # assume that list consists of elements of the same type as first element
                    for _ in range(list_length):
                        if isinstance(first_element, int):
                            element = randint(0, 9999)
                        elif isinstance(first_element, str):
                            element = chet.word()
                        elif isinstance(first_element, dict):
                            element = {k: chet.word() for k, vi in first_element.items()}
                        elif isinstance(first_element, list):
                            element = chet.pylist()
                        else:
                            element = None

                        value.append(element)
                else:
                    value = chet.word()

                result[k] = value

        return result


