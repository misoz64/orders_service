from typing import Iterator
import json
import gzip
import re


class ContextManager:
    def __enter__(self) -> 'OrdersService':
        return self

    def __exit__(self):
        pass


def validate_date(date_time: str) -> None:
    """
    Datetime format validation. Even only substring is valid, expect full YYYY-MM-dd hh:mm:ss string
    :param date_time: validated string
    :return: None
    """
    # YYYY-MM-dd hh:mm:ss
    re_c = re.compile(r'^[\d]{4}-[\d]{2}-[\d]{2}[\s]+[\d]{1,2}:[\d]{1,2}:[\d]{1,2}$')
    if not re_c.match(date_time):
        raise ValueError(f'Invalid datetime format "{date_time}"')


class DataIterator:
    """
    Class to read data from file. Json per file is expected
    """
    def __init__(self, filename: str = 'data/data.ndjson', limit: int = None) -> Iterator['DataIterator']:
        """
        Read input file and iterate over them
        :param filename: input file name. Gzipped file with '.gz' extension is also valid
        :param limit: stop reading after ${limit} lines. Don't stop if None
        """
        self._f = gzip.open(filename, 'rb') if filename.endswith('.gz') else open(filename, 'r')
        self._counter = 0
        self._limit = limit

    def __iter__(self) -> Iterator['DataIterator']:
        return self

    def __next__(self) -> json:
        self._counter += 1
        line = self._f.readline() if self._limit is None or self._limit >= self._counter else None
        if not line:
            raise StopIteration
        try:
            return json.loads(line)
        except:
            print(f'Invalid json on line {self._counter}')