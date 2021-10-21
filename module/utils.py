from typing import Iterator
from datetime import datetime
import json
import gzip


class JsonDataException(Exception):
    pass


class ContextManager:
    def __enter__(self, *args, **kwargs) -> 'OrdersService':
        return self

    def __exit__(self, *args, **kwargs):
        pass


def get_datetime(date_time: str) -> 'datetime':
    """
    Datetime format validation. Even only substring is valid, expect full YYYY-MM-dd hh:mm:ss string
    :param date_time: validated string
    :return: datetime
    """
    # YYYY-MM-dd hh:mm:ss
    try:
        return datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
    except:
        raise ValueError('Invalid date time format (YYYY-MM-dd hh:mm:ss)')


class DataIterator:
    """
    Class to read data from file. Json per file is expected
    """
    def __init__(self, filename: str, limit: int = None) -> Iterator['DataIterator']:
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