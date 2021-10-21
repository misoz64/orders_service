#!/usr/bin/env python3
from module.data_service import OrdersService


def example_main():
    with OrdersService('data/orders.sqlite') as service:
        service.store('data/data.ndjson.gz')
#        service.store('data/data.ndjson.gz', limit=10)
        service.get_orders('2018-10-13 10:06:36', '2018-11-13 07:28:58')
        service.top_buyers(limit=4)


if __name__ == '__main__':
    example_main()
