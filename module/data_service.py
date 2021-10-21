from sqlalchemy.sql import func, desc
from module.model import User, Product, Order
from module.utils import validate_date, ContextManager
from module.database import get_session, DataStorage
from typing import List, Any
SQLResult = List[Any]


class OrdersService(ContextManager):
    def __init__(self, database_file: str = 'data/orders.sqlite', *args, **kwargs):
        self._session = get_session(database_file)
        self._data_storage = DataStorage(self._session)

    def store(self, filename: str, limit: int=None):
        self._data_storage.store(filename, limit)

    def _fetch_orders(self, data_from: str, data_to: str) -> SQLResult:
        """
        Execute DB SELECT to retieve orders created between datetimes
        :param data_from: datetime min interval
        :param data_to: datetime max interval
        :return: sql result
        """
        return (
            self._session.query(Order.id, Order.created, Order.user_id, User.name.label('user_name'),
                                User.city.label('user_city')).join(User).filter(Order.created >= data_from).
                filter(Order.created <= data_to).order_by(Order.created).all()
        )

    def get_orders(self, data_from: str, data_to: str) -> None:
        """
        Print result - orders created between given datetimes
        :param data_from: datetime min interval
        :param data_to: datetime max interval
        """
        validate_date(data_from)
        validate_date(data_to)

        print(f'Orders by date:')
        print(f'\tOrder ID\tCreated\t\tUser')
        for no, order in enumerate(self._fetch_orders(data_from, data_to)):
            print(f'{no + 1}\t{order.id}\t{str(order.created)}\t{order.user_id, order.user_name, order.user_city}')
        print()

    def _fetch_best_buyers(self, limit: int) -> SQLResult:
        """
        Retrieve sorted list of ${limit} top buyers
        :param limit: results restricion
        :return: sql result
        """
        return (
            self._session.query(User.id, User.name, User.city, func.count(Product.id).label('total_products')).
                join(Order.products).join(User).group_by(Order.id).order_by(desc('total_products')).limit(limit).all()
        )

    def top_buyers(self, limit: int) -> None:
        """
        Print result - top ${limit} users by sum of their ordered products
        :param limit: results restricion
        :return: None
        """
        if limit <= 0:
            raise ValueError(f'Invalid limit value: "{limit}"')
        print(f'Top {limit} best buyers:')
        print(f'\tProducts\tUser')
        for no, buyer in enumerate(self._fetch_best_buyers(limit)):
            print(f'{no}\t{buyer.total_products}\t{buyer.id, buyer.name, buyer.city}')
        print()

