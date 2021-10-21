from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from module.utils import DataIterator
from typing import List, NewType, Dict
from module.model import Base, User, Product, Order

DictUser = NewType('DictUser', Dict)
DBUser = NewType('DBUser', User)
DictProduct = NewType('DictProduct', Dict)
DictProductsVector = NewType('DictProductsVector', List[DictProduct])
DBProductsVector = NewType('DBProductsVector', List[Product])


def get_session(database_file: str):
    """
    Initialize sqlalchemy with sqlite3 engine
    :param database_file: sqlite3 input/output file name
    :return: database session
    """
    engine = create_engine(f"sqlite:///{database_file}")
    Base.metadata.create_all(engine)
    Session = sessionmaker()
    Session.configure(bind=engine)
    return Session()


class DataStorage:
    def __init__(self, session, *args, **kwargs) -> None:
        self._session = session
        self._cache = {'users': {}, 'products': {}}

    def store(self, filename: str, limit: int=None) -> None:
        """
        Store input json record into output databaze
        :param filename: input data filename
        :param limit: only ${limit} lines will be processed. Unlimited if None
        :return: None
        """
        for record in DataIterator(filename, limit):
            if record['user']['id'] not in self._cache['users'].keys():
                self._cache['users'][record['user']['id']] = self._create_user(DictUser(record['user']))
            products = self._create_products(record['products'])
            self._create_order(record, products)
        self._session.commit()

    def _create_user(self, new_user: DictUser) -> DBUser:
        """
        Create user if not exists
        :param new_user: dict user
        :return: DB User object
        """
        db_user = (
            self._session.query(User).filter(User.id == new_user["id"]).one_or_none()
        )
        if db_user is None:
            db_user = User(**new_user)
            self._session.add(db_user)
        return db_user

    def _create_products(self, new_products: DictProductsVector) -> DBProductsVector:
        """
        Retrieve or create (if not exists) products
        :param new_products: list of dicts of products
        :return: list of DB Product instancies
        """
        db_products = DBProductsVector([])
        for new_product in new_products:
            if new_product["id"] not in self._cache['products'].keys():
                db_product = (
                    self._session.query(Product).filter(Product.id == new_product["id"]).one_or_none()
                )
                if db_product is None:
                    db_product = Product(**new_product)
                    self._session.add(db_product)
                    self._cache['products'][new_product["id"]] = db_product
            else:
                db_product = self._cache['products'][new_product["id"]]
            db_products.append(db_product)
        return db_products

    def _create_order(self, record: dict, products: DBProductsVector) -> None:
        """
        Create order with related user relation and products
        :param record:
        :param products:
        :return:
        """
        db_order = (
            self._session.query(Order).filter(Order.id == record["id"]).one_or_none()
        )
        if db_order:
            # already exists, skipping
            self._session.rollback()
            return
        db_order = Order(id=record['id'], created=datetime.fromtimestamp(record['created']))
        db_order.user_id = record['user']['id']
        for product in products:
            db_order.products.append(product)
        self._session.add(db_order)
