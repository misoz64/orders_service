from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Table
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

order_products = Table(
    "tab_order_product",
    Base.metadata,
    Column("order_id", Integer, ForeignKey("tab_order.id")),
    Column("product_id", Integer, ForeignKey("tab_product.id")),
)


class User(Base):
    __tablename__ = "tab_user"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    city = Column(String)


class Product(Base):
    __tablename__ = "tab_product"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    price = Column(Integer)
    orders = relationship(
        "Order", secondary=order_products, back_populates="products"
    )


class Order(Base):
    __tablename__ = "tab_order"
    id = Column(Integer, primary_key=True)
    created = Column(DateTime)
    user_id = Column("user_id", Integer, ForeignKey("tab_user.id"))
    products = relationship(
        "Product", secondary=order_products, back_populates="orders"
    )


