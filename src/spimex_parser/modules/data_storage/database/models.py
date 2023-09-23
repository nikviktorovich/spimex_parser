import sqlalchemy.orm
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UUID
from sqlalchemy.orm import mapped_column


Base = sqlalchemy.orm.declarative_base()


class TradingResult(Base):
    __tablename__ = 'spimex_trading_results'

    id = mapped_column(UUID(as_uuid=True), primary_key=True)
    exchange_product_id = mapped_column(String(11))
    exchange_product_name = mapped_column(String)
    oil_id = mapped_column(String(4))
    delivery_basis_id = mapped_column(String(3))
    delivery_basis_name = mapped_column(String)
    delivery_type_id = mapped_column(String(1))
    volume = mapped_column(Integer)
    total = mapped_column(Integer)
    count = mapped_column(Integer)
    date = mapped_column(Date)
    created_on = mapped_column(DateTime)
    updated_on = mapped_column(DateTime)
