import pytest
import sqlalchemy.engine

from spimex_parser.modules.data_storage import unit_of_work as data_unit_of_work
from spimex_parser.modules.parser import unit_of_work as parser_unit_of_work


@pytest.mark.usefixtures('engine')
def test__parse_and_add_trading_results(engine: sqlalchemy.engine.Engine) -> None:
    url = 'https://spimex.com/upload/reports/oil_xls/oil_xls_20230921162000.xls'
    with parser_unit_of_work.PandasSpimexTradingResultsUnitOfWork(url) as uow:
        trading_results = uow.data.list()

    with data_unit_of_work.SqlAlchemyTradingResultsUnitOfWork(engine) as uow:
        assert not uow.data.list()

        uow.data.add_bulk(trading_results=trading_results)
        uow.commit()

        assert uow.data.list()
