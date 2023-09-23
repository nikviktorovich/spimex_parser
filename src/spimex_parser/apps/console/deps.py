import contextlib
from collections.abc import Iterator

from spimex_parser.apps.console import database
from spimex_parser.modules.data_storage import unit_of_work as data_unit_of_work
from spimex_parser.modules.parser import unit_of_work as parser_unit_of_work


@contextlib.contextmanager
def get_data_uow() -> Iterator[data_unit_of_work.TradingResultsUnitOfWork]:
    engine = database.engine
    with data_unit_of_work.SqlAlchemyTradingResultsUnitOfWork(engine) as uow:
        yield uow


@contextlib.contextmanager
def get_parser_uow(
    url: str,
) -> Iterator[parser_unit_of_work.SpimexTradingResultsUnitOfWork]:
    with parser_unit_of_work.PandasSpimexTradingResultsUnitOfWork(url) as uow:
        yield uow
