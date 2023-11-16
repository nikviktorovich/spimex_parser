from spimex_parser.modules.data_storage import unit_of_work

from tests.fakes.data_storage import repositories


class FakeTradingResultsUnitOfWork(unit_of_work.TradingResultsUnitOfWork):
    data: repositories.AsyncFakeTradingResultsRepository


    def __init__(self, repo: repositories.AsyncFakeTradingResultsRepository) -> None:
        self.data = repo
    

    async def __aenter__(self) -> unit_of_work.TradingResultsUnitOfWork:
        self.data.rollback()
        return self
    

    async def __aexit__(self, *args, **kwargs) -> None:
        self.data.rollback()
    

    async def commit(self) -> None:
        self.data.commit()
    

    async def rollback(self) -> None:
        self.data.rollback()
