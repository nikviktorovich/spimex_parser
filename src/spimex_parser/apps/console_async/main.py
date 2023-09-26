import asyncio
import datetime
from collections.abc import Iterable
from typing import List

import aiohttp

from spimex_parser.apps.console_async import database
from spimex_parser.apps.console_async import deps
from spimex_parser.domain import models
from spimex_parser.modules import datetime_util


class AsyncTradingResultsManager:
    client: aiohttp.ClientSession


    def __init__(self, client: aiohttp.ClientSession) -> None:
        self.client = client


    async def load_results_from_date_to_repo(self, date: datetime.datetime) -> None:
        """Добавляет указанные данные о результатах торгов в базу данных"""
        file_url = self._get_data_file_url(date)
        trading_results = await self._load_results_from_file(file_url)

        async with deps.get_data_uow() as data_uow:
            data_uow.data.add_bulk(trading_results)
    

    def _get_data_file_url(self, date: datetime.datetime) -> str:
        """Возвращает ссылку на файл данных указанной даты"""
        formatted_date = self._format_date(date)
        return f'https://spimex.com/upload/reports/oil_xls/oil_xls_{formatted_date}.xls'
    

    def _format_date(self, date:datetime.datetime) -> str:
        """Форматирует дату в строковое представление необходимого вида"""
        return date.strftime('%Y%m%d%H%M%S')
    

    async def _load_results_from_file(self, url: str) -> List[models.TradingResult]:
        """Загружает данные результатов торгов из указанного файла"""
        async with deps.get_parser_uow(url, self.client) as parser_uow:
            return await parser_uow.data.list()


async def main() -> None:
    await database.create_tables()

    start_date = datetime.datetime(year=2023, month=1, day=1, hour=16, minute=20)
    end_date = datetime.datetime(year=2024, month=1, day=1, hour=16, minute=20)
    datetime_iterable = datetime_util.datetime_range(start_date, end_date)

    async with deps.get_async_client() as client:
        results_manager = AsyncTradingResultsManager(client)
        await run_loading_results(results_manager, datetime_iterable)



async def run_loading_results(
    results_manager: AsyncTradingResultsManager,
    datetime_iterable: Iterable[datetime.datetime],
) -> None:
    """Запускает задачи загрузки результатов с сайта в базу данных"""
    loading_results_tasks = []

    for date in datetime_iterable:
        task = loading_results_task(results_manager, date)
        loading_results_tasks.append(task)
    
    await asyncio.gather(*loading_results_tasks)


async def loading_results_task(
    results_manager: AsyncTradingResultsManager,
    date: datetime.datetime,
) -> None:
    """Задача загрузки результатов торгов с сайта Spimex в базу данных"""
    try:
        await results_manager.load_results_from_date_to_repo(date)
    except aiohttp.ClientError as e:
        print(f'Parsing error: "{str(e)}" (requested date: {date})')
    except asyncio.TimeoutError as e:
        print(f'Timeout error (requested date: {date})')
    else:
        print(f'Added trading results to database (requested date: {date})')


if __name__ == '__main__':
    asyncio.run(main())
