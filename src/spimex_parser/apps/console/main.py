import datetime
import urllib.error
from typing import List

from spimex_parser.apps.console import database
from spimex_parser.apps.console import deps
from spimex_parser.domain import models
from spimex_parser.modules import datetime_util


def main() -> None:
    database.create_tables()

    start_date = datetime.datetime(year=2023, month=1, day=1, hour=16, minute=20)
    end_date = datetime.datetime(year=2024, month=1, day=1, hour=16, minute=20)
    datetime_iterable = datetime_util.datetime_range(start_date, end_date)

    for current_date in datetime_iterable:
        try:
            add_results_from_specified_date_to_database(current_date)
        except urllib.error.HTTPError as e:
            print(f'Parsing error: "{str(e)}" (requested date: {current_date})')


def add_results_from_specified_date_to_database(date: datetime.datetime) -> None:
    """Получает данные о торгах указанной даты и добавляет в базу данных"""
    data_file_url = get_data_file_url(date)
    trading_results = load_results_from_file(data_file_url)
    add_results_to_repo(trading_results)
    print(f'Added to database contents of "{data_file_url}"')


def get_data_file_url(date: datetime.datetime) -> str:
    """Возвращает ссылку на файл данных указанной даты"""
    formatted_date = format_date(date)
    return f'https://spimex.com/upload/reports/oil_xls/oil_xls_{formatted_date}.xls'


def format_date(date: datetime.datetime) -> str:
    """Форматирует дату в строковое представление необходимого вида"""
    return date.strftime('%Y%m%d%H%M%S')


def load_results_from_file(data_file_path: str) -> List[models.TradingResult]:
    """Загружает данные результатов торгов из указанного файла"""
    with deps.get_parser_uow(data_file_path) as uow:
        return uow.data.list()


def add_results_to_repo(trading_results: List[models.TradingResult]) -> None:
    """Добавляет указанные данные о результатах торгов в базу данных"""
    with deps.get_data_uow() as uow:
        uow.data.add_bulk(trading_results)
        uow.commit()


if __name__ == '__main__':
    main()
