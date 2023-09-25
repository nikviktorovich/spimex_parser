import datetime
import urllib.error
from typing import Generator

from spimex_parser.apps.console import database
from spimex_parser.apps.console import deps
from spimex_parser.modules.data_storage import repositories


def iterate_dates(
    start: datetime.datetime,
) -> Generator[datetime.datetime, None, None]:
    current_date = start

    while True:
        yield current_date
        current_date += datetime.timedelta(days=1)


def format_date(date: datetime.datetime) -> str:
    return date.strftime('%Y%m%d%H%M%S')


def get_data_file_url(formatted_date: str) -> str:
    return f'https://spimex.com/upload/reports/oil_xls/oil_xls_{formatted_date}.xls'


def parse_file_and_add_to_repo(
    trading_results_repo: repositories.TradingResultsRepository,
    data_file_path: str,
) -> None:
    with deps.get_parser_uow(data_file_path) as uow:
        obtained_trading_results = uow.data.list()
        trading_results_repo.add_bulk(obtained_trading_results)


def parse_file_and_add_to_database(data_file_path: str) -> None:
    with deps.get_data_uow() as uow:
        parse_file_and_add_to_repo(uow.data, data_file_path)


def parse_files_and_add_to_database(date: datetime.datetime) -> None:
    formatted_date = format_date(date)
    data_file_url = get_data_file_url(formatted_date)
    parse_file_and_add_to_database(data_file_url)
    print(f'Added to database contents of "{data_file_url}"')


def main() -> None:
    database.create_tables()

    start_date = datetime.datetime(year=2023, month=1, day=1, hour=16, minute=20)

    for current_date in iterate_dates(start_date):
        try:
            parse_files_and_add_to_database(current_date)
        except urllib.error.HTTPError as e:
            print(f'Parsing error: "{str(e)}" (requested date: {current_date})')
    


if __name__ == '__main__':
    main()
