from spimex_parser.modules.parser import unit_of_work


def test_loading_from_url() -> None:
    urls = [
        'https://spimex.com/upload/reports/oil_xls/oil_xls_20230921162000.xls',
        'https://spimex.com/upload/reports/oil_xls/oil_xls_20230913162000.xls',
    ]

    for url in urls:
        with unit_of_work.PandasSpimexTradingResultsUnitOfWork(url) as uow:
            item = next(iter(uow.data))
            assert item is not None
