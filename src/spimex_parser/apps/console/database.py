import sqlalchemy
import sqlalchemy.engine

from spimex_parser import config
from spimex_parser.modules.data_storage.database import models as db_models


engine = sqlalchemy.create_engine(config.DB_CONNECTION_URL)


def create_tables() -> None:
    db_models.Base.metadata.create_all(bind=engine)
