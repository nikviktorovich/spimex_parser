FROM python:3.10

WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./src /code/src
RUN pip install --no-cache-dir -e /code/src

COPY ./migrations /code/migrations
COPY ./alembic.ini /code/alembic.ini

WORKDIR /code
CMD ["bash", "-c", "alembic upgrade head && uvicorn spimex_parser.apps.fastapi_app.main:app --host 0.0.0.0 --port 8000"]
