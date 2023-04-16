FROM python:3-slim
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2 pymysql sqlalchemy geopy cryptography pandas
WORKDIR /app
COPY . .
CMD [ "python", "./analytics.py" ]