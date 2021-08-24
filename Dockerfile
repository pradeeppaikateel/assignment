FROM bitnami/spark

USER root

COPY requirements.txt .

RUN pip install -r requirements.txt

RUN curl https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -o /opt/bitnami/spark/jars/postgresql-42.2.18.jar

COPY . /opt/bitnami/spark/postman-assignment/