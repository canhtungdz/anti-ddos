FROM bitnami/spark:3.5.6

USER root
WORKDIR /opt

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY empty.py ./
RUN spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /opt/empty.py