FROM spark:3.5.8-scala2.12-java17-python3-ubuntu

USER root

RUN pip install --no-cache-dir cassandra-driver clickhouse-driver neo4j psycopg2-binary pymongo redis

USER spark
