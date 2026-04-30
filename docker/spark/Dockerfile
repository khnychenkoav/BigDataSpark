FROM spark:3.5.8-scala2.12-java17-python3-ubuntu

USER root

RUN pip install --no-cache-dir cassandra-driver clickhouse-driver neo4j psycopg2-binary pymongo redis
RUN curl -fSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar -o /opt/spark/jars/postgresql-42.7.4.jar

USER spark
