docker cp sql/validation/postgres.sql bd_spark_postgres:/tmp/postgres_validation.sql
docker cp sql/validation/clickhouse.sql bd_spark_clickhouse:/tmp/clickhouse_validation.sql
docker cp sql/validation/cassandra.cql bd_spark_cassandra:/tmp/cassandra_validation.cql
docker cp sql/validation/mongodb.js bd_spark_mongodb:/tmp/mongodb_validation.js
docker cp sql/validation/neo4j.cypher bd_spark_neo4j:/tmp/neo4j_validation.cypher

docker exec bd_spark_postgres psql -U lab -d spark_lab -f /tmp/postgres_validation.sql
docker exec bd_spark_clickhouse clickhouse-client --user lab --password lab --multiquery --queries-file /tmp/clickhouse_validation.sql
docker exec bd_spark_cassandra cqlsh -f /tmp/cassandra_validation.cql
docker exec bd_spark_mongodb mongosh --quiet /tmp/mongodb_validation.js
docker exec bd_spark_neo4j cypher-shell -f /tmp/neo4j_validation.cypher
docker exec bd_spark_valkey valkey-cli --scan --pattern 'reports:*:meta'
