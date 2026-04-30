function Invoke-Checked {
    param([scriptblock]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE"
    }
}

Invoke-Checked { docker compose build spark }
Invoke-Checked { docker compose up -d postgres clickhouse cassandra mongodb neo4j valkey }

$containers = @(
    "bd_spark_postgres",
    "bd_spark_clickhouse",
    "bd_spark_cassandra",
    "bd_spark_mongodb",
    "bd_spark_neo4j",
    "bd_spark_valkey"
)

$deadline = (Get-Date).AddMinutes(12)
do {
    $notReady = @()
    foreach ($container in $containers) {
        $health = docker inspect -f "{{.State.Health.Status}}" $container 2>$null
        if ($health -ne "healthy") {
            $notReady += "$container=$health"
        }
    }
    if ($notReady.Count -eq 0) {
        break
    }
    Write-Host ("Waiting for services: " + ($notReady -join ", "))
    Start-Sleep -Seconds 10
} while ((Get-Date) -lt $deadline)

if ($notReady.Count -ne 0) {
    docker compose ps
    throw "Some services did not become healthy in time"
}

Invoke-Checked { docker compose run --rm spark /opt/spark/bin/spark-submit /app/jobs/spark_etl.py }
Invoke-Checked { docker cp sql/validation/postgres.sql bd_spark_postgres:/tmp/postgres_validation.sql }
Invoke-Checked { docker cp sql/validation/clickhouse.sql bd_spark_clickhouse:/tmp/clickhouse_validation.sql }
Invoke-Checked { docker cp sql/validation/cassandra.cql bd_spark_cassandra:/tmp/cassandra_validation.cql }
Invoke-Checked { docker cp sql/validation/mongodb.js bd_spark_mongodb:/tmp/mongodb_validation.js }
Invoke-Checked { docker cp sql/validation/neo4j.cypher bd_spark_neo4j:/tmp/neo4j_validation.cypher }

Invoke-Checked { docker exec bd_spark_postgres psql -U lab -d spark_lab -f /tmp/postgres_validation.sql }
Invoke-Checked { docker exec bd_spark_clickhouse clickhouse-client --user lab --password lab --multiquery --queries-file /tmp/clickhouse_validation.sql }
Invoke-Checked { docker exec bd_spark_cassandra cqlsh -f /tmp/cassandra_validation.cql }
Invoke-Checked { docker exec bd_spark_mongodb mongosh --quiet /tmp/mongodb_validation.js }
Invoke-Checked { docker exec bd_spark_neo4j cypher-shell -f /tmp/neo4j_validation.cypher }
Invoke-Checked { docker exec bd_spark_valkey valkey-cli --scan --pattern 'reports:*:meta' }
