docker compose build spark
docker compose up -d postgres clickhouse cassandra mongodb neo4j valkey

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

docker compose run --rm spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.4 /app/jobs/spark_etl.py
