#! /bin/bash

# Alpha Vantage API key
API_KEY="7ZYTGG8N0LLVQ9FJ"
API_INT="1min"
PG_HOST="host.docker.internal"
PG_USER="postgres"
PG_PASS="postgres"
PG_PORT="5430"
PG_DB="stocks"
TICKERS="AAPL GOOGL MSFT IBM NVDA ORCL INTC QCOM AMD TSM"

echo "Creating folders"
mkdir data
chmod a+w data

echo "Create and run docker containers"
docker-compose -f docker/docker-compose.yaml up --detach

echo "Waiting for containers up"
cmd="docker exec docker-airflow-worker-1 airflow variables list"
while $cmd ; ((ret=$?)) ;do
  echo "sleeping 1 sec"
  sleep 1
done

echo "Create Airflow variables"
docker exec docker-airflow-worker-1 airflow variables set apikey "$API_KEY"
docker exec docker-airflow-worker-1 airflow variables set apiint "$API_INT"
docker exec docker-airflow-worker-1 airflow variables set tickers "$TICKERS"
docker exec docker-airflow-worker-1 airflow connections add "pg_conn" --conn-uri "postgres://$PG_USER:$PG_PASS@$PG_HOST:$PG_PORT/$PG_DB"
docker exec docker-airflow-worker-1 airflow variables set conn_id "pg_conn"

echo "Copying DAGs"
cp scripts/* docker/dags
touch docker/dags/*