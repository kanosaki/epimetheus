#!/usr/bin/env bash

TARGET=http://localhost:9090
RESULT_DIR=results/$(date +%Y-%m-%dT%T%z)

command -v vegeta >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
  echo Please install vegeta command
  exit -1
fi

run_benchmark() {
  flavor=$1
  mkdir -p $RESULT_DIR/$flavor
  echo "$flavor Loading data..."
  curl -X POST -F "data=@data/preload.generated/$flavor.tsv.parquet" $TARGET/api/v1/scrape_data 
  for f in data/queries.generated/$flavor/*; do
    echo "$flavor $f"
    vegeta attack  -targets $f -workers 10 -rate '10/1s' -duration 30s > $RESULT_DIR/$flavor/$(basename $f)
  done
  echo "$flavor Cleaning up..."
  curl -X DELETE $TARGET/api/v1/scrape_data
}

run_benchmark longterm


