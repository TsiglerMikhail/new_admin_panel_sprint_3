#!/usr/bin/env bash

set -e

echo "Check ELK index..."
check_index=$(curl -s -o /dev/null -w "%{http_code}" "${ELK_DSN}/movies")
if [ "$check_index" -ne 200 ]; then
  curl -XPUT "${ELK_DSN}/movies" -H "Content-Type: application/json" -d "@es_schema.json"
  echo "index created"
fi

echo "Start app..."
python main.py
