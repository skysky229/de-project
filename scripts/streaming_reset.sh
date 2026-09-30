#!/usr/bin/env bash
# Reset the streaming pipeline to an empty state, for a clean test run.
#
# Deletes (streaming test data only):
#   - Kafka topics raw.* (the generator re-creates them on start)
#   - MinIO: raw/logistics/ (streamed raw parquet) and processed/checkpoints/streaming/
#   - ClickHouse: all rows of the 5 dwh.<fact>_streaming tables (the tables stay)
# Keeps: batch tables dwh.<fact>, the views, the raw ClickHouse tables in `default`.
#
#   scripts/streaming_reset.sh          # asks for confirmation
#   scripts/streaming_reset.sh --yes    # no prompt
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

if [[ "${1:-}" != "--yes" ]]; then
  read -r -p "Delete ALL streaming test data (Kafka raw.* topics, MinIO raw/logistics + checkpoints, dwh.*_streaming rows)? [y/N] " answer
  [[ "$answer" == "y" || "$answer" == "Y" ]] || { echo "Aborted."; exit 1; }
fi

ch() { curl -sS --fail-with-body --user "$DWH_CH_USER:$DWH_CH_PASSWORD" --data-binary "$1" "$DWH_CH_URL"; }
kafka() { docker compose exec -T kafka /opt/kafka/bin/"$@"; }

echo "1/4 stopping generator and streaming app"
docker compose stop streamgen spark-streaming

echo "2/4 deleting Kafka topics raw.*"
if [[ -n "$(kafka kafka-topics.sh --bootstrap-server localhost:9092 --list | grep '^raw\.' || true)" ]]; then
  kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic 'raw\..*'
fi
for _ in $(seq 1 30); do       # deletion is asynchronous; wait until the topics are really gone
  [[ -z "$(kafka kafka-topics.sh --bootstrap-server localhost:9092 --list | grep '^raw\.' || true)" ]] && break
  sleep 2
done

echo "3/4 deleting MinIO raw/logistics/ and checkpoints"
docker compose exec -T minio sh -c "
  mc alias set local http://localhost:9000 '$MINIO_ROOT_USER' '$MINIO_ROOT_PASSWORD' >/dev/null &&
  mc rm -r --force local/$MINIO_BUCKET_RAW/logistics/ >/dev/null 2>&1;
  mc rm -r --force local/$MINIO_BUCKET_PROCESSED/checkpoints/streaming/ >/dev/null 2>&1; true"

echo "4/4 emptying dwh.*_streaming tables"
for fact in fact_trip fact_delivery_event fact_fuel_purchase fact_maintenance fact_safety_incident; do
  ch "TRUNCATE TABLE IF EXISTS ${DWH_TARGET_DB}.${fact}_streaming"
done

echo "Done. Start again with: docker compose up -d spark-streaming streamgen"
