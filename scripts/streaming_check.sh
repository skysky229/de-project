#!/usr/bin/env bash
# Compare every streamed table across all layers:
#   Kafka messages  =  MinIO raw parquet rows  =  dwh.<fact>_streaming rows  =  view rows (source = 'streaming')
#
#   scripts/streaming_check.sh                          # everything still in Kafka
#   scripts/streaming_check.sh "2026-09-30 08:00:00"    # only rows recorded since then (UTC)
#
# Pause the generator and wait ~1 min (two micro-batches) first, otherwise the
# layers are caught mid-flight and differ by a few rows.
# Keep SINCE within the last 72 h: older Kafka messages and _streaming rows expire,
# MinIO keeps everything, so older windows can't match.
# Exit code: 0 = every row matches, 1 = a difference was found.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

SINCE="${1:-}"
if [[ -n "$SINCE" ]]; then
  SINCE_MS=$(python3 -c "import datetime,sys; print(int(datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d %H:%M:%S').replace(tzinfo=datetime.timezone.utc).timestamp() * 1000))" "$SINCE")
  CH_FILTER="src_sys_create_date >= toDateTime('$SINCE', 'UTC')"
else
  SINCE_MS=-2          # Kafka's "earliest offset still retained"
  CH_FILTER="1"
fi

ch() { curl -sS --fail-with-body --user "$DWH_CH_USER:$DWH_CH_PASSWORD" --data-binary "$1" "$DWH_CH_URL"; }

kafka_offsets() {      # "<partition> <offset>" per partition; $2 = --time value (optional)
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
    --topic "$1" ${2:+--time "$2"} 2>/dev/null | awk -F: 'NF == 3 {print $2, $3}'
}

kafka_count() {        # messages in topic $1 from SINCE until now
  # a partition with no message after SINCE has no line in the --time output: it contributes 0
  awk 'NR == FNR {at[$1] = $2; next} {n += $2 - (($1 in at) ? at[$1] : $2)} END {print n + 0}' \
    <(kafka_offsets "$1" "$SINCE_MS") <(kafka_offsets "$1")
}

echo "Counting raw parquet in MinIO with Spark (takes ~30-60 s)..."
LAKE=$(docker compose run --rm --no-deps -T spark-streaming bash -c \
  "spark-submit --master spark://spark-master:7077 --conf spark.driver.host=\$(hostname -i) \
     --conf spark.jars.ivy=/tmp/.ivy2 \
     --conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT --conf spark.hadoop.fs.s3a.path.style.access=true \
     --conf spark.hadoop.fs.s3a.access.key=$MINIO_ROOT_USER --conf spark.hadoop.fs.s3a.secret.key=$MINIO_ROOT_PASSWORD \
     /opt/spark_jobs/streaming/tools/count_raw_lake.py ${SINCE:+--since '$SINCE'}" 2>/dev/null | grep '^LAKE' || true)
[[ -n "$LAKE" ]] || { echo "Could not count MinIO (is the Spark cluster up? http://localhost:8081)"; exit 1; }

# raw table : fact table fed by it (loads and trips both feed fact_trip)
STREAMS="trips:fact_trip loads:fact_trip delivery_events:fact_delivery_event fuel_purchases:fact_fuel_purchase maintenance_records:fact_maintenance safety_incidents:fact_safety_incident"

echo
echo "Window: ${SINCE:-everything still in Kafka} (UTC)"
printf "%-20s %8s %8s %8s %11s %10s  %s\n" stream kafka minio minio_ids _streaming view status
status=0
for pair in $STREAMS; do
  table=${pair%%:*}; fact=${pair##*:}
  k=$(kafka_count "raw.$table")
  read -r m mids <<< "$(awk -v t="$table" '$2 == t {print $3, $4}' <<< "$LAKE")"
  s=$(ch "SELECT count() FROM ${DWH_TARGET_DB}.${fact}_streaming FINAL WHERE $CH_FILTER")
  v=$(ch "SELECT count() FROM ${DWH_TARGET_DB}.v_${fact} WHERE source = 'streaming' AND $CH_FILTER")
  if [[ "$k" == "$m" && "$m" == "$mids" && "$m" == "$s" && "$s" == "$v" ]]; then ok=OK; else ok=DIFF; status=1; fi
  printf "%-20s %8s %8s %8s %11s %10s  %s\n" "$table" "$k" "$m" "$mids" "$s" "$v" "$ok"
done
echo
[[ $status == 0 ]] && echo "All layers match." || echo "Differences found (generator still running? batch took over some days? window older than 72 h?)."
exit $status
