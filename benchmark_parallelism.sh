!/usr/bin/env bash

JAR="/Users/dbanelas/Developer/flink-setrastream/flink-setrastream/target/flink-setrastream-1.0-SNAPSHOT.jar"
PROM_URL="http://localhost:9090"              
PARALLELISMS=(1 2 4 8)
CSV="results_parallelism.csv"

echo "parallelism,batchesPerSec" > "$CSV"

for P in "${PARALLELISMS[@]}"; do
  echo "▶ parallelism $P"

  # 1) start job, detached (-d) so the script continues
  OUT=$(flink run -d "$JAR" \
        --features dpx dpy \
        --id robotID \
        --parallelism "$P" \
        --timestamp current_time \
        --input-topic robot_data)

  # 2) extract JobID from flink’s stdout
  JOB_ID=$(grep -oE '[0-9a-fA-F-]{8,}' <<< "$OUT" | head -n1)
  echo "   JobID $JOB_ID"

  # 3) give the job a little time to warm up & emit metrics
  sleep 60

  # 4) instant-query Prometheus (aggregated across subtasks)
  VALUE=$(curl -s -g "${PROM_URL}/api/v1/query" \
          --data-urlencode \
          'query=sum(flink_taskmanager_job_task_operator_throughput_batchesProcessedPerSecond)' |
          jq -r '.data.result[0].value[1]')

  echo "   → ${VALUE} batches/s"
  echo "${P},${VALUE}" >> "$CSV"

  # 5) stop the job
  flink cancel "$JOB_ID" >/dev/null
done

echo -e "\nDone. Results in $CSV"
