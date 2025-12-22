#!/bin/bash
# Benchmark script to compare NONE vs STRICT strategies
# Run multiple times for each strategy

export SPARK_HOME=/home/hongbin/develop/spark-3.2.0-bin-hadoop3.2
RAPIDS_JAR=/home/hongbin/code/spark-rapids3/dist/target/rapids-4-spark_2.12-25.12.0-SNAPSHOT-cuda12.jar
cd /home/hongbin/code/spark-rapids3

# Create test script
cat > /tmp/benchmark_query.scala << 'EOF'
import org.apache.spark.sql.functions._

val parquetPath = "/tmp/rapids_priority_pool_test/test_data.parquet"

// Warm up
spark.read.parquet(parquetPath).count()

// Run 4 concurrent queries (same as original test)
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

val startTime = System.currentTimeMillis()

val queries = (1 to 4).map { i =>
  Future {
    val queryStart = System.currentTimeMillis()
    val queryDf = spark.read.parquet(parquetPath)
      .filter(col("int_col") === i * 100)
      .agg(sum("double_col").as("total"))
    val resultRow = queryDf.collect()
    System.currentTimeMillis() - queryStart
  }
}

val times = queries.map(f => Await.result(f, 60.seconds))
val totalTime = System.currentTimeMillis() - startTime

println(s"RESULT: total=${totalTime}ms, queries=${times.mkString(",")}")
EOF

echo "========================================"
echo "Benchmarking Priority Scheduling Strategies"
echo "========================================"
echo ""

# Run STRICT 5 times
echo "=== STRICT Strategy (5 runs) ==="
for i in 1 2 3 4 5; do
  echo -n "Run $i: "
  $SPARK_HOME/bin/spark-shell \
    --master "local[4]" \
    --driver-memory 4g \
    --jars "$RAPIDS_JAR" \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.explain=NONE \
    --conf spark.sql.files.maxPartitionBytes=128m \
    --conf spark.rapids.sql.multiThreadedRead.numThreads=4 \
    --conf spark.rapids.sql.multiThreadedRead.priorityScheduling.strategy=STRICT \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    -i /tmp/benchmark_query.scala 2>&1 | grep "RESULT:"
done

echo ""

# Run NONE 5 times  
echo "=== NONE Strategy (5 runs) ==="
for i in 1 2 3 4 5; do
  echo -n "Run $i: "
  $SPARK_HOME/bin/spark-shell \
    --master "local[4]" \
    --driver-memory 4g \
    --jars "$RAPIDS_JAR" \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.explain=NONE \
    --conf spark.sql.files.maxPartitionBytes=128m \
    --conf spark.rapids.sql.multiThreadedRead.numThreads=4 \
    --conf spark.rapids.sql.multiThreadedRead.priorityScheduling.strategy=NONE \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    -i /tmp/benchmark_query.scala 2>&1 | grep "RESULT:"
done

echo ""
echo "========================================"
echo "Benchmark completed!"
echo "========================================"













