// Test script for NONE (disabled) priority scheduling strategy
import org.apache.spark.sql.functions._

println("=" * 80)
println("Testing NONE (disabled) Priority Scheduling Strategy")
println("=" * 80)

val parquetPath = "/tmp/rapids_priority_pool_test/test_data.parquet"

println("\n[1] Reading parquet data with NONE (standard FIFO) scheduling...")
val readDf = spark.read.parquet(parquetPath)
val result = readDf
  .groupBy(col("int_col"))
  .agg(count("*").as("cnt"), avg("double_col").as("avg_double"))
  .orderBy("int_col")

println(s"   Result count: ${result.count()}")

println("\n[2] Running concurrent queries with NONE strategy...")
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

val queries = (1 to 4).map { i =>
  Future {
    val startTime = System.currentTimeMillis()
    val queryDf = spark.read.parquet(parquetPath)
      .filter(col("int_col") === i * 100)
      .agg(sum("double_col").as("total"))
    val resultRow = queryDf.collect()
    val elapsed = System.currentTimeMillis() - startTime
    println(s"   Query $i completed in ${elapsed}ms")
    elapsed
  }
}

val times = queries.map(f => Await.result(f, 60.seconds))
println(s"\n   All queries completed. Times: ${times.mkString(", ")}ms")

println("\n" + "=" * 80)
println("NONE strategy test completed successfully!")
println("=" * 80)

Thread.sleep(2000)













