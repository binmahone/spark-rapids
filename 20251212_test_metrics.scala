// Test script to compare NONE vs STRICT metrics
import org.apache.spark.sql.functions._

println("=" * 80)
println("Testing Priority Pool Metrics")
println("=" * 80)

val parquetPath = "/tmp/rapids_priority_pool_test/test_data.parquet"

// Run multiple queries to generate enough data points
println("\n[1] Running queries...")
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

val startTime = System.currentTimeMillis()

val queries = (1 to 8).map { i =>
  Future {
    val queryStart = System.currentTimeMillis()
    val queryDf = spark.read.parquet(parquetPath)
      .filter(col("int_col") === i * 100)
      .agg(sum("double_col").as("total"), count("*").as("cnt"))
    val resultRow = queryDf.collect()
    val elapsed = System.currentTimeMillis() - queryStart
    println(s"   Query $i completed in ${elapsed}ms")
    elapsed
  }
}

val times = queries.map(f => Await.result(f, 120.seconds))
val totalTime = System.currentTimeMillis() - startTime

println(s"\n[2] All queries completed in ${totalTime}ms")
println(s"   Individual times: ${times.mkString(", ")}ms")
println(s"   Average: ${times.sum / times.length}ms")

// Metrics will be logged during shutdown
println("\n[3] Check logs for PriorityAwareFileReaderThreadPool Metrics")

println("\n" + "=" * 80)
println("Metrics test completed!")
println("=" * 80)

Thread.sleep(2000)

