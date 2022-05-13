package org.example

import org.apache.spark.sql.functions.{col, from_csv}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.example.data.generation.KafkaDataGenerator

object SparkTopN {

  var finished = false
  var startTs = 0L
  var latencies: Seq[Long] = Seq()

  def main(args: Array[String]): Unit = {

    val config = new org.apache.spark.SparkConf().setAppName("test streaming")
    config.setMaster("local[2]")

    val sparkSession = SparkSession
      .builder()
      .config(config)
      .getOrCreate()

    sparkSession.conf.set("spark.sql.streaming.schemaInference", value = true)
    sparkSession.sparkContext.setLogLevel("WARN")

    val schema = StructType(List(StructField("name", StringType), StructField("lines", LongType), StructField("ts", LongType)))

    val kafkaLines = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "input-topic")
      .option("kafka.group.id", "test")
      .load()
      .select(from_csv(col("value").cast("string"), schema, Map[String, String]()).as("data"))
      .select("data.*")

    kafkaLines.createTempView("tmpView")
//
    val query = sparkSession
      .sql("" +
//        "select name , sum, row_number() over(order by sum desc) rn " +
//        "from (" +
        "   select name, sum(lines) as sum, count(*) as cnt, max(ts) as ts " +
        "   from tmpView " +
        "   group by name " +
        "   order by sum desc "
//        ")"
        )
      .writeStream
//      .format("console")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val cnt = batchDF.agg("cnt" -> "sum").first().get(0).asInstanceOf[Long]
        println("cnt = " + cnt)
        val ts = batchDF.agg("ts" -> "max").first().get(0).asInstanceOf[Long]
        val latency = (System.currentTimeMillis() - ts)
        println("latency = " + latency)
        if (ts != 0) latencies = latencies :+ latency
        batchDF.show()
        if (cnt >= KafkaDataGenerator.limit) {
          finished = true
          println("Overall time = " + (System.currentTimeMillis() - startTs))
          println("Avg latency = " + (latencies.sum / latencies.size))
        }
      }
//      .trigger(Trigger.Continuous("1 second"))
      .trigger(Trigger.ProcessingTime(100))
      .outputMode(OutputMode.Complete())
      .start()

    createQueryShutdownThread(query)

    KafkaDataGenerator.start()

    startTs = System.currentTimeMillis()

    query.awaitTermination()
  }

  def createQueryShutdownThread(query: StreamingQuery): Unit = {
    val shutdownThread = new Thread("shutdownChecker") {
      setDaemon(true)
      override def run(): Unit = {
        while(true) {
          if (finished) {
            query.stop()
            System.exit(0)
          }
          Thread.sleep(10)
        }
      }
    }
    shutdownThread.start()
  }

}