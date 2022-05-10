package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

object TopN {


  def main(args: Array[String]): Unit = {

    val config = new org.apache.spark.SparkConf().setAppName("testing streaming")
    config.setMaster("local[2]")

    val sparkSession = SparkSession
      .builder()
      .config(config)
      .getOrCreate()

    sparkSession.conf.set("spark.sql.streaming.schemaInference", value = true)
    sparkSession.sparkContext.setLogLevel("WARN")

    val s = StructType(List(StructField("name", StringType), StructField("lines", LongType)))


    val r = sparkSession
      .readStream
      .format("org.apache.spark.sql.custom.DefaultSource")
      .schema(s)
      .load()

    r.createTempView("tmpView")

    sparkSession
      .sql("" +
        " select name, s, row_number() over (order by s desc) rn" +
        " from (" +
        "   select name, sum(lines) as s from tmpView " +
        "   group by name)")
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime(100))
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }
}