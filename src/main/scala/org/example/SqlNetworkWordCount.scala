package org.example

/**
 * Hello world!
 *
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
 * Use DataFrames and SQL to count words in UTF8 encoded, '\n' delimited text received from the
 * network every second.
 *
 * Usage: org.test.spark.streaming.sql.SqlNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.org.test.spark.streaming.sql.SqlNetworkWordCount localhost 9999`
 */

object SqlNetworkWordCount extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    setStreamingLogLevels()

    // Create the context with a 2 second batch size
    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount")
    sparkConf.setMaster("local[2]")
//    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val host = args(0)
    val port = args(1).toInt

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val input = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "2")
//      .option("port", port)
      .load()

    // Split the lines into words
//    val words = lines.as[String].flatMap(_.split(" ")).toDF("word")
    val rates = input.as[(Long, Long)].toDF("ts", "value")

//    words.createOrReplaceTempView("words")
    rates.createOrReplaceTempView("rates")
//    val wordCounts = spark.sql("select word, count(*) as total from words group by word")
    val ratesCount = spark.sql("select value, count(*) as total from rates group by value")

    // Generate running word count
//    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = ratesCount.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }

  def setStreamingLogLevels(): Unit = {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}


/** Case class for converting RDD to DataFrame */
case class Record(word: String)


/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}