package org.example

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.types.Row
import org.example.data.generation.KafkaDataGenerator

import java.util.concurrent.ConcurrentHashMap

object FlinkTopN {

  var startTs = 0L

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.executeSql(
      s"""
         |CREATE TABLE KafkaTable (
         |  `name` STRING,
         |  `lines` BIGINT,
         |  `ts` BIGINT
         |) WITH (
         |  'connector' = 'kafka',
         |  'topic' = 'input-topic',
         |  'properties.bootstrap.servers' = 'localhost:9092',
         |  'properties.group.id' = 'test',
         |  'scan.startup.mode' = 'earliest-offset',
         |  'format' = 'csv'
         |)
         |
         |""".stripMargin
    )

    KafkaDataGenerator.start()

    val sink = new TestSinkFunction
    tEnv.sqlQuery(
      s"""
         |select * from (
         |select name, sum(lines) as sum_lines, count(*) as cnt, max(ts) as ts,
         |      row_number() over(order by sum(lines) desc) as rn
         |  from KafkaTable
         |  group by name
         |) where rn < 4
         |""".stripMargin
    ).toRetractStream[Row].addSink(sink).setParallelism(1)

    startTs = System.currentTimeMillis()
    env.execute

  }
}

class TestSinkFunction extends RichSinkFunction[(Boolean, Row)] {
  val map = new ConcurrentHashMap[String, Row]()
  var latencies: Seq[Long] = Seq()
  override def invoke(value: (Boolean, Row), context: SinkFunction.Context): Unit = {

    if (value._1)
      map.put(value._2.getField("name").asInstanceOf[String], value._2)

    val sumCnt = map.values().stream()
      .mapToLong(r => r.getField("cnt").asInstanceOf[Long])
      .sum()

    if (sumCnt % 100000 == 0) {
      val avgTs = map.values().stream()
        .mapToLong(r => r.getField("ts").asInstanceOf[Long])
        .average().orElse(0)
      val latency = (System.currentTimeMillis() - avgTs).asInstanceOf[Long]
      println("sumCnt = " + sumCnt)
      println("latency = " + latency)
      latencies = latencies :+ latency
    }

    if (sumCnt >= KafkaDataGenerator.limit) {
      println("Overall time = " + (System.currentTimeMillis() - FlinkTopN.startTs))
      println("Avg latency = " + (latencies.sum / latencies.size))
      System.exit(0)
    }
  }
}
