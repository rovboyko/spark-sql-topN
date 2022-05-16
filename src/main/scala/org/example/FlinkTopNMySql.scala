package org.example

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.example.data.generation.KafkaDataGenerator

object FlinkTopNMySql {

  var startTs = 0L

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration)

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

    tEnv.executeSql(
      s"""
         |CREATE TABLE MysqlTable (
         | `name` STRING,
         | `lines` BIGINT,
         | `cnt` BIGINT,
         | `ts` BIGINT,
         | `rank` BIGINT PRIMARY KEY NOT ENFORCED
         |) WITH (
         |  'connector' = 'jdbc',
         |  'url' = 'jdbc:mysql://localhost:3306/flink',
         |  'table-name' = 'result',
         |  'username' = 'root',
         |  'password' = 'pass'
         |)
         |
         |""".stripMargin
    )

    KafkaDataGenerator.start()

    tEnv.executeSql(
      s"""
         |insert into MysqlTable
         |select name, sum_lines, cnt, ts, rn
         |from (
         |select name, sum(lines) as sum_lines, count(*) as cnt, max(ts) as ts,
         |      row_number() over(order by sum(lines) desc) as rn
         |  from KafkaTable
         |  group by name
         |) where rn <= 2
         |""".stripMargin
    )

  }
}
