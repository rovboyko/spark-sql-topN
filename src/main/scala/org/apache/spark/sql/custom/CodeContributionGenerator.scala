package org.apache.spark.sql.custom

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, SerializedOffset, Source}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Random

class CodeContributionGenerator private(sqlContext: SQLContext) extends Source {
  private var offset: LongOffset = LongOffset(-1)

  private var batches = collection.mutable.ListBuffer.empty[(String, Int, Long)]

  private val incrementalThread = dataGeneratorStartingThread()
  private val inputDataCnt = 1000000

  override def schema: StructType = CodeContributionGenerator.schema

  override def getOffset: Option[Offset] = this.synchronized {
    println(s"getOffset: $offset")

    if (offset.offset == -1) None else Some(offset)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = this.synchronized {

    val s = start.flatMap(convertToLongOffset).getOrElse(LongOffset(-1)).offset + 1
    val e = convertToLongOffset(end).getOrElse(LongOffset(-1)).offset + 1

    println(s"generating batch range $start ; $end")

    val data = batches
      .par
      .filter { case (_, _, idx) => idx >= s && idx <= e }
      .map { case (v, l, _) => (v, l) }
      .seq

    val rdd = sqlContext
      .sparkContext
      .parallelize(data)
      .map { case (v, l) => InternalRow(UTF8String.fromString(v), l.toLong) }

    sqlContext.sparkSession.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def commit(end: Offset): Unit = this.synchronized {

    val committed = convertToLongOffset(end).getOrElse(LongOffset(-1)).offset
    this.synchronized {
      val toKeep = batches.filter { case (_, _, idx) => idx > committed }

      println(s"after clean size ${toKeep.length}")
      println(s"deleted: ${batches.size - toKeep.size}")

      batches = toKeep
    }
  }

  override def stop(): Unit = incrementalThread.stop()

  private def dataGeneratorStartingThread() = {
    val names = Seq("Пашка", "Колян", "Серега")

    val t = new Thread("increment") {
      setDaemon(true)
      override def run(): Unit = {

        while (offset.offset < inputDataCnt) {
          try {
            this.synchronized {
              offset = offset + 1

              val name = names(Random.nextInt(names.size))
              val lines = Random.nextInt(100)

              batches.append((name, lines, offset.offset))
            }
          } catch {
            case e: Exception => println(e)
          }
          Thread.sleep(10)
        }
      }

    }

    t.start()

    t
  }

  private def convertToLongOffset(offset: Offset): Option[LongOffset] = offset match {
    case lo: LongOffset => Some(lo)
    case so: SerializedOffset => Some(LongOffset(so))
    case _ => None
  }
}

object CodeContributionGenerator {

  def apply(sqlContext: SQLContext): Source = new CodeContributionGenerator(sqlContext)

  lazy val schema: StructType = StructType(List(StructField("name", StringType), StructField("lines", LongType)))
}
