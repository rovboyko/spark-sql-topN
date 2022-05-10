package org.apache.spark.sql.custom

import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}

class DefaultSource extends StreamSourceProvider with DataSourceRegister with StreamSinkProvider {

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {

    (shortName(), CodeContributionGenerator.schema)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {

    CodeContributionGenerator(sqlContext)
  }

  override def shortName(): String = "InMemoryRandomStrings"

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = (batchId: Long, data: DataFrame) => {

    println(batchId)

    data.collect().foreach(println)
  }
}