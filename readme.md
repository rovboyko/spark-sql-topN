# Streaming TopN example (Spark and Flink)

The project is intended to show how to organize simple topN calculation using:
- Spark Structured Streaming - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Flink SQL - https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/overview/

## Build the project
``mvn clean install``


## Launch the Spark topN example
- start the kafka and zookeeper: 
``dcoker-compose up -d``
- invoke the ``SparkTopN.main()`` method

## Launch the Flink topN example
- start the kafka and zookeeper:
  ``dcoker-compose up -d``
- invoke the ``FlinkTopN.main()`` method