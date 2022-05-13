package org.example.data.generation

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import java.util.Properties
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import scala.util.Random

object KafkaDataGenerator {

  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")

  val names = Seq("Паша", "Колян", "Серега")

  val limit = 1000000
  var sent = 0

  def start(): Unit = {
    val recordsPerSecond = 100000


    println("Data generator starting")
    val producer = new KafkaProducer(
      properties, new IntegerSerializer, new StringSerializer
    )

    def sendToKafka(recordCount : Int): Unit = try {
      if (sent < limit) {
        for (value <- generate(recordCount)) {
          producer.send(new ProducerRecord("input-topic", null, value))
          sent += 1
        }
      }
      println("sent = " + sent)
    } catch {
      case e: Exception =>
        println("Exception while producing", e)
        System.exit(1)
    }

    def generate(recordCount : Int): Seq[String] = {
      (0 until recordCount)
        .map(_ => names(Random.nextInt(names.size))) // get random name
        .map(name => (name, Random.nextInt(100)))    // get random lines cnt
        .map(tuple => (tuple._1, tuple._2, System.currentTimeMillis())) // add timestamps
        .map(tuple => tuple.productIterator.mkString(","))
    }

    val ex = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      def run(): Unit = sendToKafka(recordsPerSecond)
    }
    val sched = ex.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        println("Data generator stopping")
        sched.cancel(false)
        producer.close()
        println("Data generator stopped")
      }
    })
    println("Data generator started")
  }
}
