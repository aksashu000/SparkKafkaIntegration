package com.ashu.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object MainKafkaProducer {
  def main(args: Array[String]): Unit = {
    sendDataToKafkaTopic("student", "src/main/resources/student.json")
  }

  val kafkaProducerConfig: Properties = {
    val p = new Properties()
    p.setProperty("bootstrap.servers", "localhost:9092")
    p.setProperty("key.serializer", classOf[StringSerializer].getName)
    p.setProperty("value.serializer", classOf[StringSerializer].getName)
    p
  }

  def sendDataToKafkaTopic(topic:String, file: String): Unit = {
    import scala.io.Source

    val producer = new KafkaProducer[String, String](kafkaProducerConfig)
    val fileHandle = Source.fromFile(file)
    for (value <- fileHandle.getLines()) {
      val key: String = value.split(",")(0).split(":")(1)
      producer.send(new ProducerRecord[String, String](topic, key, value))
    }
    fileHandle.close()
    producer.close()
  }
}
