package com.ashu
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object SparkKafkaIntegrationDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Kafka")
      .getOrCreate()

    val bootStrapServer = "localhost:9092"
    val topic = "student"
    val destTopic = "student1"
    val startingOffsets = "earliest"

    /*
      Use Kafka console producer to load JSON data to the topic. Sample data kept in <project_root>/src/main/resources/student.json
      You can use the below command to load data into Kafka topic from <kafka_home>/bin directory:
      ./kafka-console-producer.sh --broker-list localhost:9092 --topic student < student.json

      Alternatively, you can just run "MainKafkaProducer" program and it will load the student.json data to the 'student' Kafka topic
    */

    val jsonFormatSchema = StructType(List(
      StructField("StudentId", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Age", StringType, nullable = true),
      StructField("DeptId", StringType, nullable = true),
      StructField("YearOfAdmission", StringType, nullable = true)
    )
    )
    import spark.implicits._
    case class Student(studentId: String, name: String, age: String, DeptId: String, yearOfAdmission: String)
    val df:Dataset[Row] = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("subscribe", topic)
      //.option("startingOffsets", """{"student":{"0":0, "1":4, "2":0}}""") //Partition 0  offSet 0, partition 1 offset 4, partition 2 offset 0
      .option("startingOffsets", startingOffsets)
      .load()
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", schema=jsonFormatSchema).as("data"))
      .select("data.*")

    val streamListener = new StreamingQueryListener() {

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
        println("Input rows per second: " + queryProgress.progress.inputRowsPerSecond)
        println("Processed rows per second: " + queryProgress.progress.processedRowsPerSecond)

        // Query the in memory table for the current data order by 'age' desc
        val currentDf = spark.sql("select * from student_data_order_by_age_desc order by age desc")
        currentDf.show()
      }

      // We don't want to do anything with start or termination, but we have to override them anyway
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = { }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = { }
    }

    // add the new listener
    spark.streams.addListener(streamListener)

    df
      .writeStream
      .queryName("student_data_order_by_age_desc")
      //.outputMode("complete")
      .format("memory")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()
      .awaitTermination()

    //Write the data to console
    //df.writeStream.format("console").option("checkPointLocation", "/tmp/kafkaChkPnt").start().awaitTermination()

    /*
    //Write the data to Kafka topic -> an additional column will be added
    df.withColumn("LengthRule", when(length(col("Name")) === 11, "True").otherwise("False"))
      .selectExpr("CAST(StudentId AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServer)
      .option("checkpointLocation", "/tmp/checkPoint2")
      .option("topic", destTopic)
      .option("failOnDataLoss", "false")
      .start()
      .awaitTermination()

     */
  }
}
