package Producer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

object KafkaProducerData {

  val progressFile = "C:/tmp/kafka_progress/last_sent_id.txt"
  val BATCH_SIZE = 500

  def readLastSentId(): Int = {
    val p = Paths.get(progressFile)
    if (!Files.exists(p)) 0
    else new String(Files.readAllBytes(p), StandardCharsets.UTF_8).trim.toInt
  }

  def writeLastSentId(id: Int): Unit = {
    val p = Paths.get(progressFile)
    Files.createDirectories(p.getParent)
    Files.write(p, id.toString.getBytes(StandardCharsets.UTF_8))
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StaticDataProducer")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    val path ="C:\\Users\\HITECH\\OneDrive\\Desktop\\kafka-proccess\\kafka-proccess\\src\\main\\Student_CVs\\students_CVs.csv"
    val topic = "Data"

    val arrSchema = ArrayType(StringType)
    val df = spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(path)
      .withColumn("id", col("id").cast(IntegerType))
      .withColumn("studying_hours", col("studying_hours").cast(IntegerType))
      .withColumn("training", col("training").cast(IntegerType))
      .withColumn("skills", from_json(col("skills"), arrSchema))
      .withColumn("courses", from_json(col("courses"), arrSchema))
      .withColumn("created_at", to_date(col("created_at"), "M/d/yyyy"))
      .withColumn("updated_at", to_date(col("updated_at"), "M/d/yyyy"))
      .filter(col("id").isNotNull)

    val lastSentId = readLastSentId()
    println(s" Last sent id = $lastSentId")

    val dfNew = df
      .filter(col("id") > lastSentId)
      .orderBy(col("id").asc)

    val dfJson = dfNew.select(
      col("id"),
      to_json(struct(dfNew.columns.map(col): _*)).as("value")
    )

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("enable.idempotence", "true")

    val producer = new KafkaProducer[String, String](props)

    try {
      val it = dfJson.toLocalIterator()
      var batch = Vector.empty[(Int, String)]
      var batchId = 1
      var maxIdSent = lastSentId

      while (it.hasNext) {
        val row = it.next()
        val id = row.getAs[Int]("id")
        val json = row.getAs[String]("value")

        batch = batch :+ (id, json)

        if (batch.size == BATCH_SIZE) {
          println(s" Sending batch #$batchId (size=500)")
          batch.foreach { case (i, v) =>
            producer.send(new ProducerRecord[String, String](topic, i.toString, v))
          }
          producer.flush()
          maxIdSent = batch.last._1
          writeLastSentId(maxIdSent)
          batch = Vector.empty
          batchId += 1
          Thread.sleep(20000)
        }
      }

      if (batch.nonEmpty) {
        println(s" Sending batch #$batchId (size=${batch.size})")
        batch.foreach { case (i, v) =>
          producer.send(new ProducerRecord[String, String](topic, i.toString, v))
        }
        producer.flush()
        writeLastSentId(batch.last._1)
      }
      println(s" Finished sending. Last ID = ${readLastSentId()}")
    } finally {
      producer.close()
      spark.stop()
    }
  }
}