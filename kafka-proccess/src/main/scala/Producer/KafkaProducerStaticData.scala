package Producer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object KafkaProducerStaticData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StaticDataProducer")
      .master("local[*]")
      .getOrCreate()


    val path = "C:\\Users\\HITECH\\OneDrive\\Desktop\\kafka-proccess\\kafka-proccess\\src\\main\\Student_CVs\\students_CVs.csv"

    val dfRaw = spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", "false")
      .csv(path)

    val arrSchema = ArrayType(StringType)

    val df = dfRaw
      .withColumn("studying_hours", col("studying_hours").cast(IntegerType))
      .withColumn("training", col("training").cast(IntegerType))
      .withColumn("skills", from_json(col("skills"), arrSchema))
      .withColumn("courses", from_json(col("courses"), arrSchema))

    val dfJson = df.select(
      to_json(
        struct(
          col("id"),
          col("full_name"),
          col("email"),
          col("gender"),
          col("address"),
          col("birth_date"),
          col("gpa"),
          col("major"),
          col("skills"),
          col("courses"),
          col("studying_hours"),
          col("training")
        )
      ).as("value")
    )

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    try {
      val rows = dfJson.collect()
      val batches = rows.grouped(500)
      var batchNumber = 1

      batches.foreach { batch =>
        println(s"Sending batch #$batchNumber (size = ${batch.size})")

        batch.foreach { r =>
          val json = r.getAs[String]("value")
          producer.send(new ProducerRecord[String, String]("staticData", json))
        }

        producer.flush()
        println(s"Batch #$batchNumber sent. Waiting 20 seconds...\n")
        batchNumber += 1
        Thread.sleep(20000)
      }

    } finally {
      producer.close()
      spark.stop()
    }
  }
}
