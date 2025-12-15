package Producer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object KafkaProducerData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StaticDataProducer")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    val path =
      "C:/kafka-proccess/kafka-proccess/src/main/Student_CVs/students_CVs.csv"


    val dfRaw = spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(path)

    val arrSchema = ArrayType(StringType)

    val df = dfRaw
      .withColumn("studying_hours", col("studying_hours").cast(IntegerType))
      .withColumn("training", col("training").cast(IntegerType))
      .withColumn("skills", from_json(col("skills"), arrSchema))
      .withColumn("courses", from_json(col("courses"), arrSchema))
      .withColumn("created_at", to_date(col("created_at"), "M/d/yyyy"))
      .withColumn("updated_at", to_date(col("updated_at"), "M/d/yyyy"))

    val dfJson = df.select(
      to_json(struct(df.columns.map(col): _*)).as("value")
    )

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    try {
      val rows = dfJson.collect()

      println(s"Total rows read from CSV: ${rows.length}")

      rows.grouped(500).zipWithIndex.foreach {
        case (batch, idx) =>
          println(s"Sending batch #${idx + 1} (size = ${batch.size})")

          batch.foreach { r =>
            producer.send(
              new ProducerRecord[String, String](
                "staticData",
                r.getAs[String]("value")
              )
            )
          }

          producer.flush()
          Thread.sleep(20000)
      }

    } finally {
      producer.close()
      spark.stop()
    }
  }
}
