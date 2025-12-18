package Consumer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object kafkaConsumer {

  private val schema = StructType(Seq(
    StructField("id", IntegerType, true),
    StructField("full_name", StringType, true),
    StructField("email", StringType, true),
    StructField("gender", StringType, true),
    StructField("address", StringType, true),
    StructField("birth_date", StringType, true),
    StructField("gpa", StringType, true),
    StructField("major", StringType, true),
    StructField("skills", ArrayType(StringType), true),
    StructField("courses", ArrayType(StringType), true),
    StructField("studying_hours", IntegerType, true),
    StructField("training", IntegerType, true),
    StructField("created_at", StringType, true),
    StructField("updated_at", StringType, true)
  ))

  def readAndCleanStream(
                          spark: SparkSession,
                          bootstrapServers: String,
                          topic: String,
                          startingOffsets: String = "latest"
                        ): DataFrame = {
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .load()

    val parsed = kafkaStream
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")
      .filter(col("id").isNotNull)

    parsed
      .drop("gender", "address", "birth_date")
      .withColumn("_id", col("id"))
      .drop("id")
      .withColumn("created_at", to_date(col("created_at")))
      .withColumn("updated_at", to_date(col("updated_at")))
  }
}