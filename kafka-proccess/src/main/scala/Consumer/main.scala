package Consumer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

object main {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("Kafka Consumer -> Mongo")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val bootstrapServers = "localhost:9092"
    val topic = "Data"

    val mongoUri = "mongodb://localhost:27017"
    val dbName = "opportunityMatcher"
    val collectionName = "students"

    val cleanStream = kafkaConsumer.readAndCleanStream(
      spark = spark,
      bootstrapServers = bootstrapServers,
      topic = topic,
      startingOffsets = "earliest"
    )

    val query: StreamingQuery = cleanStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val c = batchDF.count()
        println(s"✅ Batch=$batchId | count=$c")
        if (c > 0) MongoWriter.writeBatch(batchDF, mongoUri, dbName, collectionName)
      }
      .outputMode("append")
      .option("checkpointLocation", "C:/tmp/spark_checkpoints/mongo_students_run5") // ✅ مسار مطلق أفضل
      .start()

    query.awaitTermination()
  }
}
