package Consumer

import org.apache.spark.sql.{DataFrame, SaveMode}

object MongoWriter {

  def writeBatch(
                  batchDF: DataFrame,
                  mongoUri: String,
                  db: String,
                  collection: String
                ): Unit = {

    batchDF.write
      .format("mongodb")
      .mode(SaveMode.Append) // ✅ الصح
      .option("spark.mongodb.write.connection.uri", mongoUri)
      .option("spark.mongodb.write.database", db)
      .option("spark.mongodb.write.collection", collection)
      .save()
  }
}
