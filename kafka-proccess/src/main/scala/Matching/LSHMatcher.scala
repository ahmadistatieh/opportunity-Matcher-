package Matching

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, MinHashLSH}
import org.apache.spark.ml.linalg.Vector

object LSHMatcher {

  case class OpportunityReq(
                             skills: Seq[String],
                             courses: Seq[String],
                             major: String,
                             minGpa: Option[Double]
                           )

  private def normalizeToken(s: String): String =
    s.trim.toLowerCase.replaceAll("\\s+", "_")

  private def buildTokens(skills: Seq[String], courses: Seq[String], major: String): Seq[String] = {
    val skillTokens  = skills.filter(_.trim.nonEmpty).map(x => s"skill:${normalizeToken(x)}")
    val courseTokens = courses.filter(_.trim.nonEmpty).map(x => s"course:${normalizeToken(x)}")
    val majorToken =
      if (major == null || major.trim.isEmpty) Seq.empty
      else Seq(s"major:${normalizeToken(major)}")

    skillTokens ++ courseTokens ++ majorToken
  }

  def loadStudents(spark: SparkSession, mongoUri: String): DataFrame = {
    spark.read
      .format("mongodb")
      .option("uri", mongoUri)
      .option("database", "opportunityMatcher")
      .option("collection", "students")
      .load()
      .withColumn("id", col("_id").cast("string"))
      .withColumn("gpa_d", regexp_replace(col("gpa"), ",", ".").cast("double"))
      .withColumn("skills_arr", col("skills"))
      .withColumn("courses_arr", col("courses"))
  }

  def top5ForOpportunity(
                          spark: SparkSession,
                          mongoUri: String,
                          req: OpportunityReq,
                          minSimilarity: Double = 0.70
                        ): DataFrame = {

    val students = loadStudents(spark, mongoUri)
      .filter(col("id").isNotNull)

    val studentsTok = students.withColumn(
      "tokens",
      array_distinct(
        concat(
          transform(col("skills_arr"),  x => concat(lit("skill:"), lower(regexp_replace(trim(x), "\\s+", "_")))),
          transform(col("courses_arr"), x => concat(lit("course:"), lower(regexp_replace(trim(x), "\\s+", "_")))),
          when(col("major").isNotNull && length(trim(col("major"))) > 0,
            array(concat(lit("major:"), lower(regexp_replace(trim(col("major")), "\\s+", "_"))))
          ).otherwise(array())
        )
      )
    )

    val hashingTF = new HashingTF()
      .setInputCol("tokens")
      .setOutputCol("features")
      .setNumFeatures(1 << 18)
      .setBinary(true)

    val featurizedStudents = hashingTF.transform(studentsTok)

    val mh = new MinHashLSH()
      .setInputCol("features")
      .setOutputCol("hashes")
      .setNumHashTables(5)

    val model = mh.fit(featurizedStudents)

    val oppTokens = buildTokens(req.skills, req.courses, req.major)

    import spark.implicits._
    val oppDF = Seq((1, oppTokens)).toDF("opp_id", "tokens")
    val featurizedOpp = hashingTF.transform(oppDF)

    val keyVec: Vector =
      featurizedOpp.select("features").head().getAs[Vector]("features")

    val k = 200
    val joined = model.approxNearestNeighbors(
      featurizedStudents,
      keyVec,
      k,
      "jaccard_distance"
    )

    val withSim = joined.withColumn("similarity", lit(1.0) - col("jaccard_distance"))

    val afterGpa =
      req.minGpa match {
        case Some(minG) => withSim.filter(col("gpa_d") >= lit(minG))
        case None       => withSim
      }

    afterGpa
      .filter(col("similarity") >= lit(minSimilarity))
      .orderBy(col("similarity").desc)
      .select("id", "full_name", "email", "major", "gpa_d", "skills_arr", "courses_arr", "similarity")
      .limit(5)
  }
}
