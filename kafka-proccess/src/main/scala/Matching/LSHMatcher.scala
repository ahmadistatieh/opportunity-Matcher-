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
                             minGpa: Option[Double],
                             training: Option[Int] = None,
                             studyingHours: Option[Int] = None
                           )

  private def normalizeToken(s: String): String =
    s.trim.toLowerCase.replaceAll("\\s+", "_")

  private def buildTokens(skills: Seq[String], courses: Seq[String], major: String,
                          training: Option[Int]): Seq[String] = {
    val skillTokens  = skills.filter(_.trim.nonEmpty).map(x => s"skill:${normalizeToken(x)}")
    val courseTokens = courses.filter(_.trim.nonEmpty).map(x => s"course:${normalizeToken(x)}")
    val majorToken   = if (major.trim.nonEmpty) Seq(s"major:${normalizeToken(major)}") else Seq.empty
    val trainingToken = training.map(t => s"training:$t").toSeq

    skillTokens ++ courseTokens ++ majorToken ++ trainingToken
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
      .withColumn("training", col("training").cast("int"))
      .withColumn("studying_hours", col("studying_hours").cast("int"))
  }

  def top5ForOpportunity(
                          spark: SparkSession,
                          mongoUri: String,
                          req: OpportunityReq,
                          minSimilarity: Double = 0.70,
                          minMatchRatio: Double = 0.5,
                          numHashTables: Int = 10,
                          k: Int = 500
                        ): DataFrame = {

    val students = loadStudents(spark, mongoUri).filter(col("id").isNotNull)

    val studentsTok = students.withColumn(
      "tokens",
      array_distinct(
        concat(
          transform(col("skills_arr"), x => concat(lit("skill:"), lower(regexp_replace(trim(x), "\\s+", "_")))),
          transform(col("courses_arr"), x => concat(lit("course:"), lower(regexp_replace(trim(x), "\\s+", "_")))),
          when(col("major").isNotNull && length(trim(col("major"))) > 0,
            array(concat(lit("major:"), lower(regexp_replace(trim(col("major")), "\\s+", "_"))))
          ).otherwise(array()),
          when(col("training").isNotNull, array(concat(lit("training:"), col("training").cast("string")))).otherwise(array())
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
      .setNumHashTables(numHashTables)

    val model = mh.fit(featurizedStudents)

    val oppTokens = buildTokens(req.skills, req.courses, req.major, req.training)

    import spark.implicits._
    val oppDF = Seq((1, oppTokens)).toDF("opp_id", "tokens")
    val featurizedOpp = hashingTF.transform(oppDF)

    val keyVec: Vector = featurizedOpp.select("features").head().getAs[Vector]("features")

    val joined = model.approxNearestNeighbors(
      featurizedStudents,
      keyVec,
      k,
      "jaccard_distance"
    )

    val withSim = joined.withColumn("similarity", lit(1.0) - col("jaccard_distance"))

    // فلترة حسب GPA و studying_hours (minimum)
    val afterGpaAndStudying = withSim
      .filter(
        req.minGpa match {
          case Some(minG) => col("gpa_d") >= lit(minG)
          case None       => lit(true)
        }
      )
      .filter(
        req.studyingHours match {
          case Some(minH) => col("studying_hours") >= lit(minH)
          case None       => lit(true)
        }
      )

    val reqTokensArray = array(oppTokens.map(lit): _*)

    val afterPostFilter = afterGpaAndStudying.withColumn(
      "matching_count", size(array_intersect(col("tokens"), reqTokensArray))
    ).withColumn(
      "total_tokens", lit(oppTokens.size)
    ).withColumn(
      "match_ratio", col("matching_count") / col("total_tokens")
    ).withColumn(
      "final_score", col("similarity") * 0.7 + col("match_ratio") * 0.3
    ).filter(col("match_ratio") >= lit(minMatchRatio))

    afterPostFilter
      .filter(col("similarity") >= lit(minSimilarity))
      .orderBy(col("final_score").desc)
      .select(
        "id", "full_name", "email", "major", "gpa_d",
        "skills_arr", "courses_arr", "training", "studying_hours",
        "similarity", "match_ratio", "final_score"
      )
      .limit(10)
  }
}
