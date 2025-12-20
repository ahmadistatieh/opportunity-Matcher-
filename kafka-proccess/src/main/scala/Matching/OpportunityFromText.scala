package Matching

import scala.util.Try
import scala.util.matching.Regex

object OpportunityFromText {

  private val bulletPrefix = """(?:-|\u2022|\*|\u2013|\u2014)\s*"""

  private def extractSectionBlock(t: String, sectionHeaderRegex: String): Option[String] = {
    val pattern =
      (s"(?is)$sectionHeaderRegex\\s*:\\s*\\n(.*?)(?:\\n\\s*\\w[^\\n]{0,40}\\s*:\\s*\\n|\\z)").r
    pattern.findFirstMatchIn(t).map(_.group(1))
  }

  private def extractBullets(t: String, sectionHeaderRegex: String): Seq[String] = {
    extractSectionBlock(t, sectionHeaderRegex).toSeq.flatMap { block =>
      block
        .split("\n")
        .toSeq
        .map(_.trim)
        .filter(_.nonEmpty)
        .flatMap { line =>
          val isBullet = line.matches(s"^$bulletPrefix.+")
          val cleaned  = line.replaceAll(s"^$bulletPrefix", "").trim
          if (isBullet && cleaned.nonEmpty) Some(cleaned) else None
        }
    }
  }

  def infer(text: String): LSHApiMain.OpportunityRequest = {
    val t = Option(text).getOrElse("").trim

    def findOne(r: Regex): Option[String] =
      r.findFirstMatchIn(t).map(_.group(1).trim).filter(_.nonEmpty)

    def findInt(r: Regex): Option[Int] =
      findOne(r).flatMap(s => Try(s.trim.toInt).toOption)

    def findDouble(r: Regex): Option[Double] =
      findOne(r).flatMap(s => Try(s.trim.toDouble).toOption)

    val major =
      findOne("(?i)Major\\s*[:\\-]\\s*(.+)".r).getOrElse("")

    val skills =
      (extractBullets(t, "Required\\s*Skills") ++ extractBullets(t, "Skills")).distinct

    val courses =
      (extractBullets(t, "Relevant\\s*Courses") ++ extractBullets(t, "Courses")).distinct

    val minGpa =
      findDouble("(?i)(?:Minimum\\s*GPA|Min\\s*GPA)\\s*[:\\-]\\s*([0-9]+(?:\\.[0-9]+)?)".r)

    val studyingHours =
      findInt("(?i)Studying\\s*Hours\\s*[:\\-]\\s*(\\d+)".r)

    val training =
      findInt("(?i)(?:Training\\s*Level|Training)\\s*[:\\-]\\s*(\\d+)".r)

    LSHApiMain.OpportunityRequest(
      skills = skills,
      courses = courses,
      major = major,
      minGpa = minGpa,
      training = training,
      studyingHours = studyingHours
    )
  }
}
