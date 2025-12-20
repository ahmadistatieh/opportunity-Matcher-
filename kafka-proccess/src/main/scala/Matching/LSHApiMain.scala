package Matching

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object LSHApiMain extends App {

  implicit val system: ActorSystem = ActorSystem("lsh-api-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = system.dispatcher

  case class OpportunityRequest(
                                 skills: Seq[String] = Seq.empty,
                                 courses: Seq[String] = Seq.empty,
                                 major: String = "",
                                 minGpa: Option[Double] = None,
                                 minSimilarity: Option[Double] = None,
                                 training: Option[Int] = None,
                                 studyingHours: Option[Int] = None
                               )

  case class StudentResponse(
                              id: String,
                              full_name: String,
                              email: String,
                              major: String,
                              gpa_d: Double,
                              skills_arr: Seq[String],
                              courses_arr: Seq[String],
                              training: Option[Int],
                              studying_hours: Option[Int],
                              similarity: Double
                            )

  implicit val oppFormat: RootJsonFormat[OpportunityRequest] = jsonFormat7(OpportunityRequest)
  implicit val studentFormat: RootJsonFormat[StudentResponse] = jsonFormat10(StudentResponse)

  private def jsonError(status: StatusCode, message: String): HttpResponse =
    HttpResponse(
      status = status,
      entity = HttpEntity(
        ContentTypes.`application/json`,
        JsObject("error" -> JsString(message)).compactPrint
      )
    )

  private def getSeqString(row: org.apache.spark.sql.Row, col: String): Seq[String] =
    Option(row.getAs[Any](col)) match {
      case Some(seq: Seq[_])   => seq.map(_.toString)
      case Some(arr: Array[_]) => arr.map(_.toString).toSeq
      case Some(v)             => Seq(v.toString)
      case None                => Seq.empty
    }

  private def getIntOption(row: org.apache.spark.sql.Row, col: String): Option[Int] =
    Option(row.getAs[Any](col)) match {
      case Some(i: Int)    => Some(i)
      case Some(d: Double) => Some(d.toInt)
      case Some(s: String) => Try(s.toInt).toOption
      case _               => None
    }

  private def normalizeReq(req: OpportunityRequest): OpportunityRequest =
    req.copy(
      skills = req.skills.map(_.trim).filter(_.nonEmpty),
      courses = req.courses.map(_.trim).filter(_.nonEmpty),
      major = req.major.trim
    )

  private def hasAtLeastOneInput(req: OpportunityRequest): Boolean =
    req.skills.nonEmpty || req.courses.nonEmpty || req.major.nonEmpty || req.training.nonEmpty || req.studyingHours.nonEmpty

  private def runMatching(req: OpportunityRequest): HttpResponse = {
    val normalized = normalizeReq(req)

    if (!hasAtLeastOneInput(normalized)) {
      jsonError(
        StatusCodes.BadRequest,
        "Request must include at least one of: skills, courses, major, training, studyingHours"
      )
    } else {
      val opp = LSHMatcher.OpportunityReq(
        skills = normalized.skills,
        courses = normalized.courses,
        major = normalized.major,
        minGpa = normalized.minGpa,
        training = normalized.training,
        studyingHours = normalized.studyingHours
      )

      val minSim = normalized.minSimilarity.getOrElse(0.70)

      val df = LSHMatcher.top5ForOpportunity(
        spark = spark,
        mongoUri = mongoUri,
        req = opp,
        minSimilarity = minSim
      )

      val students = df.collect().map { row =>
        StudentResponse(
          id = Option(row.getAs[Any]("id")).map(_.toString).getOrElse(""),
          full_name = Option(row.getAs[String]("full_name")).getOrElse(""),
          email = Option(row.getAs[String]("email")).getOrElse(""),
          major = Option(row.getAs[String]("major")).getOrElse(""),
          gpa_d = Try(row.getAs[Double]("gpa_d")).getOrElse(0.0),
          skills_arr = getSeqString(row, "skills_arr"),
          courses_arr = getSeqString(row, "courses_arr"),
          training = getIntOption(row, "training"),
          studying_hours = getIntOption(row, "studying_hours"),
          similarity = Try(row.getAs[Double]("similarity")).getOrElse(0.0)
        )
      }.toList

      HttpResponse(
        status = StatusCodes.OK,
        entity = HttpEntity(ContentTypes.`application/json`, students.toJson.compactPrint)
      )
    }
  }

  private def parseOptDouble(s: String): Option[Double] = Try(s.trim.toDouble).toOption
  private def parseOptInt(s: String): Option[Int] = Try(s.trim.toInt).toOption

  private def splitCsvLike(s: String): Seq[String] =
    s.split("[,\n;]").toSeq.map(_.trim).filter(_.nonEmpty)

  private def reqFromFields(fields: Map[String, String]): OpportunityRequest =
    OpportunityRequest(
      skills = fields.get("skills").map(splitCsvLike).getOrElse(Seq.empty),
      courses = fields.get("courses").map(splitCsvLike).getOrElse(Seq.empty),
      major = fields.getOrElse("major", ""),
      minGpa = fields.get("minGpa").flatMap(parseOptDouble),
      minSimilarity = fields.get("minSimilarity").flatMap(parseOptDouble),
      training = fields.get("training").flatMap(parseOptInt),
      studyingHours = fields.get("studyingHours").flatMap(parseOptInt)
    )

  val spark = SparkSession.builder()
    .appName("LSH API Server")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  val mongoUri = "mongodb://localhost:27017"

  private val corsHeaders = List(
    `Access-Control-Allow-Origin`.*,
    `Access-Control-Allow-Headers`("Content-Type", "Authorization"),
    `Access-Control-Allow-Methods`(HttpMethods.GET, HttpMethods.POST, HttpMethods.OPTIONS)
  )

  private def withCors(route: Route): Route =
    respondWithHeaders(corsHeaders) {
      concat(
        options { complete(StatusCodes.OK) },
        route
      )
    }

  implicit val rejectionHandler: RejectionHandler =
    RejectionHandler.newBuilder()
      .handleNotFound { complete(jsonError(StatusCodes.NotFound, "Route not found")) }
      .result()

  implicit val exceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case ex: DeserializationException =>
        complete(jsonError(StatusCodes.BadRequest, s"Invalid JSON format: ${ex.getMessage}"))
      case ex: Exception =>
        ex.printStackTrace()
        complete(jsonError(StatusCodes.InternalServerError, s"Server error: ${ex.getMessage}"))
    }

  val route: Route =
    withCors {
      concat(

        path("match_students") {
          post {
            entity(as[String]) { body =>
              Try(body.parseJson.convertTo[OpportunityRequest]) match {
                case Failure(err) =>
                  complete(jsonError(StatusCodes.BadRequest, s"Invalid request JSON: ${err.getMessage}"))
                case Success(req) =>
                  complete(runMatching(req))
              }
            }
          }
        },

        path("match_students_file") {
          post {
            withSizeLimit(15 * 1024 * 1024) { // 15MB
              entity(as[Multipart.FormData]) { formData =>

                val strictF = formData.toStrict(10.seconds)

                onComplete(strictF) {
                  case Failure(ex) =>
                    complete(jsonError(StatusCodes.BadRequest, s"Invalid multipart data: ${ex.getMessage}"))

                  case Success(strict) =>
                    val parts = strict.strictParts.toVector

                    val fields: Map[String, String] =
                      parts
                        .filter(p => p.filename.isEmpty)
                        .map(p => p.name -> p.entity.data.utf8String)
                        .toMap

                    val filePartOpt = parts.find(p => p.name == "file" && p.filename.nonEmpty)

                    filePartOpt match {
                      case None =>
                        complete(jsonError(StatusCodes.BadRequest, "Missing multipart field: file"))

                      case Some(filePart) =>
                        val originalName = filePart.filename.getOrElse("upload.bin")
                        val tmp: Path =
                          Files.createTempFile("opportunity_upload_", "_" + originalName.replaceAll("\\s+", "_"))

                        Files.write(tmp, filePart.entity.data.toArray)

                        val extractedText = Try(TextExtractor.extractText(tmp)).getOrElse("")
                        Try(Files.deleteIfExists(tmp))

                        val fromFields = normalizeReq(reqFromFields(fields))

                        val finalReq =
                          if (hasAtLeastOneInput(fromFields)) {
                            fromFields
                          } else {
                            val inferred = OpportunityFromText.infer(extractedText)
                            inferred.copy(
                              minGpa = fromFields.minGpa.orElse(inferred.minGpa),
                              minSimilarity = fromFields.minSimilarity.orElse(inferred.minSimilarity)
                            )
                          }

                        complete(runMatching(finalReq))
                    }
                }
              }
            }
          }
        },

        pathSingleSlash {
          get { complete("LSH API is running. POST /match_students or POST /match_students_file") }
        }
      )
    }

  Http().newServerAt("localhost", 8080).bind(route)
  println("Server running at http://localhost:8080/")
}
