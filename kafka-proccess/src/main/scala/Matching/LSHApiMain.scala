package Matching

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object LSHApiMain extends App {

  implicit val system: ActorSystem = ActorSystem("lsh-api-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

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

  implicit val oppFormat: RootJsonFormat[OpportunityRequest] =
    jsonFormat7(OpportunityRequest)

  implicit val studentFormat: RootJsonFormat[StudentResponse] =
    jsonFormat10(StudentResponse)

  private def jsonError(status: StatusCode, message: String): HttpResponse =
    HttpResponse(
      status = status,
      entity = HttpEntity(
        ContentTypes.`application/json`,
        JsObject("error" -> JsString(message)).compactPrint
      )
    )

  val spark = SparkSession.builder()
    .appName("LSH API Server")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val mongoUri = "mongodb://localhost:27017"

  private val corsHeaders = List(
    `Access-Control-Allow-Origin`.*,
    `Access-Control-Allow-Headers`("Content-Type"),
    `Access-Control-Allow-Methods`(HttpMethods.GET, HttpMethods.POST, HttpMethods.OPTIONS)
  )

  private def withCors(route: Route) =
    respondWithHeaders(corsHeaders) {
      options { complete(StatusCodes.OK) } ~ route
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

  val route =
    withCors {
      path("match_students") {
        post {
          entity(as[String]) { body =>
            Try(body.parseJson.convertTo[OpportunityRequest]) match {

              case Failure(err) =>
                complete(jsonError(StatusCodes.BadRequest, s"Invalid request JSON: ${err.getMessage}"))

              case Success(req) =>
                val skills  = req.skills.map(_.trim).filter(_.nonEmpty)
                val courses = req.courses.map(_.trim).filter(_.nonEmpty)
                val major   = req.major.trim

                if (skills.isEmpty && courses.isEmpty && major.isEmpty && req.training.isEmpty && req.studyingHours.isEmpty) {
                  complete(jsonError(
                    StatusCodes.BadRequest,
                    "Request must include at least one of: skills, courses, major, training, studyingHours"
                  ))
                } else {

                  val opp = LSHMatcher.OpportunityReq(
                    skills = skills,
                    courses = courses,
                    major = major,
                    minGpa = req.minGpa,
                    training = req.training,
                    studyingHours = req.studyingHours
                  )

                  val minSim = req.minSimilarity.getOrElse(0.70)

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

                  complete(HttpEntity(ContentTypes.`application/json`, students.toJson.compactPrint))
                }
            }
          }
        }
      } ~
        pathSingleSlash {
          get { complete("LSH API is running. POST /match_students") }
        }
    }

  Http().newServerAt("localhost", 8080).bind(route)
  println(" Server running at http://localhost:8080/")
}
