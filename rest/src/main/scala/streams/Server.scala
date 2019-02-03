package streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.stream._
import akka.stream.scaladsl.{Broadcast, BroadcastHub, Flow, GraphDSL, Keep, Merge, Source}
import slick.basic.DatabasePublisher
import slick.jdbc.{ResultSetConcurrency, ResultSetType}
import spray.json.{BasicFormats, _}

import scala.concurrent.Future
import scala.io.StdIn

case class Task(id: Option[Long], term: String, priority: Int, num: Int)

case class TaskCreated(id: Long)

case class Result(id: Long, results: List[String])

import slick.jdbc.PostgresProfile.api._

import scala.concurrent.duration._

class TaskService(db: Database) {
  import streams.gen.Tables
  def create(task: Task): Future[Long] =
    db
      .run(
        Tables.Task.returning(Tables.Task.map(_.id)) += Tables.TaskRow(0L, task.term, task.num, task.priority)
      )

  def list(id: Long): DatabasePublisher[Task] =
    db.stream(
      Tables.Task
        .filter(_.id >= id)
        .sortBy(_.priority.desc)
        .result
        .withStatementParameters(
          rsType = ResultSetType.ForwardOnly,
          rsConcurrency = ResultSetConcurrency.ReadOnly,
          fetchSize = 10000)
        .transactionally
    ).mapResult(
      task => Task(Some(task.id), task.term, task.priority, task.num)
    )
}

object Server extends SprayJsonSupport with BasicFormats with DefaultJsonProtocol {

  def main(args: Array[String]) {

    implicit val system = ActorSystem("streams-system")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val db = Database.forConfig("streams-test")
    val taskService = new TaskService(db)

    def dummySearch(name: String)(task: Task): Source[(Long, String), NotUsed] = {
      Source((1 to (task.num*2)).toList.map(i => (task.id.get, s"$name:${task.term}_$i"))).take(task.num)
    }

    val searchEngines = List(
      Flow[Task].throttle(10, 1 second).flatMapConcat(dummySearch("google")),
      Flow[Task].throttle(10, 1 second).flatMapConcat(dummySearch("yandex"))
    )

    val searchFlow = Flow.fromGraph(GraphDSL.create() {
      implicit builder =>
        import GraphDSL.Implicits._
        val broadcast = builder.add(Broadcast[Task](searchEngines.size))
        val merge = builder.add(Merge[(Long, String)](searchEngines.size))
        searchEngines.foreach(
          broadcast ~> _ ~> merge
        )
        FlowShape(broadcast.in, merge.out)
    }).groupBy(Int.MaxValue, _._1)
      .map {case (id, url) => (id, Set(url))}
      .reduce[(Long, Set[String])] {case ((id, sa), (_, sb)) => (id, sa ++ sb)}
      .mergeSubstreams
      .map {case (id, urls) => Result(id, urls.toList)}
      .throttle(1, 10 second)

    implicit val jsonStreamingSupport = EntityStreamingSupport.json()
    implicit val taskFormat = jsonFormat4(Task)
    implicit val resultFormat = jsonFormat2(Result)
    implicit val taskCreatedFormat = jsonFormat1(TaskCreated)
    try {
      val routes =
        path("task") {
          post {
            entity(as[Task]) {
              task => {
                complete(taskService.create(task).map(TaskCreated))
              }
            }
          }
        } ~
        path("results") {
          get {
            parameter("from") {
              id => complete {
                Source
                  .fromPublisher(taskService.list(id.toLong))
                  .map(item => {println(item); item})
                  .via(searchFlow)
                  .toMat(BroadcastHub.sink[Result])(Keep.right)
                  .run()
              }
            }
          }
        }
      val port = 8087
      val bindingFuture = Http().bindAndHandle(routes, "localhost", port)

      println(s"http://localhost:$port/\nPress RETURN to stop...")
      StdIn.readLine()
      bindingFuture
        .flatMap(_.unbind())
        .onComplete(_ => system.terminate())
    } finally db.close()
  }
}
