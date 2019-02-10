package streams

import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.stream._
import akka.stream.scaladsl.{Broadcast, BroadcastHub, Flow, GraphDSL, Keep, Merge, MergeHub, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
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

  def list(id: Long) =
    Source
      .fromPublisher(
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

    val searchEngines2 = List(
      Flow[(Task, UUID)].throttle(10, 1 second).flatMapConcat{case (task, uuid) => dummySearch("google")(task).map{case (id, url) => (id, url, uuid)}},
      Flow[(Task, UUID)].throttle(10, 1 second).flatMapConcat{case (task, uuid) => dummySearch("yandex")(task).map{case (id, url) => (id, url, uuid)}}
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
      .map (item => {println("before reduce:" + item);item})
      .reduce[(Long, Set[String])] {case ((id, sa), (_, sb)) => (id, sa ++ sb)}
      .map (item => {println("after reduce:" + item);item})
      .mergeSubstreams
      .map {case (id, urls) => Result(id, urls.toList)}
      .throttle(1, 10 second)

    val searchFlow2 = Flow.fromGraph(GraphDSL.create() {
      implicit builder =>
        import GraphDSL.Implicits._
        val broadcast = builder.add(Broadcast[(Task, UUID)](searchEngines.size))
        val merge = builder.add(Merge[(Long, String, UUID)](searchEngines.size))
        searchEngines2.foreach(
          broadcast ~> _ ~> merge
        )
        FlowShape(broadcast.in, merge.out)
    }).groupBy(Int.MaxValue, item => item._1 + ":" + item._3)
      .map {case (id, url, uuid) => (id, Set(url), uuid)}
      .map (item => {println("before reduce:" + item);item})
      .reduce[(Long, Set[String], UUID)] {case ((id, sa, uuid), (_, sb, _)) => (id, sa ++ sb, uuid)}
      .map (item => {println("after reduce:" + item);item})
      .mergeSubstreams
      .map {case (id, urls, uuid) => (Result(id, urls.toList), uuid)}
      .throttle(1, 10 second)

    val (sink, source) =
      MergeHub.source[(Task, UUID)]
//        .flatMapConcat{case (id, uuid) => taskService.list(id).map(task => (task, uuid))}
        .via(searchFlow2)
        .toMat(BroadcastHub.sink(bufferSize = 256))(Keep.both)
        .run()

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
                taskService.list(id.toLong)
                  .map(item => {println(item); item})
                  .via(searchFlow)
                  .toMat(BroadcastHub.sink[Result])(Keep.right)
                  .run()
              }
            }
          }
        } ~
        path("res2") {
          get {
            parameter("from") {
              id => complete {
                val requestUUID = UUID.randomUUID()
                taskService.list(id.toLong).map(task=>(task, requestUUID)).runWith(sink)
//                Source.single((id.toLong, requestUUID)).runWith(sink)
                source.collect{case (result, uuid) if uuid==requestUUID => result}
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
