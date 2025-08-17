import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route.seal
import akka.stream.scaladsl._
import scala.util.Random
import scala.io.StdIn
import akka.util.ByteString
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.XReadArgs
import scala.concurrent.duration._
import scala.concurrent.Await
import collection.JavaConverters.asScalaBufferConverter
import collection.JavaConverters.mapAsJavaMapConverter
import scala.jdk.FutureConverters._
import concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import akka.stream.Materializer
import java.util.Date
import akka.pattern.StatusReply.Success
import akka.Done

object HttpServerRoutingMinimal {

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem(Behaviors.empty, "my-system")
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.executionContext

    val redisClient = RedisClient.create("redis://redis:6379")

    val route =
      concat(
        path("hello") {
          get {
            val numbers =
              Source
                .tick(
                  1.second, // delay of first tick
                  1.second, // delay of subsequent ticks
                  "tick" // element emitted each tick
                )
                .map { _ =>
                  Random.nextInt()
                }

            complete(
              HttpEntity(
                ContentTypes.`text/plain(UTF-8)`,
                numbers.map(n => ByteString(s"$n\n"))
              )
            )
          }
        },
        path("sse") {
          get {
            parameters("user") { (user) =>
              val requestId = Random.nextInt(100).toString
              println(s"request $requestId")

              val source = subscribe2(redisClient, "stream:" + user, requestId)
                .map { message =>
                  ByteString(message.toString() + "\n")
                }

              complete(
                HttpEntity(
                  ContentTypes.`text/plain(UTF-8)`,
                  source
                )
              )
            }
          }
        }
      )

    val bindingFuture =
      Http().newServerAt("localhost", 8080).bind(route)

    println(
      s"Server now online. Please navigate to http://localhost:8080/hello\nPress RETURN to stop..."
    )
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }

  def subscribe(
      redisClient: RedisClient,
      stream: String,
      resuestId: String
  ) = {
    val connection: StatefulRedisConnection[String, String] = redisClient.connect()
    val asyncCommands: RedisAsyncCommands[String, String] = connection.async()

    println(s"$resuestId subscribe")
    // ストリームからメッセージを読み取る
    Source
      .unfoldAsync("$") { lastId =>
        println(s"${new Date()} $resuestId xread")
        asyncCommands
          .xread(
            XReadArgs().block(1_000),
            XReadArgs.StreamOffset.from(stream, lastId)
          )
          .asScala
          .map { messages =>
            val messagesScala = messages.asScala
            val nextId = messagesScala.lastOption.map(_.getId()).getOrElse(lastId)
            Some((nextId, messagesScala))
          }
      }
      .mapConcat(identity)
  }

  def subscribe2(
      redisClient: RedisClient,
      stream: String,
      requestId: String
  ) = {
    val connection: StatefulRedisConnection[String, String] = redisClient.connect()
    val asyncCommands: RedisAsyncCommands[String, String] = connection.async()

    println(s"$requestId subscribe")
    // ストリームからメッセージを読み取る
    var lastId = "$"
    Source
      .unfoldResourceAsync(
        create = () => Future.successful(redisClient.connect()),
        read = connection => {
          println(s"xread pre $requestId: $lastId")
          asyncCommands
            .xread(
              XReadArgs().block(1_000),
              XReadArgs.StreamOffset.from(stream, lastId)
            )
            .asScala
            .map { messages =>
              val messagesScala = messages.asScala
              val nextId = messagesScala.lastOption.map(_.getId()).getOrElse(lastId)
              lastId = nextId
              Some(messagesScala)
            }
        },
        close = (connection) =>
          connection.closeAsync().asScala.map { _ =>
            println(s"close $requestId")
            Done
          }
      )
      .mapConcat(identity)
  }
}
