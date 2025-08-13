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

object HttpServerRoutingMinimal {

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem(Behaviors.empty, "my-system")
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.executionContext

    val route =
      path("hello") {
        get {
          val numbers =
            Source.fromIterator(() => Iterator.continually(Random.nextInt()))

          complete(
            HttpEntity(
              ContentTypes.`text/plain(UTF-8)`,
              numbers.map(n => ByteString(s"$n\n"))
            )
          )
        }

      }

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
}


object RedisStreamsExample {
  def main(args: Array[String]): Unit = {
    val redisClient = RedisClient.create("redis://localhost:6379")
    val connection: StatefulRedisConnection[String, String] = redisClient.connect()
    val syncCommands: RedisCommands[String, String] = connection.sync()

    // ストリームにメッセージを追加
    val messageId = syncCommands.xadd("mystream", "name", "John", "age", "30")
    println(s"Message added with ID: $messageId")

    // ストリームからメッセージを読み取る
    val messages = syncCommands.xread(1, 1000, "mystream")
    messages.forEach { message =>
      println(s"Message ID: ${message.getId}, Values: ${message.getValues}")
    }

    connection.close()
    redisClient.shutdown()
  }
}
