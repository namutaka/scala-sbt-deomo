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

object RedisStreamsExample {
  def subscribe(redisClient: RedisClient)(implicit mat: Materializer) = {
    val connection: StatefulRedisConnection[String, String] = redisClient.connect()
    val syncCommands: RedisAsyncCommands[String, String] = connection.async()

    println(s"111")
    // ストリームからメッセージを読み取る
    Source
      .tick(
        1.second, // delay of first tick
        1.second, // delay of subsequent ticks
        "tick" // element emitted each tick
      )
      .mapAsync(1) { _ =>
        println(s"xread")
        syncCommands
          .xread(
            XReadArgs().count(10),
            XReadArgs.StreamOffset.lastConsumed("mystream")
          )
          .asScala
      }
      .map { messages =>
        messages.asScala.map { message =>
          println(s"Message ID: ${message.getId}, Values: ${message.toString()}")
        }
      }
      .runWith(
        Sink.ignore
      )
  }

  def subscribe2(redisClient: RedisClient)(implicit mat: Materializer) = {
    val connection: StatefulRedisConnection[String, String] = redisClient.connect()
    val syncCommands: RedisAsyncCommands[String, String] = connection.async()

    println(s"111")
    // ストリームからメッセージを読み取る
    Source
      .unfoldAsync("$") { lastId =>
        println(s"xread")
        syncCommands
          .xread(
            XReadArgs().block(0),
            XReadArgs.StreamOffset.from("mystream", lastId)
          )
          .asScala
          .map { messages =>
            val messagesScala = messages.asScala
            val nextId = messagesScala.lastOption.map(_.getId()).getOrElse(lastId)
            Some((nextId, messagesScala))
          }
      }
      .map { messages =>
        messages.map { message =>
          println(s"Message ID: ${message.getId}, Values: ${message.toString()}")
        }
      }
      .runWith(
        Sink.ignore
      )
  }

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem(Behaviors.empty, "my-system")

    val redisClient = RedisClient.create("redis://redis:6379")

    println(s"2212")

    val stream = "stream:1"

    val getF = {
      val connection: StatefulRedisConnection[String, String] = redisClient.connect()
      val syncCommands: RedisAsyncCommands[String, String] = connection.async()

      println(s"333")
      Source
        .tick(
          2.second, // delay of first tick
          2.second, // delay of subsequent ticks
          "tick" // element emitted each tick
        )
        .mapAsync(1) { e =>
          println(s"xadd")
          // ストリームにメッセージを追加
          Future.traverse(1 to Random.nextInt(10)) { i =>
            syncCommands
              .xadd(
                stream,
                Map("name" -> e, "age" -> Random.nextInt(100).toString, "id" -> i.toString).asJava
              )
              .asScala
          }
        }
        .map { messageId =>
          println(s"Message added with ID: $messageId")
        }
        .runWith(
          Sink.ignore
        )
        .recover { case e: Throwable =>
          println(e.toString())
          throw e
        }
    }

    println(s"444")
    // Await.result(subscribe2, Duration.Inf)
    Await.result(getF, Duration.Inf)

    redisClient.shutdown()
    system.terminate()
  }
}
