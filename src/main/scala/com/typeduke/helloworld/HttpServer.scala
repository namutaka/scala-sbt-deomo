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
import akka.Done
import com.typeduke.helloworld.RedisStreamReadService
import java.util.concurrent.LinkedBlockingQueue
import scala.jdk.CollectionConverters._
import scala.util.Failure
import akka.actor.Status
import akka.stream.CompletionStrategy
import org.slf4j.LoggerFactory
import scala.util.Success
import io.lettuce.core.BitFieldArgs.OverflowType
import akka.stream.OverflowStrategy
import com.typeduke.helloworld.RedisStreamReadService.MessageData

object HttpServerRoutingMinimal {
  val logger = LoggerFactory.getLogger(this.getClass())

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem(Behaviors.empty, "my-system")
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.executionContext

    val redisClient = RedisClient.create("redis://redis:6379")

    val service = new RedisStreamReadService()
    service.start(redisClient)

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
              logger.info(s"request $requestId")

              val source = service
                .subscribe2(redisClient, "stream:" + user, requestId)
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
        },
        path("sse2") {
          get {
            parameters("user") { (user) =>
              val streamKey = "stream:" + user

              val requestId = Random.nextInt(100).toString
              println(s"request $streamKey - $requestId")

              val source =
                Source
                  // .actorRefWithBackpressure[MessageData](
                  //   ackMessage = "ack",
                  //   // complete when we send akka.actor.status.Success
                  //   completionMatcher = { case _: Status.Success =>
                  //     CompletionStrategy.immediately
                  //   },
                  //   // do not fail on any message
                  //   failureMatcher = PartialFunction.empty
                  // )
                  .actorRef[MessageData](32, OverflowStrategy.dropHead)
                  .watchTermination() { (actorRef, future) =>
                    val subscribeItem =
                      service.subscribe(streamKey, (msg) => actorRef ! msg)

                    future.onComplete { res =>
                      logger.info("terminate: " + res.toString())
                      service.unsubscribe(subscribeItem)
                      res match {
                        case Failure(exception) =>
                          logger.error("terminate: " + res.toString(), exception)
                        case Success(_) =>
                          ()
                      }
                    }
                    actorRef
                  }
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

    service.stop()

    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }

}
