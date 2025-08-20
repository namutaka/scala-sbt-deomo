package com.typeduke.helloworld

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
import scala.jdk.FutureConverters._
import concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import akka.stream.Materializer
import akka.Done
import io.lettuce.core.StreamMessage
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import scala.jdk.CollectionConverters._
import collection.convert.ImplicitConversions._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.Date
import java.util.UUID
import org.slf4j.LoggerFactory

class RedisStreamReadService {

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
    println(s"$requestId subscribe")
    // ストリームからメッセージを読み取る
    var lastId = "$"
    Source
      .unfoldResourceAsync(
        create = () => Future.successful(redisClient.connect()),
        read = connection => {
          println(s"xread pre $requestId: $lastId")
          val asyncCommands: RedisAsyncCommands[String, String] = connection.async()
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

  val logger = LoggerFactory.getLogger(this.getClass())
  import RedisStreamReadService._
  val subscribeQueue = java.util.concurrent.LinkedBlockingQueue[ReceiverTask]()
  val receivers = scala.collection.mutable.HashMap[String, StreamReceiver]()
  val active = AtomicBoolean(true)

  val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

  def subscribe(streamKey: String, receiver: Receiver): SubscribeItem = synchronized {
    val id = UUID.randomUUID().toString()

    receivers.updateWith(streamKey) {
      case Some(prev) =>
        println(s"sub add : $streamKey")
        prev.receivers.addOne(id -> receiver)
        Some(prev)

      case None =>
        println(s"sub new : $streamKey")
        subscribeQueue.offer(ReceiverTask(streamKey, "$"))
        Some(StreamReceiver(streamKey, scala.collection.mutable.Map(id -> receiver)))
    }

    SubscribeItem(streamKey, id)
  }

  def unsubscribe(item: SubscribeItem) = {
    receivers.updateWith(item.streamKey) {
      case Some(prev) =>
        prev.receivers.remove(item.id)
        Option.when(prev.receivers.nonEmpty)(prev)
      case None => None
    }
  }

  def stop() = {
    active.set(false)
  }

  def start(
      redisClient: RedisClient
  ) = {
    Future {
      val connection: StatefulRedisConnection[String, String] = redisClient.connect()
      val syncCommands: RedisCommands[String, String] = connection.sync()

      try {
        while (active.get()) {
          val tasks = {
            val tasks = Seq.newBuilder[ReceiverTask]
            println("get task")
            tasks.addOne(subscribeQueue.take())
            while (tasks.knownSize < 5 && !subscribeQueue.isEmpty()) {
              tasks.addOne(subscribeQueue.poll())
            }
            println("get task: END")
            tasks.result()
          }

          val streams = tasks.map { task =>
            XReadArgs.StreamOffset.from(task.streamKey, task.lastId)
          }

          println(s"xread : $streams")
          val messages =
            syncCommands.xread(XReadArgs().block(10_000), streams: _*).asScala

          val newIdMap = {
            val newIdMap = Map.newBuilder[String, String]
            messages.foreach { message =>
              val streamKey = message.getStream()
              receivers.get(streamKey).foreach { receiver =>
                receiver.receivers.foreach { (_, receiver) => receiver(message) }
              }

              newIdMap.addOne(streamKey -> message.getId())
            }
            newIdMap.result()
          }

          val renewTasks =
            tasks
              .filter { task =>
                receivers.contains(task.streamKey)
              }
              .map { task =>
                newIdMap.get(task.streamKey) match {
                  case Some(lastId) => task.copy(lastId = lastId)
                  case None         => task
                }
              }

          subscribeQueue.addAll(renewTasks.toSeq)
          println("recievers: " + subscribeQueue.map { r => r.log }.mkString(" , "))
        }
      } catch {
        case e: Throwable =>
          logger.error(s"error: $e", e)

      } finally {
        connection.close()
      }
      println("finish")
    }(executionContext)
  }
}

object RedisStreamReadService {
  type ID = String
  type MessageData = StreamMessage[String, String]

  type Receiver = (MessageData) => Unit

  case class SubscribeItem(streamKey: String, id: ID)

  case class ReceiverTask(streamKey: String, lastId: String) {
    def log = s"$streamKey:$lastId"
  }

  case class StreamReceiver(
      streamKey: String,
      receivers: scala.collection.mutable.Map[ID, Receiver]
  )
}
