package com.gruchalski.kafka.test

import java.nio.charset.StandardCharsets

import com.gruchalski.kafka.scala._
import org.apache.kafka.clients.producer.RecordMetadata
import org.scalatest.{BeforeAndAfterAll, Inside, Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, Waiters}
import org.scalatest.time.{Milliseconds, Seconds, Span}

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

class SerializersTest extends WordSpec with Matchers with Eventually with Inside with BeforeAndAfterAll {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(100, Milliseconds)))

  // import default serdes:
  import com.gruchalski.kafka.scala.DefaultSerdes._

  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global
  val cluster = KafkaCluster()
  var clusterConfig: Configuration = _

  override def beforeAll(): Unit = {
    cluster.start() match {
      case Some(safe) ⇒
        val topics = safe.configuration.`com.gruchalski.kafka.topics`.flatten
        @volatile var topicCreateStatuses = List.empty[KafkaTopicCreateResult]
        Future.sequence(safe.cluster.withTopics(topics)).onComplete {
          case Success(statuses) ⇒
            topicCreateStatuses = statuses
          case Failure(ex) ⇒
            fail(ex)
        }
        eventually {
          topicCreateStatuses should matchPattern {
            case List(
            KafkaTopicCreateResult(_, KafkaTopicStatus.Exists(), None),
            KafkaTopicCreateResult(_, KafkaTopicStatus.Exists(), None)) ⇒
          }
        }
        clusterConfig = safe.configuration
      case None ⇒ fail("Expected Kafka cluster to come up.")
    }
  }

  override def afterAll(): Unit = {
    cluster.stop()
  }

  "Kafka cluster serialization" must {
    "serialize and deserialize" when {

      "given string as key and an int as value" in {
        cluster.produce(
          clusterConfig.`com.gruchalski.kafka.topics`.head.get.name,
          Some("key-string"),
          1).toEither match {
          case Right(f) ⇒
            import scala.concurrent.duration._
            val result = Await.result(f, 1 second)
            result shouldBe a [RecordMetadata]
          case Left(error) ⇒
            fail(error)
        }
        eventually {
          cluster.consume[String, Int](
            clusterConfig.`com.gruchalski.kafka.topics`.head.get.name
          ).toEither should matchPattern {
            case Right(Some(ConsumedItem(Some("key-string"), 1, _))) ⇒
          }
        }
      }

      "given int as a key and a double as value" in {
        cluster.produce(
          clusterConfig.`com.gruchalski.kafka.topics`.head.get.name,
          Some(1),
          1D).toEither match {
          case Right(f) ⇒
            import scala.concurrent.duration._
            val result = Await.result(f, 1 second)
            result shouldBe a [RecordMetadata]
          case Left(error) ⇒
            fail(error)
        }
        eventually {
          cluster.consume[Int, Double](
            clusterConfig.`com.gruchalski.kafka.topics`.head.get.name
          ).toEither should matchPattern {
            case Right(Some(ConsumedItem(Some(1), 1D, _))) ⇒
          }
        }
      }

      "given long as a key and a byte array as value" in {
        val bytes = "some-byte-array".getBytes(StandardCharsets.UTF_8)
        cluster.produce(
          clusterConfig.`com.gruchalski.kafka.topics`.head.get.name,
          Some(100L),
          bytes).toEither match {
          case Right(f) ⇒
            import scala.concurrent.duration._
            val result = Await.result(f, 1 second)
            result shouldBe a [RecordMetadata]
          case Left(error) ⇒
            fail(error)
        }
        eventually {
          cluster.consume[Long, Array[Byte]](
            clusterConfig.`com.gruchalski.kafka.topics`.head.get.name
          ).toEither should matchPattern {
            case Right(Some(ConsumedItem(Some(100L), bytes, _))) ⇒
          }
        }
      }

    }
  }

}
