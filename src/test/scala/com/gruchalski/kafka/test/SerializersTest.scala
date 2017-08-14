/*
 * Copyright 2017 Radek Gruchalski
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gruchalski.kafka.test

import java.nio.charset.StandardCharsets

import com.gruchalski.kafka.scala._
import org.apache.kafka.clients.producer.RecordMetadata
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Inside, Matchers, WordSpec}

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
          1
        ).toVersionCompatibleEither match {
            case Right(f) ⇒
              import scala.concurrent.duration._
              val result = Await.result(f, 1 second)
              result shouldBe a[RecordMetadata]
            case Left(error) ⇒
              fail(error)
          }
        eventually {
          cluster.consume[String, Int](
            clusterConfig.`com.gruchalski.kafka.topics`.head.get.name
          ).toVersionCompatibleEither should matchPattern {
            case Right(Some(ConsumedItem(Some("key-string"), 1, _))) ⇒
          }
        }
      }

      "given int as a key and a double as value" in {
        cluster.produce(
          clusterConfig.`com.gruchalski.kafka.topics`.head.get.name,
          Some(1),
          1D
        ).toVersionCompatibleEither match {
            case Right(f) ⇒
              import scala.concurrent.duration._
              val result = Await.result(f, 1 second)
              result shouldBe a[RecordMetadata]
            case Left(error) ⇒
              fail(error)
          }
        eventually {
          cluster.consume[Int, Double](
            clusterConfig.`com.gruchalski.kafka.topics`.head.get.name
          ).toVersionCompatibleEither should matchPattern {
            case Right(Some(ConsumedItem(Some(1), 1D, _))) ⇒
          }
        }
      }

      "given long as a key and a byte array as value" in {
        val bytes = "some-byte-array".getBytes(StandardCharsets.UTF_8)
        cluster.produce(
          clusterConfig.`com.gruchalski.kafka.topics`.head.get.name,
          Some(100L),
          bytes
        ).toVersionCompatibleEither match {
            case Right(f) ⇒
              import scala.concurrent.duration._
              val result = Await.result(f, 1 second)
              result shouldBe a[RecordMetadata]
            case Left(error) ⇒
              fail(error)
          }
        eventually {
          cluster.consume[Long, Array[Byte]](
            clusterConfig.`com.gruchalski.kafka.topics`.head.get.name
          ).toVersionCompatibleEither should matchPattern {
            case Right(Some(ConsumedItem(Some(100L), bytes, _))) ⇒
          }
        }
      }

      "given string as a key and a string as value" in {
        cluster.produce(
          clusterConfig.`com.gruchalski.kafka.topics`.head.get.name,
          Some("a key"),
          "a value"
        ).toVersionCompatibleEither match {
            case Right(f) ⇒
              import scala.concurrent.duration._
              val result = Await.result(f, 1 second)
              result shouldBe a[RecordMetadata]
            case Left(error) ⇒
              fail(error)
          }
        eventually {
          cluster.consume[String, String](
            clusterConfig.`com.gruchalski.kafka.topics`.head.get.name
          ).toVersionCompatibleEither should matchPattern {
            case Right(Some(ConsumedItem(Some("a key"), "a value", _))) ⇒
          }
        }
      }

    }
  }

}
