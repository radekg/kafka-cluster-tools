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

package com.gruchalski.kafka

import com.gruchalski.kafka.scala.{ConsumedItem, KafkaCluster, KafkaTopicCreateResult, KafkaTopicStatus}
import com.gruchalski.kafka.serializer.scala.TestConcreteProvider
import org.apache.kafka.clients.producer.RecordMetadata
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Milliseconds, Second, Seconds, Span}
import org.scalatest.{Inside, Matchers, WordSpec}

import concurrent.{ExecutionContext, Future}
import util.{Failure, Success}

class KafkaClusterTest extends WordSpec with Matchers with Eventually with Inside {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(100, Milliseconds)))

  implicit val ec: ExecutionContext = _root_.scala.concurrent.ExecutionContext.Implicits.global

  "The MessagePack serialization" must {
    "serialize and deserialize" when {
      "given an inplementation of serializable" in {

        val inst = TestConcreteProvider.ConcreteExample(property = "unit test data")
        val bytes = inst.serializer().serialize("topic-name", inst)
        inst.deserializer().deserialize("topic-name", bytes) shouldBe inst

      }
    }
  }

  "Kafka cluster" must {
    "start, create topics and stop" when {

      "requested to start" in {
        val cluster = KafkaCluster()
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
          case None ⇒ fail("Expected Kafka cluster to come up.")
        }
        cluster.stop()
      }

      "produce and consume messages from a topic" in {
        val cluster = KafkaCluster()
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

            val topicToUse = safe.configuration.`com.gruchalski.kafka.topics`.head.get.name
            val concreteToUse = TestConcreteProvider.ConcreteExample(property = "full kafka publish / consume test")

            // should be able to publish:
            var receivedMetadata: Option[RecordMetadata] = None
            safe.cluster.produce(topicToUse, concreteToUse).foreach { f ⇒
              f.onComplete {
                case Success(metadata) ⇒ receivedMetadata = Some(metadata)
                case Failure(ex)       ⇒ fail(ex)
              }
            }
            eventually {
              receivedMetadata should matchPattern { case Some(_) ⇒ }
            }

            // should be able to consume the published message:
            implicit val concreteDeserializer = concreteToUse.deserializer()
            eventually {
              val consumed = safe.cluster.consume[TestConcreteProvider.ConcreteExample](topicToUse)
              consumed should matchPattern { case Some(ConsumedItem(concreteToUse, _)) ⇒ }
            }

          case None ⇒ fail("Expected Kafka cluster to come up.")
        }
        cluster.stop()
      }

    }
  }

}
