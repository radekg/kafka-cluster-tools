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

import java.io.File

import com.gruchalski.kafka.scala.{Configuration, KafkaCluster, KafkaTopicCreateResult, KafkaTopicStatus}
import com.gruchalski.kafka.test.serializer.scala.TestConcreteProvider
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Inside, Matchers, WordSpec}

import scala.concurrent.Future
import scala.util.{Failure, Success}

class KafkaInvalidConfigurationTest extends WordSpec with Matchers with Eventually with Inside with BeforeAndAfterAll {

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(100, Milliseconds)))

  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global
  implicit val config: Config = ConfigFactory.parseFile(new File(getClass.getClassLoader.getResource("invalid.conf").toURI))
  val cluster = KafkaCluster()
  val clusterConfig = new Configuration(config);

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
      case None ⇒ fail("Expected Kafka cluster to come up.")
    }
  }

  override def afterAll(): Unit = {
    cluster.stop()
  }

  "Kafka cluster" must {

    "gracefully handle errors" when {

      "invalid Kafka producer configuration is given" in {
        import com.gruchalski.kafka.test.serializer.scala.TestConcreteSerdes._
        cluster.produce(
          clusterConfig.`com.gruchalski.kafka.topics`.head.get.name,
          TestConcreteProvider.ConcreteExample()
        ).toVersionCompatibleEither should matchPattern { case Left(_) ⇒ }
      }

      "invalid Kafka consumer configuration is given" in {
        import com.gruchalski.kafka.test.serializer.scala.TestConcreteSerdes._
        cluster.consume[TestConcreteProvider.ConcreteExample](
          clusterConfig.`com.gruchalski.kafka.topics`.head.get.name
        ).toVersionCompatibleEither should matchPattern { case Left(_) ⇒ }
      }

    }

  }

}
