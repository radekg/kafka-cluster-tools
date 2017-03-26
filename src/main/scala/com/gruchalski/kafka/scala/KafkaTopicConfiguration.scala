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

package com.gruchalski.kafka.scala

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import kafka.admin.RackAwareMode
import org.apache.kafka.common.protocol.SecurityProtocol

import scala.util.Try

/**
 * Kafka topic configuration companion object.
 */
object KafkaTopicConfiguration {

  val defaultRackAwareMode = RackAwareMode.Enforced

  def apply(name: String, config: Config) = {
    import com.gruchalski.typesafe.config.ConfigImplicits._
    val partitions = Try { config.getInt("partitions") }.getOrElse(1)
    val replicationFactor = Try { config.getInt("replicationFactor") }.getOrElse(1)
    val topicConfig = (Try { config.getConfig("topicConfig") }.getOrElse(ConfigFactory.empty())).toProperties()
    val rackAwareMode = Try {
      toRackAwareMode(
        config.getString("rackAwareMode")
      ).getOrElse(defaultRackAwareMode)
    }.getOrElse(defaultRackAwareMode)
    new KafkaTopicConfiguration(name, partitions, replicationFactor, topicConfig, rackAwareMode)
  }

  def apply(
    name: String,
    partitions: Int = 1,
    replicationFactor: Int = 1,
    topicConfig: Properties = new Properties(),
    rackAwareMode: RackAwareMode = defaultRackAwareMode
  ) = {
    new KafkaTopicConfiguration(name, partitions, replicationFactor, topicConfig, rackAwareMode)
  }

  def toRackAwareMode(value: String): Option[RackAwareMode] = {
    value match {
      case "enforced" ⇒ Some(defaultRackAwareMode)
      case "disabled" ⇒ Some(RackAwareMode.Disabled)
      case "safe"     ⇒ Some(RackAwareMode.Safe)
      case _          ⇒ None
    }
  }

  def toSecurityProtocol(value: String): SecurityProtocol = {
    value match {
      case "plaintext"      ⇒ SecurityProtocol.PLAINTEXT
      case "sasl_plaintext" ⇒ SecurityProtocol.SASL_PLAINTEXT
      case "sasl_ssl"       ⇒ SecurityProtocol.SASL_SSL
      case "ssl"            ⇒ SecurityProtocol.SSL
      case "trace"          ⇒ SecurityProtocol.TRACE
      case _                ⇒ SecurityProtocol.PLAINTEXT
    }
  }

}

/**
 * Kafka topic configuration.
 * @param name topic name
 * @param partitions number of partitions
 * @param replicationFactor replication factor
 * @param topicConfig topic configuration properties
 * @param rackAwareMode rack awareness mode
 */
class KafkaTopicConfiguration(
  val name: String,
  val partitions: Int,
  val replicationFactor: Int,
  val topicConfig: Properties,
  val rackAwareMode: RackAwareMode
)

/**
 * Kafka topic existence status.
 */
object KafkaTopicStatus {
  sealed abstract class Status()

  /**
   * Topic exists.
   */
  case class Exists() extends Status {
    override def toString: String = {
      com.gruchalski.kafka.java8.KafkaTopicStatus.Exists
    }
  }

  /**
   * Topic does not exist.
   */
  case class DoesNotExist() extends Status {
    override def toString: String = {
      com.gruchalski.kafka.java8.KafkaTopicStatus.DoesNotExist
    }
  }
}

/**
 * Reports a status of topic create request.
 * @param topicConfig topic configuration
 * @param status topic creation status
 * @param error in case of an error, contains the underlying error
 */
case class KafkaTopicCreateResult(
  topicConfig: KafkaTopicConfiguration,
  status: KafkaTopicStatus.Status,
  error: Option[Throwable] = None
)
