/*
 * Copyright 2017 Rad Gruchalski
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

import com.gruchalski.typesafe.config.ConfigImplicits._
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

/**
 * Configuration companion object.
 */
object Configuration {
  def apply(underlying: Config) = new Configuration(underlying)
}

/**
 * Kafka cluster configuration.
 * @param underlying Typesafe configuration object
 */
class Configuration(val underlying: Config) {

  /**
   * ZooKeeeper ensemble size.
   */
  lazy val `com.gruchalski.zookeeper.server.ensemble-size` =
    Try { underlying.getInt("com.gruchalski.zookeeper.server.ensemble-size") }
      .getOrElse(1)

  /**
   * Number of Kafka brokers to start.
   */
  lazy val `com.gruchalski.kafka.cluster.size` =
    Try { underlying.getInt("com.gruchalski.kafka.cluster-size") }
      .getOrElse(1)

  lazy val `com.gruchalski.kafka.broker.enable-controlled-shutdown` =
    Try { underlying.getBoolean("com.gruchalski.kafka.broker.enable-controlled-shutdown") }
      .getOrElse(true)

  lazy val `com.gruchalski.kafka.broker.enable-delete-topic` =
    Try { underlying.getBoolean("com.gruchalski.kafka.broker.enable-delete-topic") }
      .getOrElse(false)

  lazy val `com.gruchalski.kafka.broker.enable-plaintext` =
    Try { underlying.getBoolean("com.gruchalski.kafka.broker.enable-plaintext") }
      .getOrElse(true)

  lazy val `com.gruchalski.kafka.broker.enable-ssl` =
    Try { underlying.getBoolean("com.gruchalski.kafka.broker.enable-ssl") }
      .getOrElse(false)

  lazy val `com.gruchalski.kafka.broker.enable-sasl-plaintext` =
    Try { underlying.getBoolean("com.gruchalski.kafka.broker.enable-sasl-plaintext") }
      .getOrElse(false)

  lazy val `com.gruchalski.kafka.broker.enable-sasl-ssl` =
    Try { underlying.getBoolean("com.gruchalski.kafka.broker.enable-sasl-ssl") }
      .getOrElse(false)

  lazy val `com.gruchalski.kafka.broker.rack-info` =
    Try { Some(underlying.getString("com.gruchalski.kafka.broker.rack-info")) }
      .getOrElse(None)

  lazy val `com.gruchalski.kafka.topic-wait-for-create-success-timeout-ms` =
    Try { underlying.getLong("com.gruchalski.kafka.topic-wait-for-create-success-timeout-ms") }
      .getOrElse(1000L)

  lazy val `com.gruchalski.kafka.producer.acks` =
    Try { underlying.getInt("com.gruchalski.kafka.producer.acks") }
      .getOrElse(-1)

  lazy val `com.gruchalski.kafka.producer.max-block-ms` =
    Try { underlying.getLong("com.gruchalski.kafka.producer.max-block-ms") }
      .getOrElse(60L * 1000L)

  lazy val `com.gruchalski.kafka.producer.buffer-size` =
    Try { underlying.getLong("com.gruchalski.kafka.producer.buffer-size") }
      .getOrElse(1024L * 1024L)

  lazy val `com.gruchalski.kafka.producer.retries` =
    Try { underlying.getInt("com.gruchalski.kafka.producer.retries") }
      .getOrElse(0)

  lazy val `com.gruchalski.kafka.producer.linger-ms` =
    Try { underlying.getLong("com.gruchalski.kafka.producer.linger-ms") }
      .getOrElse(0L)

  lazy val `com.gruchalski.kafka.producer.request-timeout-ms` =
    Try { underlying.getLong("com.gruchalski.kafka.producer.request-timeout-ms") }
      .getOrElse(10L * 1024L)

  lazy val `com.gruchalski.kafka.producer.security-protocol` =
    KafkaTopicConfiguration.toSecurityProtocol(
      Try { underlying.getString("com.gruchalski.kafka.producer.security-protocol") }
        .getOrElse("plaintext")
    )

  lazy val `com.gruchalski.kafka.producer.props` =
    Try { underlying.getConfig("com.gruchalski.kafka.producer.props") }
      .getOrElse(ConfigFactory.empty()).toProperties()

  lazy val `com.gruchalski.kafka.consumer.auto-offset-reset` =
    Try { underlying.getString("com.gruchalski.kafka.consumer.auto-offset-reset") }
      .getOrElse("earliest")

  lazy val `com.gruchalski.kafka.consumer.group-id` =
    Try { underlying.getString("com.gruchalski.kafka.consumer.group-id") }
      .getOrElse("group")

  lazy val `com.gruchalski.kafka.producer.partition-fetch-size` =
    Try { underlying.getLong("com.gruchalski.kafka.producer.partition-fetch-size") }
      .getOrElse(4096L)

  lazy val `com.gruchalski.kafka.producer.session-timeout` =
    Try { underlying.getInt("com.gruchalski.kafka.producer.session-timeout") }
      .getOrElse(30000)

  lazy val `com.gruchalski.kafka.consumer.security-protocol` =
    KafkaTopicConfiguration.toSecurityProtocol(
      Try { underlying.getString("com.gruchalski.kafka.consumer.security-protocol") }
        .getOrElse("plaintext")
    )

  lazy val `com.gruchalski.kafka.consumer.props` =
    Try { underlying.getConfig("com.gruchalski.kafka.consumer.props") }
      .getOrElse(ConfigFactory.empty()).toProperties()

  /**
   * Optional list
   */
  lazy val `com.gruchalski.kafka.topics` = {
    import com.gruchalski.utils.StringImplicits.StringExtensions

    import scala.collection.JavaConverters._
    val topicConfigs = Try { underlying.getConfigList("com.gruchalski.kafka.topics").asScala.toList }
      .getOrElse(List.empty[Config])
    topicConfigs.map { topicConfig ⇒
      Try { Some(topicConfig.getString("name")) }.getOrElse(None) match {
        case Some(name) ⇒
          Some(KafkaTopicConfiguration(name.unquoted(), topicConfig))
        case None ⇒
          None
      }
    }
  }

}
