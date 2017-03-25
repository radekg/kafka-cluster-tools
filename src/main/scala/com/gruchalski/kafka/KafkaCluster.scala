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

import java.util.concurrent.{ExecutorService, Executors}

import com.typesafe.config.{Config, ConfigFactory}
import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, Deserializer}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Representation of a successfully started cluster. A cluster instance wrapped in this object is safe to use
 * in the program. Returned by [[KafkaCluster.start()]].
 * @param cluster a safe to use cluster
 * @param configuration configuration used by the cluster
 */
case class KafkaClusterSafe(cluster: KafkaCluster, configuration: Configuration)

class KafkaCluster()(implicit config: Config, ec: ExecutionContext) {

  private val configuration = Configuration(config)
  private val zooKeeper = EmbeddedZooKeeper(configuration)
  private var kafkaServers = List.empty[KafkaServer]
  private var executor: Option[ExecutorService] = None

  private var producers = scala.collection.mutable.HashMap.empty[Class[_], KafkaProducer[_, _]]
  private var consumers = scala.collection.mutable.HashMap.empty[Class[_], KafkaConsumer[_, _]]

  /**
   * Start the cluster. Method is idempotent, subsequent calls return currently running cluster, if cluster is
   * running.<br/>
   * The method will start a ZooKeeper ensemble to be used by the Kafka cluster.
   * @return safe to use cluster data
   */
  def start(): Option[KafkaClusterSafe] = {
    if (kafkaServers.length == 0) {
      zooKeeper.start() match {
        case Some(EmbeddedZooKeeper.EmbeddedZooKeeperData(instances, connectionString)) ⇒
          kafkaServers = TestUtils.createBrokerConfigs(
            configuration.`com.gruchalski.kafka.cluster.size`,
            connectionString,
            enableControlledShutdown = configuration.`com.gruchalski.kafka.broker.enable-controlled-shutdown`,
            enableDeleteTopic = configuration.`com.gruchalski.kafka.broker.enable-delete-topic`,
            enablePlaintext = configuration.`com.gruchalski.kafka.broker.enable-plaintext`,
            enableSsl = configuration.`com.gruchalski.kafka.broker.enable-ssl`,
            enableSaslPlaintext = configuration.`com.gruchalski.kafka.broker.enable-sasl-plaintext`,
            enableSaslSsl = configuration.`com.gruchalski.kafka.broker.enable-sasl-ssl`,
            rackInfo = (1 to configuration.`com.gruchalski.kafka.cluster.size`)
              .foldLeft(Map.empty[Int, String]) { (accum, index) ⇒
                configuration.`com.gruchalski.kafka.broker.rack-info` match {
                  case Some(rackInfo) ⇒
                    accum ++ Map(index → rackInfo)
                  case None ⇒
                    accum
                }
              }
          ).map(new KafkaConfig(_, true)).map(new KafkaServer(_, new MockTime())).map { kafka ⇒
              Try {
                kafka.startup()
                Some(kafka)
              }.getOrElse(None)
            }.toList.flatten
          if (kafkaServers.length != configuration.`com.gruchalski.kafka.cluster.size`) {
            // not all servers started, we shouldn't continue:
            stop()
          }
          executor = Some(Executors.newSingleThreadExecutor())
          Some(KafkaClusterSafe(this, configuration))
        case None ⇒
          None
      }
    } else {
      Some(KafkaClusterSafe(this, configuration))
    }
  }

  /**
   * Stop the cluster, if running.
   */
  def stop(): Unit = {
    executor.foreach(_.shutdownNow())
    kafkaServers.foreach(_.shutdown())
    zooKeeper.stop()
  }

  /**
   * Create topics and wait for the topics to be ready to use. For every topic requestd, if the topic is not found
   * in Kafka within the <code>com.gruchalski.kafka.topic-wait-for-create-success-timeout-ms</code> timeout,
   * the topic will be considered as not created.
   * @param topicConfigs topic configurations
   * @return list of create statuses futures
   */
  def withTopics(topicConfigs: List[KafkaTopicConfiguration]): List[Future[KafkaTopicCreateResult]] = {
    kafkaServers.headOption match {
      case Some(kafkaServer) ⇒
        topicConfigs.map { topicConfig ⇒

          Try {
            AdminUtils.createTopic(
              kafkaServer.zkUtils,
              topicConfig.name,
              topicConfig.partitions,
              topicConfig.replicationFactor,
              topicConfig.topicConfig,
              topicConfig.rackAwareMode
            )
            Future {
              val start = System.currentTimeMillis()
              var status: KafkaTopicStatus.Status = KafkaTopicStatus.DoesNotExist
              var timeout = false
              while (status == KafkaTopicStatus.DoesNotExist || !timeout) {
                Thread.sleep(50)
                if (AdminUtils.topicExists(kafkaServer.zkUtils, topicConfig.name)) {
                  status = KafkaTopicStatus.Exists
                }
                timeout = System.currentTimeMillis() - start >= configuration.`com.gruchalski.kafka.topic-wait-for-create-success-timeout-ms`
              }
              KafkaTopicCreateResult(topicConfig, status)
            }
          }.toEither match {
            case Left(error) ⇒
              Future(KafkaTopicCreateResult(topicConfig, KafkaTopicStatus.DoesNotExist, Some(error)))
            case Right(result) ⇒
              result
          }

        }
      case None ⇒
        List.empty[Future[KafkaTopicCreateResult]]
    }
  }

  /**
   * Produce a message of a given type. If the producer for the given type does not exist, it will be created.
   * @param topic topic to send the messge to
   * @param value value to send
   * @param callback callback handling metadata or error, the callback is used to return a scala future
   * @tparam T type of the value to send
   * @return the metadata / error future
   */
  def produce[T](topic: String, value: SerializerProvider[T], callback: ProducerCallback = ProducerCallback()): Option[Future[RecordMetadata]] = {
    produce(topic, None, value, callback)
  }

  def produce[T](topic: String, key: Option[Array[Byte]], value: SerializerProvider[T], callback: ProducerCallback): Option[Future[RecordMetadata]] = {
    kafkaServers.headOption match {
      case Some(kafkaServer) ⇒
        if (!producers.contains(value.serializer().getClass)) {
          producers.put(
            value.serializer().getClass,
            TestUtils.createNewProducer[Array[Byte], T](
              brokerList = bootstrapServers().mkString(","),
              acks = configuration.`com.gruchalski.kafka.producer.acks`,
              maxBlockMs = configuration.`com.gruchalski.kafka.producer.max-block-ms`,
              bufferSize = configuration.`com.gruchalski.kafka.producer.buffer-size`,
              retries = configuration.`com.gruchalski.kafka.producer.retries`,
              lingerMs = configuration.`com.gruchalski.kafka.producer.linger-ms`,
              requestTimeoutMs = configuration.`com.gruchalski.kafka.producer.request-timeout-ms`,
              securityProtocol = configuration.`com.gruchalski.kafka.producer.security-protocol`,
              props = Some(configuration.`com.gruchalski.kafka.producer.props`),
              keySerializer = new ByteArraySerializer,
              valueSerializer = value.serializer()
            )
          )
        }
        producers.get(value.serializer().getClass).map(_.asInstanceOf[KafkaProducer[Array[Byte], SerializerProvider[T]]].send(
          key match {
            case Some(keyData) ⇒
              new ProducerRecord(topic, keyData, value)
            case None ⇒
              new ProducerRecord(topic, value)
          },
          callback
        ))
        Some(callback.result())
      case None ⇒
        None
    }
  }

  /**
   * Consume a Kafka message from a given topic using given deserializer.
   * @param topic topic to consume from
   * @param deserializer deserializer to handle the type of the message
   * @tparam T type of the message to consume
   * @return a consumed object, if available at the time of the call
   */
  def consume[T <: DeserializerProvider[_]](topic: String)(implicit deserializer: Deserializer[T]): Option[T] = {
    kafkaServers.headOption match {
      case Some(kafkaServer) ⇒
        if (!consumers.contains(deserializer.getClass)) {
          val props = configuration.`com.gruchalski.kafka.consumer.props`
          props.put("key.deserializer", (new ByteArrayDeserializer).getClass.getCanonicalName)
          props.put("value.deserializer", deserializer.getClass.getCanonicalName)
          consumers.put(
            deserializer.getClass,
            TestUtils.createNewConsumer(
              brokerList = bootstrapServers().mkString(","),
              groupId = configuration.`com.gruchalski.kafka.consumer.group-id`,
              autoOffsetReset = configuration.`com.gruchalski.kafka.consumer.auto-offset-reset`,
              partitionFetchSize = configuration.`com.gruchalski.kafka.producer.partition-fetch-size`,
              sessionTimeout = configuration.`com.gruchalski.kafka.producer.session-timeout`,
              securityProtocol = configuration.`com.gruchalski.kafka.consumer.security-protocol`,
              props = Some(props)
            )
          )
          None
        }
        None
      case None ⇒
        None
    }
  }

  /**
   * Get bootstrap servers for the safe cluster.
   * @return
   */
  def bootstrapServers(): List[String] = {
    kafkaServers.map { server ⇒
      Try(Some(s"localhost:${server.boundPort(ListenerName.normalised("plaintext"))}")).getOrElse(None)
    }.flatten
  }

}

/**
 * Kafka cluster companion object.
 */
object KafkaCluster {

  def apply()(implicit ec: ExecutionContext, config: Config = ConfigFactory.load().resolve()) =
    new KafkaCluster()(config, ec)

}
