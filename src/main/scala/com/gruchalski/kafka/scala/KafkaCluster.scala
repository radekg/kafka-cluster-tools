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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedDeque, ExecutorService, Executors}

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.serialization.{ByteArraySerializer, Deserializer, Serializer}

import scala.collection.mutable.{HashMap ⇒ MHashMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Representation of a successfully started cluster. A cluster instance wrapped in this object is safe to use
 * in the program. Returned by [[KafkaCluster.start()]].
 * @param cluster a safe to use cluster
 * @param configuration configuration used by the cluster
 */
case class KafkaClusterSafe(cluster: KafkaCluster, configuration: Configuration)

class KafkaCluster()(implicit
  config: Config,
    ec: ExecutionContext) {

  import DefaultSerdes._

  private val logger = Logger(getClass)

  private val configuration = Configuration(config)
  private val zooKeeper = EmbeddedZooKeeper(configuration)
  private var maybeZooKeeperData: Option[EmbeddedZooKeeper.EmbeddedZooKeeperData] = None
  private var kafkaServers = List.empty[KafkaServer]
  private var executor: Option[ExecutorService] = None

  private var producer: Option[KafkaProducer[Array[Byte], Array[Byte]]] = None
  private var consumerWorker: Option[ConsumerWorker] = None
  private val consumerInitialized = new AtomicBoolean(false)

  private var outQueues = MHashMap.empty[String, KafkaCluster.OutQueue]

  /**
   * Start the cluster. Method is idempotent, subsequent calls return currently running cluster, if cluster is
   * running.<br/>
   * The method will start a ZooKeeper ensemble to be used by the Kafka cluster.
   * @return safe to use cluster data
   */
  def start(): Option[KafkaClusterSafe] = {
    logger.info("Attempting starting Kafka Cluster...")
    if (kafkaServers.length == 0) {
      zooKeeper.start() match {
        case Some(EmbeddedZooKeeper.EmbeddedZooKeeperData(instances, connectionString)) ⇒
          logger.info(s"Requesting ${configuration.`com.gruchalski.kafka.cluster.size`} Kafka brokers...")
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
            logger.error(s"Only ${kafkaServers.length} Kafka brokers came up successfully. Kafka is now going to stop.")
            // not all servers started, we shouldn't continue:
            stop()
            logger.error("Kafka is not running.")
            None
          } else {
            maybeZooKeeperData = Some(EmbeddedZooKeeper.EmbeddedZooKeeperData(instances, connectionString))
            executor = Some(Executors.newSingleThreadExecutor())
            logger.info("Kafka cluster is now running.")
            Some(KafkaClusterSafe(this, configuration))
          }
        case None ⇒
          logger.error("ZooKeeper has not started. Kafka cluster will not attempting its start.")
          None
      }
    } else {
      logger.warn("Kafka cluster is already running.")
      Some(KafkaClusterSafe(this, configuration))
    }
  }

  /**
   * Stop the cluster, if running.
   */
  def stop(): Unit = {
    logger.info("Kafka cluster stop requested, stopping all components...")
    executor.foreach(_.shutdownNow())
    producer.foreach(_.close())
    consumerWorker.foreach { c ⇒ Try(c.askStop(ConsumerWorkerProtocol.Shutdown())) }
    kafkaServers.foreach(_.shutdown())
    zooKeeper.stop()
    consumerWorker = None
    producer = None
    executor = None
    outQueues.clear()
    maybeZooKeeperData = None
    kafkaServers = List.empty[KafkaServer]
    consumerInitialized.set(false)
    logger.info("All components stopped. Please consider not reusing this instance.")
  }

  /**
   * Create topics and wait for the topics to be ready to use. For every topic requestd, if the topic is not found
   * in Kafka within the <code>com.gruchalski.kafka.topic-wait-for-create-success-timeout-ms</code> timeout,
   * the topic will be considered as not created.
   * @param topicConfigs topic configurations
   * @return list of create statuses futures
   */
  def withTopics(implicit topicConfigs: List[KafkaTopicConfiguration] = List.empty[KafkaTopicConfiguration]): List[Future[KafkaTopicCreateResult]] = {
    kafkaServers.headOption match {
      case Some(kafkaServer) ⇒
        topicConfigs.map { topicConfig ⇒

          logger.info(s"Attempting creating topic ${topicConfig.name} with partitions: ${topicConfig.partitions}...")
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
              var status: KafkaTopicStatus.Status = KafkaTopicStatus.DoesNotExist()
              var timeout = false
              while (status == KafkaTopicStatus.DoesNotExist || !timeout) {
                Thread.sleep(50)
                if (AdminUtils.topicExists(kafkaServer.zkUtils, topicConfig.name)) {
                  status = KafkaTopicStatus.Exists()
                }
                timeout = System.currentTimeMillis() - start >= configuration.`com.gruchalski.kafka.topic-wait-for-create-success-timeout-ms`
              }
              if (timeout && status == KafkaTopicStatus.DoesNotExist()) {
                logger.error(s"Creating topic ${topicConfig.name} timed out. Considered not created!")
              }
              KafkaTopicCreateResult(topicConfig, status)
            }
          }.toEither match {
            case Left(error) ⇒
              logger.error(s"Topic ${topicConfig.name} not created. Reason: $error.")
              Future(KafkaTopicCreateResult(topicConfig, KafkaTopicStatus.DoesNotExist(), Some(error)))
            case Right(result) ⇒
              logger.info(s"Topic ${topicConfig.name} created sucessfully.")
              result
          }

        }
      case None ⇒
        logger.error("No Kafka servers available in the cluster for topic creation.")
        List.empty[Future[KafkaTopicCreateResult]]
    }
  }

  /**
   * For a list of topics, return a list of expected successful create statuses.
   * @param topicConfigs topic configurations
   * @return list of expected successful statuses
   */
  def expectedResultsForTopicCreation(implicit topicConfigs: List[KafkaTopicConfiguration] = List.empty[KafkaTopicConfiguration]): List[KafkaTopicCreateResult] = {
    topicConfigs.map { topicConfig ⇒
      new KafkaTopicCreateResult(topicConfig, KafkaTopicStatus.Exists(), None)
    }
  }

  /**
   * Produce a message of a given type. If the producer for the given type does not exist, it will be created.
   * @param topic topic to send the message to
   * @param value value to send
   * @tparam V type of the value to send
   * @return the metadata / error future
   */
  implicit def produce[V](topic: String, value: V)(implicit serializer: Serializer[V]): Try[Future[RecordMetadata]] = {
    produce[Array[Byte], V](topic, None, value)
  }

  /**
   * Produce a message of a given type. If the producer for the given type does not exist, it will be created.
   * @param topic topic to send the message to
   * @param key key to send
   * @param value value to send
   * @tparam K type of the key to send
   * @tparam V type of the value to send
   * @return the metadata / error future
   */
  implicit def produce[K, V](topic: String, key: Option[K], value: V)(implicit keySerializer: Serializer[K], valueSerializer: Serializer[V]): Try[Future[RecordMetadata]] = {
    kafkaServers.headOption match {
      case Some(kafkaServer) ⇒
        Try {
          if (producer == None) {
            logger.info("No producer found. Creating one...")
            producer = Some(TestUtils.createNewProducer[Array[Byte], Array[Byte]](
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
              valueSerializer = new ByteArraySerializer
            ))
            logger.info("Producer is now ready.")
          }
          val callback = new ProducerCallback
          producer.foreach { p ⇒
            p.send(
              key match {
                case Some(keyData) ⇒
                  new ProducerRecord(topic, keySerializer.serialize(topic, keyData.asInstanceOf[K]), valueSerializer.serialize(topic, value.asInstanceOf[V]))
                case None ⇒
                  new ProducerRecord(topic, valueSerializer.serialize(topic, value.asInstanceOf[V]))
              },
              callback
            )
          }
          callback.result()
        }
      case None ⇒
        logger.error("No Kafka servers available in the cluster for publishing.")
        Try(throw new NoServersToProduce)
    }
  }

  implicit def consume[V](topic: String)(implicit valueDeserializer: Deserializer[V]): Try[Option[ConsumedItem[Array[Byte], V]]] = {
    consume[Array[Byte], V](topic)
  }

  /**
   * Consume a Kafka message from a given topic using given deserializer.
   * @param topic topic to consume from
   * @param keyDeserializer deserializer to handle the key
   * @param valueDeserializer deserializer to handle the value
   * @tparam K type of the key to consume
   * @tparam V type of the value to consume
   * @return a consumed object, if available at the time of the call
   */
  implicit def consume[K, V](topic: String)(implicit keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): Try[Option[ConsumedItem[K, V]]] = {
    kafkaServers.headOption match {
      case Some(kafkaServer) ⇒
        Try {
          if (consumerWorker == None) {
            logger.info("No consumer found. Creating one...")
            val consumer = TestUtils.createNewConsumer(
              brokerList = bootstrapServers().mkString(","),
              groupId = configuration.`com.gruchalski.kafka.consumer.group-id`,
              autoOffsetReset = configuration.`com.gruchalski.kafka.consumer.auto-offset-reset`,
              partitionFetchSize = configuration.`com.gruchalski.kafka.consumer.partition-fetch-size`,
              sessionTimeout = configuration.`com.gruchalski.kafka.consumer.session-timeout`,
              securityProtocol = configuration.`com.gruchalski.kafka.consumer.security-protocol`,
              props = Some(configuration.`com.gruchalski.kafka.consumer.props`)
            )
            logger.info("Consumer is now ready.")
            val work = new ConsumerWorker(configuration, consumer)
            consumerWorker = Some(work)
            executor.foreach(_.submit(work))
          }
          if (!outQueues.contains(topic)) {
            consumerWorker.foreach { worker ⇒
              val newQueue = new ConcurrentLinkedDeque[ConsumerRecord[Array[Byte], Array[Byte]]]()
              outQueues.put(topic, newQueue)
              worker.askSubscribe(ConsumerWorkerProtocol.Subscribe(topic, newQueue))
            }
          }

          outQueues.get(topic) match {
            case Some(queue) ⇒
              Option(queue.poll()) match {
                case Some(record) ⇒
                  Some(
                    ConsumedItem(
                      Try(Option(keyDeserializer.deserialize(topic, record.key()))).getOrElse(None),
                      valueDeserializer.deserialize(topic, record.value()),
                      record
                    )
                  )
                case None ⇒
                  None
              }
            case None ⇒
              throw new ExpectedAtLeastEmptyTopicPLaceholderException(topic)
          }

        }
      case None ⇒
        logger.error("No Kafka servers available in the cluster for consumption.")
        Try(None)
    }
  }

  /**
   * Get bootstrap servers for the safe cluster.
   * @return list of plaintext listener addresses
   */
  def bootstrapServers(): List[String] = {
    kafkaServers.map { server ⇒
      Try(Some(s"localhost:${server.boundPort(ListenerName.normalised("plaintext"))}")).getOrElse(None)
    }.flatten
  }

  /**
   * Get list of ZooKeeper addresses, if any.
   * @return list of zookeeper addresses
   */
  def zooKeeperAddresses(): List[String] = maybeZooKeeperData
    .map(_.connectionString.split(",").toList)
    .getOrElse(List.empty[String])

}

/**
 * Kafka cluster companion object.
 */
object KafkaCluster {

  type OutQueue = ConcurrentLinkedDeque[ConsumerRecord[Array[Byte], Array[Byte]]]

  def apply()(implicit
    ec: ExecutionContext = concurrent.ExecutionContext.Implicits.global,
    config: Config = ConfigFactory.load().resolve()) =
    new KafkaCluster()(config, ec)

}
