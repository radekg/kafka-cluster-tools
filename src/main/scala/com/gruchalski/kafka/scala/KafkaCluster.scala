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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{ConcurrentLinkedDeque, ExecutorService, Executors}

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.Logger
import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.serialization.{ByteArraySerializer, Deserializer}

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

  private val logger = Logger(getClass)

  private val configuration = Configuration(config)
  private val zooKeeper = EmbeddedZooKeeper(configuration)
  private var kafkaServers = List.empty[KafkaServer]
  private var executor: Option[ExecutorService] = None

  private var producer: Option[KafkaProducer[Array[Byte], Array[Byte]]] = None
  private var consumer: Option[KafkaConsumer[Array[Byte], Array[Byte]]] = None
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
    consumer.foreach { c ⇒ Try(c.close()) }
    kafkaServers.foreach(_.shutdown())
    zooKeeper.stop()
    consumer = None
    producer = None
    executor = None
    outQueues.clear()
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
  def withTopics(topicConfigs: List[KafkaTopicConfiguration]): List[Future[KafkaTopicCreateResult]] = {
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
              if (timeout) {
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
   * Produce a message of a given type. If the producer for the given type does not exist, it will be created.
   * @param topic topic to send the message to
   * @param value value to send
   * @param callback callback handling metadata or error, the callback is used to return a scala future
   * @tparam T type of the value to send
   * @return the metadata / error future
   */
  def produce[T](topic: String, value: SerializerProvider[T], callback: ProducerCallback = ProducerCallback()): Try[Option[Future[RecordMetadata]]] = {
    produce(topic, None, value, callback)
  }

  def produce[T](topic: String, key: Option[Array[Byte]], value: SerializerProvider[T], callback: ProducerCallback): Try[Option[Future[RecordMetadata]]] = {
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
          producer.foreach { p ⇒
            p.send(
              key match {
                case Some(keyData) ⇒
                  new ProducerRecord(topic, keyData, value.serializer().serialize(topic, value.asInstanceOf[T]))
                case None ⇒
                  new ProducerRecord(topic, value.serializer().serialize(topic, value.asInstanceOf[T]))
              },
              callback
            )
          }
          Some(callback.result())
        }
      case None ⇒
        logger.error("No Kafka servers available in the cluster for publishing.")
        Try(None)
    }
  }

  /**
   * Consume a Kafka message from a given topic using given deserializer.
   * @param topic topic to consume from
   * @param deserializer deserializer to handle the type of the message
   * @tparam T type of the message to consume
   * @return a consumed object, if available at the time of the call
   */
  def consume[T <: DeserializerProvider[_]](topic: String)(implicit deserializer: Deserializer[T]): Try[Option[ConsumedItem[T]]] = {
    kafkaServers.headOption match {
      case Some(kafkaServer) ⇒
        Try {
          if (consumer == None) {
            logger.info("No consumer found. Creating one...")
            val _consumer = TestUtils.createNewConsumer(
              brokerList = bootstrapServers().mkString(","),
              groupId = configuration.`com.gruchalski.kafka.consumer.group-id`,
              autoOffsetReset = configuration.`com.gruchalski.kafka.consumer.auto-offset-reset`,
              partitionFetchSize = configuration.`com.gruchalski.kafka.consumer.partition-fetch-size`,
              sessionTimeout = configuration.`com.gruchalski.kafka.consumer.session-timeout`,
              securityProtocol = configuration.`com.gruchalski.kafka.consumer.security-protocol`,
              props = Some(configuration.`com.gruchalski.kafka.consumer.props`)
            )
            logger.info("Consumer is now ready.")
            consumer = Some(_consumer)
          }
          if (!outQueues.contains(topic)) {
            consumer.foreach { c ⇒
              if (c.listTopics().containsKey(topic)) {
                import collection.JavaConverters._
                if (consumerInitialized.get() == false) {
                  schedulePoll()
                  consumerInitialized.set(true)
                }
                outQueues.put(topic, new ConcurrentLinkedDeque[ConsumerRecord[Array[Byte], Array[Byte]]]())
                c.subscribe(List(topic).asJava)
              } else {
                throw new NoTopicException(topic)
              }
            }
          }

          Option(outQueues.getOrElseUpdate(topic, new ConcurrentLinkedDeque[ConsumerRecord[Array[Byte], Array[Byte]]]()).poll()) match {
            case Some(record) ⇒
              Some(ConsumedItem(deserializer.deserialize(topic, record.value()), record))
            case None ⇒
              None
          }
        }
      case None ⇒
        logger.error("No Kafka servers available in the cluster for consumption.")
        Try(None)
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

  private def schedulePoll(): Unit = {
    consumer.foreach { c ⇒
      executor.foreach { exec ⇒
        if (!exec.isShutdown) {
          scheduleWithConsumerAndExecutor(c, exec)
        }
      }
    }
  }

  private def scheduleWithConsumerAndExecutor(consumer: KafkaConsumer[Array[Byte], Array[Byte]], executor: ExecutorService): Unit = {
    executor.submit(ConsumerWork(configuration.`com.gruchalski.kafka.consumer.poll-timeout-ms`, consumer) { result ⇒
      result match {
        case Left(error) ⇒
          logger.error(s"Error while polling for messages. Reason: $error.")
        case Right(records) ⇒
          records.foreach { record ⇒
            outQueues.get(record.topic).foreach(_.offer(record))
          }
      }
      schedulePoll()
    })
  }

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
