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

import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedDeque}

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

import scala.concurrent.{Future, Promise}

/**
 * Producer callback. Use <code>result()</code> method to get the data.
 */
case class ProducerCallback() extends Callback {
  private val p = Promise[RecordMetadata]()
  override def onCompletion(recordMetadata: RecordMetadata, e: Exception) = {
    Option(e) match {
      case Some(cause) ⇒ p.failure(cause)
      case None        ⇒ p.success(recordMetadata)
    }
  }

  /**
   * Get the data future.
   * @return data future
   */
  def result(): Future[RecordMetadata] = p.future
}

/**
 * Represents a materialized consumed item.
 * @param key deserialized key
 * @param value deserialized value
 * @param record original consumed record
 * @tparam K type of a consumed key
 * @tparam V type of a consumed value
 */
case class ConsumedItem[K, V](key: Option[K], value: V, record: ConsumerRecord[Array[Byte], Array[Byte]])

/**
 * Consumer worker protocol:
 */
object ConsumerWorkerProtocol {
  sealed trait Protocol

  /**
   * Subscribe to a topic.
   * @param topic topic to subscribe to
   * @param outQueue outbound queue to use for the messages of this topic
   */
  case class Subscribe(
    topic: String,
    outQueue: ConcurrentLinkedDeque[ConsumerRecord[Array[Byte], Array[Byte]]]
  ) extends Protocol

  /**
   * As the worker to terminate and close the consumer.
   */
  case class Shutdown() extends Protocol
}

/**
 * Consumer worker runnable.
 * @param configuration Kafka cluster configuration
 * @param consumer Kafka consumer
 */
class ConsumerWorker(
    configuration: Configuration,
    consumer: KafkaConsumer[Array[Byte], Array[Byte]]
) extends Runnable {

  private val logger = Logger(getClass)

  private var terminated = false
  private val subscriptions = new ConcurrentHashMap[String, ConcurrentLinkedDeque[ConsumerRecord[Array[Byte], Array[Byte]]]]()
  private val nextPassSubscribe = new util.ArrayList[ConsumerWorkerProtocol.Subscribe]()

  /**
   * Offer a message to the worker.
   * @param message message to offer
   */
  def askSubscribe(message: ConsumerWorkerProtocol.Subscribe): Unit = {
    if (!terminated) {
      logger.info(s"I will attempt subscribing to: ${message.topic}...")
      nextPassSubscribe.add(message)
    }
  }

  def askStop(message: ConsumerWorkerProtocol.Shutdown): Unit = {
    logger.info(s"I am going to ask the consumer to stop...")
    terminated = true
  }

  override def run(): Unit = {
    import scala.collection.JavaConverters._
    while (!terminated) {

      // do we need to subscribe to anything?
      if (!nextPassSubscribe.isEmpty) {
        nextPassSubscribe.asScala.foreach { message ⇒
          if (!subscriptions.containsKey(message.topic)) {
            if (consumer.listTopics().containsKey(message.topic)) {
              logger.info(s"Subscribing to topic ${message.topic}...")
              consumer.subscribe(List(message.topic).asJava)
              subscriptions.put(message.topic, message.outQueue)
              logger.info(s"Successfully subscribed to topic ${message.topic}.")
            } else {
              logger.warn(s"Topic ${message.topic} not found by the consumer.")
            }
          }
        }
        nextPassSubscribe.clear()
      }

      if (!subscriptions.isEmpty) {
        consumer.poll(configuration.`com.gruchalski.kafka.consumer.poll-timeout-ms`).iterator().asScala.foreach { record ⇒
          if (subscriptions.containsKey(record.topic())) {
            subscriptions.get(record.topic()).offer(record)
          }
        }
      }
    }

    logger.info("Kafka consumer stopped.")

    consumer.close()
  }
}
