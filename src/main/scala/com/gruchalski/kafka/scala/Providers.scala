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

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

import scala.concurrent.{Future, Promise}
import scala.util.Try

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
 * Internal Kafka consumer worker.
 * @param pollTimeout how long to wait for a single batch of data
 * @param consumer Kafka consumer
 * @param callback callback
 */
case class ConsumerWork(
    pollTimeout: Long,
    consumer: KafkaConsumer[Array[Byte], Array[Byte]]
)(callback: (Either[Throwable, List[ConsumerRecord[Array[Byte], Array[Byte]]]]) ⇒ Unit) extends Runnable {
  override def run(): Unit = {
    import collection.JavaConverters._
    callback.apply(Try(consumer.poll(pollTimeout).asScala.toList).toEither)
  }
}
