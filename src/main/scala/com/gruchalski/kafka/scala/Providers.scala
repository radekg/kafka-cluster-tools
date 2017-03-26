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

import concurrent.{Future, Promise}
import util.Try

case class ProducerCallback() extends Callback {
  private val p = Promise[RecordMetadata]()
  override def onCompletion(recordMetadata: RecordMetadata, e: Exception) = {
    Option(e) match {
      case Some(cause) ⇒ p.failure(cause)
      case None        ⇒ p.success(recordMetadata)
    }
  }
  def result(): Future[RecordMetadata] = p.future
}

case class ConsumedItem[T](deserializedItem: T, record: ConsumerRecord[Array[Byte], Array[Byte]])

case class ConsumerWork(
    pollTimeout: Long,
    consumer: KafkaConsumer[Array[Byte], Array[Byte]]
)(callback: (Either[Throwable, List[ConsumerRecord[Array[Byte], Array[Byte]]]]) ⇒ Unit) extends Runnable {
  override def run(): Unit = {
    import collection.JavaConverters._
    callback.apply(Try(consumer.poll(pollTimeout).asScala.toList).toEither)
  }
}
