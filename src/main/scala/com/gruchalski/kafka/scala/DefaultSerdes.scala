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

import org.apache.kafka.common.serialization._

object DefaultSerdes {

  implicit val byteArraySerializer = new ByteArraySerializer
  implicit val byteArrayDeserializer = new ByteArrayDeserializer

  implicit val byteBufferSerializer = new ByteBufferSerializer
  implicit val byteBufferDeserializer = new ByteBufferDeserializer

  implicit val bytesSerializer = new BytesSerializer
  implicit val bytesDeserializer = new BytesDeserializer

  implicit val doubleSerializer = new DoubleSerializer
  implicit val doubleDeserializer = new DoubleDeserializer

  implicit val integerSerializer = new IntegerSerializer
  implicit val integerDeserializer = new IntegerDeserializer

  implicit val longSerializer = new LongSerializer
  implicit val longDeserializer = new LongDeserializer

  implicit val stringSerializer = new StringSerializer
  implicit val stringDeserializer = new StringDeserializer

  // scala specific:

  implicit val intSerializer = new Serializer[Int] {
    val s = integerSerializer
    def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = s.configure(configs, isKey)
    def serialize(topic: String, data: Int): Array[Byte] = s.serialize(topic, data)
    def close() = s.close()
  }
  implicit val intDeserializer = new Deserializer[Int] {
    val d = integerDeserializer
    def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = d.configure(configs, isKey)
    def deserialize(topic: String, data: Array[Byte]): Int = d.deserialize(topic, data)
    def close() = d.close()
  }

  implicit val scalaDoubleSerializer = new Serializer[Double] {
    val s = doubleSerializer
    def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = s.configure(configs, isKey)
    def serialize(topic: String, data: Double): Array[Byte] = s.serialize(topic, data)
    def close() = s.close()
  }
  implicit val scalaDoubleDeserializer = new Deserializer[Double] {
    val d = doubleDeserializer
    def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = d.configure(configs, isKey)
    def deserialize(topic: String, data: Array[Byte]): Double = d.deserialize(topic, data)
    def close() = d.close()
  }

  implicit val scalaLongSerializer = new Serializer[Long] {
    val s = longSerializer
    def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = s.configure(configs, isKey)
    def serialize(topic: String, data: Long): Array[Byte] = s.serialize(topic, data)
    def close() = s.close()
  }
  implicit val scalaLongDeserializer = new Deserializer[Long] {
    val d = longDeserializer
    def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = d.configure(configs, isKey)
    def deserialize(topic: String, data: Array[Byte]): Long = d.deserialize(topic, data)
    def close() = d.close()
  }

}
