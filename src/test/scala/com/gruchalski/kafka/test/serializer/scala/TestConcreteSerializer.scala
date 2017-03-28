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

package com.gruchalski.kafka.test.serializer.scala

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.msgpack.core.MessagePack

import scala.util.Try

object TestConcreteSerdes {

  implicit val concreteExampleSerializer = new Serializer[TestConcreteProvider.ConcreteExample] {
    override def configure(map: java.util.Map[String, _], b: Boolean): Unit = {}
    override def close(): Unit = {}
    override def serialize(topic: String, input: TestConcreteProvider.ConcreteExample): Array[Byte] = {
      val packer = MessagePack.newDefaultBufferPacker()
      try {
        packer
          .packInt(input.id)
          .packInt(1)
          // actual data:
          .packString(input.property)
        packer.toByteArray
      } catch {
        case any: Throwable ⇒
          null
      } finally {
        Try(packer.close())
      }
    }
  }

  implicit val concreteExampleDeserializer = new Deserializer[TestConcreteProvider.ConcreteExample] {
    override def configure(map: java.util.Map[String, _], b: Boolean): Unit = {}
    override def close(): Unit = {}
    override def deserialize(topic: String, input: Array[Byte]): TestConcreteProvider.ConcreteExample = {
      val unpacker = MessagePack.newDefaultUnpacker(input)
      try {
        val `type` = unpacker.unpackInt()
        val version = unpacker.unpackInt()
        if (Some(`type`) == TestConcreteProvider.types.get(TestConcreteProvider.ConcreteExample.getClass)) {
          TestConcreteProvider.ConcreteExample(property = unpacker.unpackString())
        } else {
          null.asInstanceOf[TestConcreteProvider.ConcreteExample]
        }
      } catch {
        case any: Throwable ⇒
          null.asInstanceOf[TestConcreteProvider.ConcreteExample]
      } finally {
        Try(unpacker.close())
      }
    }
  }

}

object TestConcreteProvider {

  sealed trait IdProvider { val id: Int }

  val types = Map[Class[_], Int](
    ConcreteExample.getClass → 1
  )

  sealed trait TestConcrete extends IdProvider
  case class ConcreteExample(property: String = "I am concrete")
      extends TestConcrete {
    override val id = types.get(ConcreteExample.getClass) match {
      case Some(value) ⇒ value
      case None ⇒
        throw new Exception(s"Serialization error: serialization type ID not found for class ${getClass} in $types.")
    }
  }

}
