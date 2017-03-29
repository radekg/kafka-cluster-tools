# Kafka cluster utilities

An opinionated set of tools for working with Kafka in unit tests.  

Primarily targeted at Scala 2.12 but comes with a Java 8 compatibility API.  

The library handles serialization and deserialization on its own, outside of Kafka. It uses `Array[Byte]` for key
and value serializers / deserializers under the hood. Serializers and deserializers are provided to the library
by the concrete implementation of the messages.

## Build

    git clone https://github.com/radekg/kafka-cluster-tools.git
    cd kafka-cluster-tools
    sbt clean test publishLocal
    
## Use with SBT

    libraryDependencies += "com.gruchalski" %% "kafka-cluster-tools" % "1.2.1" % "test"

## Use with Maven

    <dependency>
        <groupId>com.gruchalski</groupId>
        <artifactId>kafka-cluster-tools</artifactId>
        <version>1.2.1</version>
        <scope>test</scope>
    </dependency>

## Using with Scala

```scala
import com.gruchalski.kafka.scala.{ConsumedItem, KafkaCluster, KafkaTopicCreateResult, KafkaTopicStatus}
import org.apache.kafka.clients.producer.RecordMetadata
import scala.concurrent.Future
import scala.util.{Success, Failure}

import com.gruchalski.kafka.scala.DefaultSerdes._

implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

// create a new instance of the cluster and start it:
KafkaCluster().start() match {
  case None ⇒ println("Expected Kafka cluster to come up.")
  case Some(safe) ⇒
    // the cluster is now started, create topics; topics can be declared in the configuration:
    val topics = safe.configuration.`com.gruchalski.kafka.topics`.flatten
    Future.sequence(safe.cluster.withTopics(topics)).onComplete {
      case Failure(error) ⇒ println(s"Failed to start the cluster. Reason $error.")
      case Success(statuses) ⇒
        // topics have been created; to inspect the statuses of topic creation, look inside of statuses
        val topicToUse = "some-topic-created-in-the-previous-step"
        safe.cluster.produce(topicToUse, Some("a key"), "a value").toEither match {
          case Left(error) ⇒ println(s"Failed to publish. Reason: $error.")
          case Right(fut) ⇒ fut.onComplete {
            case Failure(error) ⇒ println(s"Failed to publish. Reason: $error.")
            case Success(recordMetadata) ⇒
              // message has been published, we can consume;
              // one may have to allow for some time or use an approach similar to scalatest Eventually:
              val consumed = safe.cluster.consume[String, String](topicToUse)
              // when finished working with it, shut it all down:
              safe.cluster.stop()
          }
        }
    }
}
```

## Using with Java 8

The library comes with a Java 8 compatibility layer. To use the library with Java 8, an example straight from
the unit test:

```java
package com.gruchalski.kafka.test;

import com.gruchalski.kafka.java8.KafkaCluster;
import com.gruchalski.kafka.java8.KafkaClusterSafe;
import com.gruchalski.kafka.java8.KafkaTopicStatus;
import com.gruchalski.kafka.scala.ConsumedItem;
import com.gruchalski.kafka.scala.KafkaTopicConfiguration;
import com.gruchalski.kafka.scala.KafkaTopicCreateResult;
import com.gruchalski.testing.AsyncUtil;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class SimpleTypesTest extends TestCase {
    private String aKey = "a test key";
    private String aValue = "a test value";
    public void testClusterSetup() {
        new KafkaCluster().start().ifPresent(new Consumer<KafkaClusterSafe>() {
            @Override
            public void accept(KafkaClusterSafe kafkaClusterSafe) {
                ArrayList<KafkaTopicConfiguration> topics = new ArrayList<>();
                topics.add(new KafkaTopicConfiguration(
                        "test-topic",
                        1,
                        1,
                        new Properties(),
                        KafkaTopicConfiguration.toRackAwareMode("enforced").get()
                ));
                CountDownLatch latch = new CountDownLatch(topics.size());
                CompletableFuture<List<KafkaTopicCreateResult>> topicCreateStatuses = kafkaClusterSafe.cluster.withTopics(topics);
                topicCreateStatuses.thenAccept(results -> {
                    for (KafkaTopicCreateResult result : results) {
                        if (result.status().toString().equals(KafkaTopicStatus.Exists)) {
                            latch.countDown();
                        } else {
                            fail("Expected topic " + result.topicConfig().name() + " to be created.");
                        }
                    }
                });
                try {
                    latch.await(10000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    fail(ex.getMessage());
                }
                try {
                    CountDownLatch producerLatch = new CountDownLatch(1);
                    kafkaClusterSafe.cluster.produce(
                            topics.get(0).name(),
                            aKey,
                            aValue).thenAccept(result -> {
                        producerLatch.countDown();
                    });
                    try {
                        producerLatch.await(10000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ex) {
                        fail(ex.getMessage());
                    }
                    AsyncUtil.eventually(() -> {
                        try {
                            Optional<ConsumedItem<String, String>> consumed = kafkaClusterSafe.cluster.consume(
                                    topics.get(0).name(), String.class, String.class);
                            assertEquals(consumed.get().key(), aKey);
                            assertEquals(consumed.get().value(), aValue);
                        } catch (Throwable t) {
                            fail(t.getMessage());
                        }
                    });
                } catch (Throwable t) {
                    fail(t.getMessage());
                } finally {
                    kafkaClusterSafe.cluster.stop();
                }
            }
        });
    }
}
```

## Configuration

The configuration is documented in the `reference.conf` file. Please have a look inside. As this project uses
Typesafe Config for configuration, the usual Typesafe Config rules apply. Especially:

- override full config with: `ConfigFactory.parseProperties(java.util.Properties)`
- load full config directly from a resource file: `ConfigFactory.parseFile(new File(getClass.getClassLoader.getResource("custom-config.conf").toURI))`
- override parts of the config with:

```
ConfigFactory.empty()
  .withValue(..., ConfigValueFactory.fromAnyRef(...))
  // ...
  .withFallback(
    ConfigFactory.load().resolve() // merge with the default reference.conf
  )
```

- use `-Dconfig.file=...` to override the complete configuration

[More about Typesafe Config](https://github.com/typesafehub/config).

## Custom serializers / deserializers

### Scala

To create a serializer / deserializer for a case class, follow the example below:

```scala
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.msgpack.core.MessagePack
import scala.util.Try

sealed trait TestConcrete
case class ConcreteExample(property: String = "I am concrete") extends TestConcrete {
  def typeId: Int = 1
}

object TestConcreteSerdes {
  implicit val concreteExampleSerializer = new Serializer[ConcreteExample] {
    override def configure(map: java.util.Map[String, _], b: Boolean): Unit = {}
    override def close(): Unit = {}
    override def serialize(topic: String, input: ConcreteExample): Array[Byte] = {
      val packer = MessagePack.newDefaultBufferPacker()
      try {
        packer
          .packInt(input.typeId)
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
  implicit val concreteExampleDeserializer = new Deserializer[ConcreteExample] {
    override def configure(map: java.util.Map[String, _], b: Boolean): Unit = {}
    override def close(): Unit = {}
    override def deserialize(topic: String, input: Array[Byte]): ConcreteExample = {
      val unpacker = MessagePack.newDefaultUnpacker(input)
      try {
        // if the type is what you expect it to be...
        val _type = unpacker.unpackInt()
        val concrete = ConcreteExample(property = unpacker.unpackString())
        if (_type == concrete.typeId) {
          concrete
        } else {
          null.asInstanceOf[ConcreteExample]
        }
      } catch {
        case any: Throwable ⇒
          null.asInstanceOf[ConcreteExample]
      } finally {
        Try(unpacker.close())
      }
    }
  }
}
```

And to publish / consume:

```scala
import TestConcreteSerdes._
// publish:
safe.cluster.produce("topic", Some("a key"), ConcreteExample(property = "Hey, publishing custom type"))
// consume:
val consumed = safe.cluster.consume[String, ConcreteExample]("topic")
// consumed.toEither should be
// Right(Some(ConsumedItem(Some("a key"), concrete-item, original-kafka-record)))
```

### Java

In Java, this is a little bit more complicated because there is no way to take advantage of automatic type inference.

First, a class we would like to serialize / deserialize:

```java
public class JavaMessage {
    public int id() {
        return 1;
    }
    public final String property;
    public JavaMessage() {
        this("undefined value");
    }
    public JavaMessage(String property) {
        this.property = property;
    }
}
```

Create a serializer class:

```java
import org.apache.kafka.common.serialization.Serializer;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.util.Map;

public class JavaMessageSerializer implements Serializer<JavaMessage> {
    public void configure(Map<String, ?> var1, boolean var2) {}
    public void close() {}
    public byte[] serialize(String topic, JavaMessage input) {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        try {
            packer.packInt(input.id())
                    // actual data:
                    .packString(input.property);
            return packer.toByteArray();
        } catch (IOException ex) {
            return null;
        }
    }
}
```

And the deserializer:

```java
import org.apache.kafka.common.serialization.Deserializer;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.util.Map;

public class JavaMessageDeserializer implements Deserializer<JavaMessage> {
    public void configure(Map<String, ?> var1, boolean var2) {}
    public void close() {}
    public JavaMessage deserialize(String topic, byte[] data) {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data);
        try {
            int type = unpacker.unpackInt();
            JavaMessage value = new JavaMessage(unpacker.unpackString());
            if (value.id() == type) {
                return value;
            }
        } catch (IOException ex) {
            return null;
        } finally {
            try { unpacker.close(); } catch (IOException ex) { }
        }
        return null;
    }
}
```

Before these can be published and consumed, they need to be registered in the serde registry:

```java
import com.gruchalski.kafka.java8.SerdeRegistry;

public class Demo {
    SerdeRegistry serdes = SerdeRegistry.getInstance();
    public Demo() {
        serdes.registerSerializer(JavaMessage.class, new JavaMessageSerializer());
        serdes.registerDeserializer(JavaMessage.class, new JavaMessageDeserializer());
    }
    
    // and now they can be published and consumed:
    
    public CompletableFuture<RecordMetadata> produce(KafkaCluster cluster, JavaMessage message) {
        return cluster.produce(
                "topic",
                "a key",
                message);
        /*
        Use:
        CompletableFuture<RecordMetadata> f = produce(...)
        f.thenAccept(result -> {
          // the message has been produced
        });
        */
    }
    
    public Optional<ConsumedItem<String, JavaMessage>> consume(KafkaCluster cluster) {
        return cluster.consume("topic", String.class, JavaMessage.class);
    }
}
```

## License

Author: Radek Gruchalski (radek@gruchalski.com)

This work will be available under Apache License, Version 2.0.

Copyright 2017 Rad Gruchalski (radek@gruchalski.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
the specific language governing permissions and limitations under the License.