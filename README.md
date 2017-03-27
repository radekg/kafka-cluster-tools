# Kafka cluster utilities

An opinionated set of tools for working with Kafka in unit tests.  

Primararly targeted at Scala 2.12 but comes with a Java 8 compatibility API.  

The library handles serialization and deserialization on its own, outside of Kafka. It uses `Array[Byte]` for key
and value serializaers / deserializers under the hood. Serializers and deserializers are provided to the library
by the concrete implementation of the messages.

## Build

    git clone https://github.com/radekg/kafka-cluster-tools.git
    cd kafka-cluster-tools
    sbt clean test publishLocal
    
## Use with SBT

    libraryDependencies += "com.gruchalski" %% "kafka-cluster-tools" % "1.0.0" % "test"

## Use with Maven

    <dependency>
        <groupId>com.gruchalski</groupId>
        <artifactId>kafka-cluster-tools</artifactId>
        <version>1.0.0</version>
        <scope>test</scope>
    </dependency>

## Using with Scala

```scala
import com.gruchalski.kafka.scala.{ConsumedItem, KafkaCluster, KafkaTopicCreateResult, KafkaTopicStatus}
import com.gruchalski.kafka.serializer.scala.TestConcreteProvider // example in test
import org.apache.kafka.clients.producer.RecordMetadata
import scala.concurrent.Future
import scala.util.{Success, Failure}

val cluster = KafkaCluster()
cluster.start() match {
  case Some(safe) ⇒
    val topics = safe.configuration.`com.gruchalski.kafka.topics`.flatten
    Future.sequence(safe.cluster.withTopics(topics)).onComplete {
      case Success(statuses) ⇒
        // to inspect the statuses of topic creation, look inside of statuses
        val topicToUse = "some-topic-created-in-the-previous-step"
        val concreteToUse = TestConcreteProvider.ConcreteExample(property = "full kafka publish / consume test")
        safe.cluster.produce(topicToUse, concreteToUse).foreach { opt ⇒
          opt.foreach { f ⇒
            f.onComplete {
              case Success(metadata) ⇒
                // record has been published, we can try consuming;
                // consuming may require subsequent tries because of the async nature of Kafka:
                implicit val concreteDeserializer = concreteToUse.deserializer()
                // you may have to allow for some time or use an approach similar to scalatest Eventually:
                val consumed = safe.cluster.consume[TestConcreteProvider.ConcreteExample](topicToUse)
                // when finished working with it, shut it all down:
                safe.cluster.stop()
              case Failure(ex)       ⇒ println(s"Failed to publish. Reason: $ex.")
            }
          }
        }
      case Failure(ex) ⇒
        println(s"Failed to start the cluster. Reason $ex.")
    }
  case None ⇒ println("Expected Kafka cluster to come up.")
}
```

## Using with Java 8

The library comes with a Java 8 compatibility layer. To use the library with Java 8:

```java
import com.gruchalski.kafka.java8.KafkaCluster;
import com.gruchalski.kafka.java8.KafkaClusterSafe;
import com.gruchalski.kafka.java8.KafkaTopicStatus;
import com.gruchalski.kafka.java8.ProducerCallback;
import com.gruchalski.kafka.scala.ConsumedItem;
import com.gruchalski.kafka.scala.KafkaTopicConfiguration;
import com.gruchalski.kafka.scala.KafkaTopicCreateResult;

import com.gruchalski.kafka.serializer.java8.concrete.ConcreteJavaMessageImplementation; // in test

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Playground {
    public void runIt() {
        KafkaCluster cluster = new KafkaCluster();
        Optional<KafkaClusterSafe> maybeCluster = cluster.start();
        maybeCluster.ifPresent(new Consumer<KafkaClusterSafe>() {
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
                
                // request creating topics:
                CountDownLatch latch = new CountDownLatch(topics.size());
                CompletableFuture<List<KafkaTopicCreateResult>> topicCreateStatuses = kafkaClusterSafe.cluster.withTopics(topics);
                // wait for topics to be created:
                topicCreateStatuses.thenAccept(results -> {
                    for (KafkaTopicCreateResult result : results) {
                        if (result.status().toString().equals(KafkaTopicStatus.Exists)) {
                            latch.countDown();
                        } else {
                            System.out.println("Expected topic " + result.topicConfig().name() + " to be created.");
                            kafkaClusterSafe.cluster.stop();
                            System.exit(1);
                        }
                    }
                });
                
                try {
                    latch.await(10000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    System.out.println(ex.getMessage());
                    kafkaClusterSafe.cluster.stop();
                    System.exit(1);
                }
                
                // create a message:
                ConcreteJavaMessageImplementation concrete = new ConcreteJavaMessageImplementation("unit-test-concrete");
                // create a callback:
                ProducerCallback callback = new ProducerCallback();
                // produce the message:
                
                try {
                    kafkaClusterSafe.cluster.produce(
                            topics.get(0).name(),
                            concrete,
                            callback);
                    // wait for the metadata or error:
                    CountDownLatch producerLatch = new CountDownLatch(1);
                    callback.result().thenAccept(result -> {
                       producerLatch.countDown();
                    });
                    
                    try {
                        producerLatch.await(10000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ex) {
                        System.out.println(ex.getMessage());
                        kafkaClusterSafe.cluster.stop();
                        System.exit(1);
                    }
                    
                    AsyncUtil.eventually(() -> {
                        try {
                            Optional<ConsumedItem<ConcreteJavaMessageImplementation>> consumed = kafkaClusterSafe.cluster.consume(topics.get(0).name(), concrete.deserializer());
                            assertEquals(consumed.get().deserializedItem().property, concrete.property);
                        } catch (Throwable t) {
                            System.out.println("Expected while consuming: " + t.getMessage() + ".");
                            kafkaClusterSafe.cluster.stop();
                            System.exit(1);
                        }
                    });
                } catch (Throwable t) {
                    System.out.println("Error while publishing: " + t.getMessage() + ".");
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
- override parts of the config with:

```
ConfigFactory.empty()
  .withValue(..., ConfigValueFactory.fromAnyRef(...))
  // ...
  .withFallback(
    ConfigFactory.load().resolve() // merge with the default reference.conf
  )
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