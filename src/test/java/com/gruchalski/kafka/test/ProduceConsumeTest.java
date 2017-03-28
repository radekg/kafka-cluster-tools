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

package com.gruchalski.kafka.test;

import com.gruchalski.kafka.java8.KafkaCluster;
import com.gruchalski.kafka.java8.KafkaClusterSafe;
import com.gruchalski.kafka.java8.KafkaTopicStatus;
import com.gruchalski.kafka.java8.SerdeRegistry;
import com.gruchalski.kafka.java8.exceptions.NoDeserializerException;
import com.gruchalski.kafka.java8.exceptions.NoSerializerException;
import com.gruchalski.kafka.scala.ConsumedItem;
import com.gruchalski.kafka.scala.KafkaTopicConfiguration;
import com.gruchalski.kafka.scala.KafkaTopicCreateResult;
import com.gruchalski.kafka.test.serializer.java8.concrete.ConcreteJavaMessageImplementation;
import com.gruchalski.kafka.test.serializer.java8.concrete.JavaConcreteDeserializer;
import com.gruchalski.kafka.test.serializer.java8.concrete.JavaConcreteSerializer;
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

public class ProduceConsumeTest extends TestCase {

    private SerdeRegistry serdes = SerdeRegistry.getInstance();

    @Override
    protected void setUp() throws Exception {
        serdes.registerSerializer(ConcreteJavaMessageImplementation.class, new JavaConcreteSerializer());
        serdes.registerDeserializer(ConcreteJavaMessageImplementation.class, new JavaConcreteDeserializer());
    }

    public void testSerializerDeserializer() {
        try {
            ConcreteJavaMessageImplementation impl = new ConcreteJavaMessageImplementation("unit-test-java");
            byte[] serialized = serdes.getSerializerFor(impl).serialize("topic", impl);
            ConcreteJavaMessageImplementation deserialized = serdes.getDeserializerFor(impl.getClass()).deserialize("test", serialized);
            assertEquals(deserialized.property, impl.property);
        } catch (NoSerializerException ex) {
            fail(ex.getMessage());
        } catch (NoDeserializerException ex) {
            fail(ex.getMessage());
        }
    }

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
                ConcreteJavaMessageImplementation concrete = new ConcreteJavaMessageImplementation("unit-test-concrete");
                try {
                    CountDownLatch producerLatch = new CountDownLatch(1);
                    kafkaClusterSafe.cluster.produce(
                            topics.get(0).name(),
                            concrete).thenAccept(result -> {
                        producerLatch.countDown();
                    });
                    try {
                        producerLatch.await(10000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ex) {
                        fail(ex.getMessage());
                    }
                    AsyncUtil.eventually(() -> {
                        try {

                            Optional<ConsumedItem<byte[], ConcreteJavaMessageImplementation>> consumed = kafkaClusterSafe.cluster.consume(
                                    topics.get(0).name(),
                                    ConcreteJavaMessageImplementation.class);

                            assertEquals(consumed.get().value().property, concrete.property);
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
