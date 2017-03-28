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
