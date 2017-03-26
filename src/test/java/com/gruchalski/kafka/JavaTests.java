package com.gruchalski.kafka;

import com.gruchalski.kafka.java8compat.KafkaCluster;
import com.gruchalski.kafka.java8compat.KafkaClusterSafe;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class JavaTests extends TestCase {

    public void testClusterSetup() {
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

                CountDownLatch latch = new CountDownLatch(topics.size());
                CompletableFuture<List<KafkaTopicCreateResult>> topicCreateStatuses = kafkaClusterSafe.cluster.withTopics(topics);
                topicCreateStatuses.thenAccept(results -> {
                    for (KafkaTopicCreateResult result : results) {
                        if (result.status().toString().equals("exists")) {
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

                kafkaClusterSafe.cluster.stop();
            }
        });
    }

}
