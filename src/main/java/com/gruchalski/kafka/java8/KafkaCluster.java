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

package com.gruchalski.kafka.java8;

import com.gruchalski.kafka.java8.compat.ScalaCompat;
import com.gruchalski.kafka.scala.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Java {@see com.gruchalski.kafka.scala.KafkaCluster} representation.
 */
public class KafkaCluster {

    final private com.gruchalski.kafka.scala.KafkaCluster cluster;

    /**
     * Create new KafkaCluster with default configuration.
     */
    public KafkaCluster() {
        this(ConfigFactory.load().resolve());
    }

    /**
     * Create KafkaCluster with a specific configuration.
     * @param underlying Typesafe configuration object
     */
    public KafkaCluster(Config underlying) {
        cluster = new com.gruchalski.kafka.scala.KafkaCluster(
                underlying,
                com.gruchalski.kafka.scala.KafkaCluster.apply$default$1());
    }

    /**
     * Start the cluster. Method is idempotent, subsequent calls return currently running cluster, if cluster is
     * running.<br/>
     * The method will start a ZooKeeper ensemble to be used by the Kafka cluster.
     * @return safe to use cluster data
     */
    public Optional<KafkaClusterSafe> start() {
        Optional<com.gruchalski.kafka.scala.KafkaClusterSafe> maybeSafe = ScalaCompat.fromScala(cluster.start());
        if (maybeSafe.isPresent()) {
            return Optional.of(new KafkaClusterSafe(this, maybeSafe.get().configuration()));
        }
        return Optional.ofNullable(null);
    }

    /**
     * Stop the cluster, if running.
     */
    public void stop() {
        cluster.stop();
    }

    /**
     * Create topics and wait for the topics to be ready to use. For every topic requestd, if the topic is not found
     * in Kafka within the <code>com.gruchalski.kafka.topic-wait-for-create-success-timeout-ms</code> timeout,
     * the topic will be considered as not created.
     * @param topicConfigs topic configurations
     * @return list of create statuses futures
     */
    public CompletableFuture<List<KafkaTopicCreateResult>> withTopics(List<KafkaTopicConfiguration> topicConfigs) {
        CompletableFuture<KafkaTopicCreateResult> seq[] = ScalaCompat.fromScala(
                cluster.withTopics(
                        scala.collection.JavaConverters.asScalaIterator(
                                topicConfigs.iterator()
                        ).toList()));
        return CompletableFuture.allOf(seq).thenApply(v -> Arrays.stream(seq).map(CompletableFuture::join).collect(Collectors.toList()));
    }

    /**
     * Produce a message of a given type. If the producer for the given type does not exist, it will be created.
     * @param topic topic to send the message to
     * @param value value to send
     * @param callback callback handling metadata or error, the callback is used to return a scala future
     * @tparam <T> type of the value to send
     * @return the metadata / error future
     */
    public <T> Optional<CompletableFuture<RecordMetadata>> produce(String topic, SerializerProvider<T> value, com.gruchalski.kafka.java8.ProducerCallback callback)
    throws Throwable {
        // left is an error:
        scala.util.Try<scala.Option<scala.concurrent.Future<RecordMetadata>>> _try = cluster.produce(topic, value, callback.callback);
        scala.util.Either<Throwable, scala.Option<scala.concurrent.Future<RecordMetadata>>> _either = _try.toEither();
        if (_either.isLeft()) {
            throw _either.left().get();
        }
        // well, else if right...
        Optional<scala.concurrent.Future<RecordMetadata>> optional = ScalaCompat.fromScala(_either.right().get());
        if (optional.isPresent()) {
            return Optional.ofNullable(ScalaCompat.fromScala(optional.get()));
        } else {
            return Optional.ofNullable(null);
        }
    }

    /**
     * Consume a Kafka message from a given topic using given deserializer.
     * @param topic topic to consume from
     * @param deserializer deserializer to handle the type of the message
     * @tparam <T> type of the message to consume
     * @return a consumed object, if available at the time of the call
     */
    public <T extends DeserializerProvider<?>> Optional<ConsumedItem<T>> consume(String topic, Deserializer<T> deserializer)
    throws Throwable {
        scala.util.Try<scala.Option<ConsumedItem<T>>> _try = cluster.consume(topic, deserializer);
        scala.util.Either<Throwable, scala.Option<ConsumedItem<T>>> _either = _try.toEither();
        if (_either.isLeft()) {
            throw _either.left().get();
        }
        return ScalaCompat.fromScala(_either.right().get());
    }
}
