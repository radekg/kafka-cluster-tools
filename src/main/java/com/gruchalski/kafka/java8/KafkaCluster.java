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
import com.gruchalski.kafka.scala.ConsumedItem;
import com.gruchalski.kafka.scala.KafkaTopicConfiguration;
import com.gruchalski.kafka.scala.KafkaTopicCreateResult;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Java {@see com.gruchalski.kafka.scala.KafkaCluster} representation.
 */
public class KafkaCluster {

    final private com.gruchalski.kafka.scala.KafkaCluster cluster;

    final private SerdeRegistry serdeRegistry = SerdeRegistry.getInstance();

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
     * For a list of topics, return a list of expected successful create statuses.
     * @param topicConfigs topic configurations
     * @return list of expected successful statuses
     */
    public List<KafkaTopicCreateResult> expectedResultsForTopicCreation(List<KafkaTopicConfiguration> topicConfigs) {
        List<KafkaTopicCreateResult> list = new ArrayList<>();
        Iterator<KafkaTopicCreateResult> javaIter = scala.collection.JavaConverters.asJavaIterator(
          cluster.expectedResultsForTopicCreation(
            scala.collection.JavaConverters.asScalaIterator(
              topicConfigs.iterator()
            ).toList()
          ).toIterator()
        );
        javaIter.forEachRemaining(list::add);
        return list;
    }

    /**
     * Produce a message of a given type. If the producer for the given type does not exist, it will be created.
     * @param topic topic to send the message to
     * @param value value to send
     * @param <V> type of the value to send
     * @return the metadata / error future
     */
    public <V> CompletableFuture<RecordMetadata> produce(String topic, V value)
    throws Throwable {
        return produce(topic, null, value);
    }

    /**
     * Produce a message of a given type. If the producer for the given type does not exist, it will be created.
     * @param topic topic to send the message to
     * @param key key to send
     * @param value value to send
     * @param <K> type of the key to send
     * @param <V> type of the value to send
     * @return the metadata / error future
     */
    public <K, V> CompletableFuture<RecordMetadata> produce(String topic, K key, V value)
            throws Throwable {
        // left is an error:
        scala.util.Try<scala.concurrent.Future<RecordMetadata>> _try = cluster.produce(
                topic,
                scala.Option.apply(key),
                value, serdeRegistry.getSerializerFor(key),
                serdeRegistry.getSerializerFor(value));
        scala.util.Either<Throwable, scala.concurrent.Future<RecordMetadata>> _either = _try.toEither();
        if (_either.isLeft()) {
            throw _either.left().get();
        }
        // well, else if right...
        return ScalaCompat.fromScala(_either.right().get());
    }

    /**
     * Consume a Kafka message from a given topic using given deserializer.
     * @param topic topic to consume from
     * @param valueDeserializerTypeClass class of the value type to deserialize
     * @param <V> type of the value to consume
     * @return a consumed object, if available at the time of the call
     */
    public <V> Optional<ConsumedItem<byte[], V>> consume(String topic, Class<V> valueDeserializerTypeClass)
    throws Throwable {
        return consume(topic, byte[].class, valueDeserializerTypeClass);
    }

    /**
     * Consume a Kafka message from a given topic using given deserializer.
     * @param topic topic to consume from
     * @param keyDeserializerTypeClass class of the key type to deserialize
     * @param valueDeserializerTypeClass class of the value type to deserialize
     * @param <K> type of the key to consume
     * @param <V> type of the value to consume
     * @return a consumed object, if available at the time of the call
     */
    public <K, V> Optional<ConsumedItem<K, V>> consume(String topic, Class<K> keyDeserializerTypeClass, Class<V> valueDeserializerTypeClass)
            throws Throwable {
        scala.util.Try<scala.Option<ConsumedItem<K, V>>> _try = cluster.consume(
                topic,
                serdeRegistry.getDeserializerFor(keyDeserializerTypeClass),
                serdeRegistry.getDeserializerFor(valueDeserializerTypeClass));
        scala.util.Either<Throwable, scala.Option<ConsumedItem<K, V>>> _either = _try.toEither();
        if (_either.isLeft()) {
            throw _either.left().get();
        }
        return ScalaCompat.fromScala(_either.right().get());
    }
}
