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

import com.gruchalski.kafka.scala.KafkaTopicCreateResult;
import com.gruchalski.kafka.scala.SerializerProvider;
import com.gruchalski.kafka.scala.KafkaTopicConfiguration;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class KafkaCluster {

    final private com.gruchalski.kafka.scala.KafkaCluster cluster;

    public KafkaCluster() {
        this(ConfigFactory.load().resolve());
    }

    public KafkaCluster(Config underlying) {
        cluster = new com.gruchalski.kafka.scala.KafkaCluster(
                underlying,
                com.gruchalski.kafka.scala.KafkaCluster.apply$default$1());
    }

    public Optional<KafkaClusterSafe> start() {
        Optional<com.gruchalski.kafka.scala.KafkaClusterSafe> maybeSafe = Optional.ofNullable(cluster.start().get());
        if (maybeSafe.isPresent()) {
            return Optional.of(new KafkaClusterSafe(this, maybeSafe.get().configuration()));
        }
        return Optional.ofNullable(null);
    }

    public void stop() {
        cluster.stop();
    }

    public CompletableFuture<List<KafkaTopicCreateResult>> withTopics(List<KafkaTopicConfiguration> topics) {
        scala.collection.immutable.List<scala.concurrent.Future<KafkaTopicCreateResult>> result =
                cluster.withTopics(scala.collection.JavaConverters.asScalaIterator(topics.iterator()).toList());
        ArrayList<CompletableFuture<KafkaTopicCreateResult>> jresult = new ArrayList<>();
        CompletableFuture<KafkaTopicCreateResult> arrItems[] = new CompletableFuture[result.size()];
        for (int i=0; i<result.length(); i++) {
            jresult.add(scala.compat.java8.FutureConverters.toJava(result.apply(i)).toCompletableFuture());
            arrItems[i] = scala.compat.java8.FutureConverters.toJava(result.apply(i)).toCompletableFuture();
        }
        return CompletableFuture.allOf(arrItems).thenApply(v -> Arrays.stream(arrItems).map(CompletableFuture::join).collect(Collectors.toList()));
    }

    public <T> Optional<CompletableFuture<RecordMetadata>> produce(String topic, SerializerProvider<T> value, ProducerCallback callback) {
        Optional<scala.concurrent.Future<RecordMetadata>> optional = Optional.ofNullable(cluster.produce(topic, value, callback.callback).get());
        if (optional.isPresent()) {
            return Optional.ofNullable(scala.compat.java8.FutureConverters.toJava(optional.get()).toCompletableFuture());
        } else {
            return Optional.ofNullable(null);
        }
    }
}
