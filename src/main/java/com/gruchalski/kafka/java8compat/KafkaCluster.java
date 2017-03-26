package com.gruchalski.kafka.java8compat;

import com.gruchalski.kafka.KafkaTopicConfiguration;
import com.gruchalski.kafka.KafkaTopicCreateResult;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import javaslang.collection.Stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class KafkaCluster {

    final private com.gruchalski.kafka.KafkaCluster cluster;

    public KafkaCluster() {
        this(ConfigFactory.load().resolve());
    }

    public KafkaCluster(Config underlying) {
        cluster = new com.gruchalski.kafka.KafkaCluster(
                underlying,
                com.gruchalski.kafka.KafkaCluster.apply$default$1());
    }

    public Optional<KafkaClusterSafe> start() {
        Optional<com.gruchalski.kafka.KafkaClusterSafe> maybeSafe = Optional.ofNullable(cluster.start().get());
        if (maybeSafe.isPresent()) {
            return Optional.of(new KafkaClusterSafe(this, maybeSafe.get().configuration()));
        }
        return Optional.ofNullable(null);
    }

    public void stop() {
        cluster.stop();
    }

    public CompletableFuture<List<KafkaTopicCreateResult>> withTopics(List<com.gruchalski.kafka.KafkaTopicConfiguration> topics) {
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
}
