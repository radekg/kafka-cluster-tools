package com.gruchalski.kafka.java8compat;

import com.gruchalski.kafka.Configuration;

public class KafkaClusterSafe {

    public final KafkaCluster cluster;
    public final Configuration configuration;

    public KafkaClusterSafe(KafkaCluster cluster, Configuration configuration) {
        this.cluster = cluster;
        this.configuration = configuration;
    }

}
