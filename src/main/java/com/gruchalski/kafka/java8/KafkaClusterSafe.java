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

import com.gruchalski.kafka.scala.Configuration;

/**
 * Representation of a successfully started cluster. A cluster instance wrapped in this object is safe to use
 * in the program. Returned by {@link com.gruchalski.kafka.java8.KafkaCluster#start()}.
 */
public class KafkaClusterSafe {

    /**
     * a safe to use cluster
     */
    public final KafkaCluster cluster;

    /**
     * configuration used by the cluster
     */
    public final Configuration configuration;

    /**
     * Create an instance of safe to use Kafka cluster.
     * @param cluster a safe to use Kafka cluster
     * @param configuration a safe to use configuration
     */
    public KafkaClusterSafe(KafkaCluster cluster, Configuration configuration) {
        this.cluster = cluster;
        this.configuration = configuration;
    }

}
