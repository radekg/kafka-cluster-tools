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

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.CompletableFuture;

public class ProducerCallback {
    public final com.gruchalski.kafka.scala.ProducerCallback callback;
    public ProducerCallback() {
        callback = new com.gruchalski.kafka.scala.ProducerCallback();
    }
    public CompletableFuture<RecordMetadata> result() {
        return scala.compat.java8.FutureConverters.toJava(callback.result()).toCompletableFuture();
    }
}
