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

package com.gruchalski.kafka.serializer.java8;

import com.gruchalski.kafka.scala.DeserializerProvider;
import com.gruchalski.kafka.scala.SerializerProvider;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class ConcreteJavaMessageImplementation
        extends JavaConcreteMessageType
        implements SerializerProvider<ConcreteJavaMessageImplementation>, DeserializerProvider<ConcreteJavaMessageImplementation> {

    public final String property;

    public ConcreteJavaMessageImplementation() {
        this("undefined value");
    }

    public ConcreteJavaMessageImplementation(String property) {
        this.property = property;
    }

    public Serializer<ConcreteJavaMessageImplementation> serializer() {
        return new JavaConcreteSerializer<ConcreteJavaMessageImplementation>();
    }

    public Deserializer<ConcreteJavaMessageImplementation> deserializer() {
        return new JavaConcreteDeserializer<ConcreteJavaMessageImplementation>();
    }
}
