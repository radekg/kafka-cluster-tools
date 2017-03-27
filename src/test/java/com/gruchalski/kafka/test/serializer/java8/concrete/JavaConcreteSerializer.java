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

package com.gruchalski.kafka.test.serializer.java8.concrete;

import org.apache.kafka.common.serialization.Serializer;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.util.Map;

public class JavaConcreteSerializer<T extends JavaConcreteMessageType> implements Serializer<T> {
    public void configure(Map<String, ?> var1, boolean var2) {}
    public void close() {}
    public byte[] serialize(String topic, T input) {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        if (input instanceof ConcreteJavaMessageImplementation) {
            ConcreteJavaMessageImplementation item = (ConcreteJavaMessageImplementation)input;
            try {
                packer.packInt(item.id())
                        .packInt(1)
                        // actual data:
                        .packString(item.property);
                return packer.toByteArray();
            } catch (IOException ex) {
                return null;
            }
        }

        return null;
    }
}
