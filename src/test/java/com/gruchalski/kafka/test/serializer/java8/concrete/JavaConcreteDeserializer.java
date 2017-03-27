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

import org.apache.kafka.common.serialization.Deserializer;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.util.Map;

public class JavaConcreteDeserializer<T extends JavaConcreteMessageType> implements Deserializer<T> {
    public void configure(Map<String, ?> var1, boolean var2) {}
    public void close() {}
    public T deserialize(String topic, byte[] data) {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data);
        try {
            int version = unpacker.unpackInt();
            int type = unpacker.unpackInt();
            if (type == 1) {
                return (T)new ConcreteJavaMessageImplementation(
                        unpacker.unpackString()
                );
            }
        } catch (IOException ex) {
            return null;
        } finally {
            try { unpacker.close(); } catch (IOException ex) {
                // who cares...
            }
        }
        return null;
    }
}
