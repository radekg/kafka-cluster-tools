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

import com.gruchalski.kafka.java8.exceptions.NoDeserializerException;
import com.gruchalski.kafka.java8.exceptions.NoSerializerException;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serde registry.
 */
public class SerdeRegistry {

    private ConcurrentHashMap<Class<?>, Serializer<?>> defaultSerializers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Class<?>, Deserializer<?>> defaultDeserializers = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Class<?>, Serializer<?>> registeredSerializers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Class<?>, Deserializer<?>> registeredDeserializers = new ConcurrentHashMap<>();

    private static Object lock = 1;
    private static SerdeRegistry instance = null;

    /**
     * Get an instance of the serde registry.
     * @return
     */
    public static SerdeRegistry getInstance() {
        synchronized (lock) {
            if (instance == null) {
                instance = new SerdeRegistry();
            }
        }
        return instance;
    }

    /**
     * Construct a serde registry and set defaults.
     */
    private SerdeRegistry() {
        defaultSerializers.putIfAbsent(byte[].class, new ByteArraySerializer());
        defaultSerializers.putIfAbsent(ByteBuffer.class, new ByteBufferSerializer());
        defaultSerializers.putIfAbsent(Bytes.class, new BytesSerializer());
        defaultSerializers.putIfAbsent(Double.class, new DoubleSerializer());
        defaultSerializers.putIfAbsent(Integer.class, new IntegerSerializer());
        defaultSerializers.putIfAbsent(Long.class, new LongSerializer());
        defaultSerializers.putIfAbsent(String.class, new StringSerializer());

        defaultDeserializers.putIfAbsent(byte[].class, new ByteArrayDeserializer());
        defaultDeserializers.putIfAbsent(ByteBuffer.class, new ByteBufferDeserializer());
        defaultDeserializers.putIfAbsent(Bytes.class, new BytesDeserializer());
        defaultDeserializers.putIfAbsent(Double.class, new DoubleDeserializer());
        defaultDeserializers.putIfAbsent(Integer.class, new IntegerDeserializer());
        defaultDeserializers.putIfAbsent(Long.class, new LongDeserializer());
        defaultDeserializers.putIfAbsent(String.class, new StringDeserializer());
    }

    /**
     * Register a serializer for the type.
     * @param clazz type class
     * @param serializer serializer instance
     * @param <K> type of the class
     */
    public <K> void registerSerializer(Class<K> clazz, Serializer<K> serializer) {
        registeredSerializers.putIfAbsent(clazz, serializer);
    }

    /**
     * Register a deserializer for the type.
     * @param clazz type class
     * @param deserializer deserializer instance
     * @param <K> type of the class
     */
    public <K> void registerDeserializer(Class<K> clazz, Deserializer<K> deserializer) {
        registeredDeserializers.putIfAbsent(clazz, deserializer);
    }

    /**
     * Register a serializer / deserializer pair for the type.
     * @param clazz type class
     * @param serializer serializer instance
     * @param serializer deserializer instance
     * @param <K> type of the class
     */
    public <K> void registerPair(Class<K> clazz, Serializer<K> serializer, Deserializer<K> deserializer) {
        registeredSerializers.putIfAbsent(clazz, serializer);
        registeredDeserializers.putIfAbsent(clazz, deserializer);
    }

    /**
     * Get a serializer instance for the type.
     * @param item item for which type to get the serializer for
     * @param <K> type of the item
     * @return serializer for the type
     * @throws NoSerializerException serializer for the type has not been found
     */
    public <K> Serializer<K> getSerializerFor(K item)
    throws NoSerializerException {
        // default to byte[]
        if (item == null) {
            return (Serializer<K>) defaultSerializers.get(byte[].class);
        }
        if (defaultSerializers.containsKey(item.getClass())) {
            return (Serializer<K>)defaultSerializers.get(item.getClass());
        }
        if (registeredSerializers.containsKey(item.getClass())) {
            return (Serializer<K>)registeredSerializers.get(item.getClass());
        }
        throw new NoSerializerException("No serializer for " + item.getClass() + " registered.");
    }

    /**
     * Get a deserializer for the type.
     * @param clazz class type of the deserializer to get
     * @param <K> class type
     * @return deserializer for the type
     * @throws NoDeserializerException deserializer for the type has not been found
     */
    public <K> Deserializer<K> getDeserializerFor(Class<K> clazz)
            throws NoDeserializerException {
        if (defaultDeserializers.containsKey(clazz)) {
            return (Deserializer<K>)defaultDeserializers.get(clazz);
        }
        if (registeredDeserializers.containsKey(clazz)) {
            return (Deserializer<K>)registeredDeserializers.get(clazz);
        }
        throw new NoDeserializerException("No deserializer for " + clazz + " registered.");
    }

}
