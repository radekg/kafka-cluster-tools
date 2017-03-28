package com.gruchalski.kafka.java8;

import com.gruchalski.kafka.java8.exceptions.NoDeserializerException;
import com.gruchalski.kafka.java8.exceptions.NoSerializerException;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class SerdeRegistry {

    private ConcurrentHashMap<Class<?>, Serializer<?>> defaultSerializers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Class<?>, Deserializer<?>> defaultDeserializers = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Class<?>, Serializer<?>> registeredSerializers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Class<?>, Deserializer<?>> registeredDeserializers = new ConcurrentHashMap<>();

    private static Object lock = 1;
    private static SerdeRegistry instance = null;

    public static SerdeRegistry getInstance() {
        synchronized (lock) {
            if (instance == null) {
                instance = new SerdeRegistry();
            }
        }
        return instance;
    }

    private SerdeRegistry() {
        defaultSerializers.putIfAbsent(ByteBuffer.class, new ByteBufferSerializer());
        defaultSerializers.putIfAbsent(Bytes.class, new BytesSerializer());
        defaultSerializers.putIfAbsent(Double.class, new DoubleSerializer());
        defaultSerializers.putIfAbsent(Integer.class, new IntegerSerializer());
        defaultSerializers.putIfAbsent(Long.class, new LongSerializer());
        defaultSerializers.putIfAbsent(String.class, new StringSerializer());
        defaultDeserializers.putIfAbsent(ByteBuffer.class, new ByteBufferDeserializer());
        defaultDeserializers.putIfAbsent(Bytes.class, new BytesDeserializer());
        defaultDeserializers.putIfAbsent(Double.class, new DoubleDeserializer());
        defaultDeserializers.putIfAbsent(Integer.class, new IntegerDeserializer());
        defaultDeserializers.putIfAbsent(Long.class, new LongDeserializer());
        defaultDeserializers.putIfAbsent(String.class, new StringDeserializer());
    }

    public <K> void registerSerializer(Class<K> clazz, Serializer<K> serializer) {
        registeredSerializers.putIfAbsent(clazz, serializer);
    }
    public <K> void registerDeserializer(Class<K> clazz, Deserializer<K> deserializer) {
        registeredDeserializers.putIfAbsent(clazz, deserializer);
    }

    public <K> void registerPair(Class<K> clazz, Serializer<K> serializer, Deserializer<K> deserializer) {
        registeredSerializers.putIfAbsent(clazz, serializer);
        registeredDeserializers.putIfAbsent(clazz, deserializer);
    }

    public <K> Serializer<K> getSerializerFor(K item)
    throws NoSerializerException {
        if (defaultSerializers.containsKey(item.getClass())) {
            return (Serializer<K>)defaultSerializers.get(item.getClass());
        }
        if (registeredSerializers.containsKey(item.getClass())) {
            return (Serializer<K>)registeredSerializers.get(item.getClass());
        }
        throw new NoSerializerException("No serializer for " + item.getClass() + " registered.");
    }

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
