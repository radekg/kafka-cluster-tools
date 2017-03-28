package com.gruchalski.kafka.java8.exceptions;

public class NoDeserializerException extends Exception {
    public NoDeserializerException(String message) {
        super(message);
    }
}
