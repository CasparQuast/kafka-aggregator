package org.example;

import org.apache.kafka.common.serialization.Serde;

public class SerdeFactory {

    @SuppressWarnings("unchecked")
    public static <T> Serde<T> getSerde(String serdeClassName) {
        try {
            Class<?> clazz = Class.forName(serdeClassName);
            return (Serde<T>) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Serde instance for: " + serdeClassName, e);
        }
    }
}
