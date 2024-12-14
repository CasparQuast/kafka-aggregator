package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.example.Trade;

import java.util.Map;

public class TradeSerde implements Serde<Trade> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Serializer<Trade> serializer = new Serializer<>() {
        @Override
        public byte[] serialize(String topic, Trade data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing Trade object", e);
            }
        }
    };

    private final Deserializer<Trade> deserializer = new Deserializer<>() {
        @Override
        public Trade deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(data, Trade.class);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing Trade object", e);
            }
        }
    };

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to configure
    }

    @Override
    public Serializer<Trade> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Trade> deserializer() {
        return deserializer;
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
