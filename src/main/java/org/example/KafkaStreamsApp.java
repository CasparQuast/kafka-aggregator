package org.example;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaStreamsApp {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApp.class);

    public static void main(String[] args) {
        Config config = new Config();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "binance-trade-aggregator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrap());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TradeSerde.class.getName());

        StreamsBuilder builder = new StreamsBuilder();

        String inputTopic = config.getKafkaTopic();
        String outputTopic = config.getKafkaAggregatedTopic();

        var stringSerde = Serdes.String();
        var tradeSerde = new TradeSerde();

        KStream<String, String> inputStream = builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde));

        KStream<String, Trade> tradesStream = inputStream
            .flatMapValues(value -> {
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    Trade trade = mapper.readValue(value, Trade.class);
                    if (!"trade".equals(trade.getEventType())) {
                        logger.info("Ignoring non-trade event: {}", trade.getEventType());
                        return java.util.Collections.emptyList();
                    }
                    return java.util.Collections.singletonList(trade);
                } catch (Exception e) {
                    logger.error("Invalid message format: {}", value, e);
                    return java.util.Collections.emptyList();
                }
            })
            .filter((key, trade) -> trade != null)
            .selectKey((key, trade) -> trade.getSymbol());

        TimeWindows window = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(config.getWindowSizeMinutes()));

        KTable<Windowed<String>, Long> tradeCount = tradesStream
            .groupByKey(Grouped.with(stringSerde, tradeSerde))
            .windowedBy(window)
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("trade-count-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long()));

        tradeCount
            .toStream()
            .map((windowedKey, totalCount) -> KeyValue.pair(
                windowedKey.key(),
                formatAsJson(windowedKey, totalCount)
            ))
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Kafka Streams...");
            streams.close(Duration.ofSeconds(10));
        }));

        try {
            streams.start();
            logger.info("Kafka Streams application started successfully.");
        } catch (Throwable e) {
            logger.error("Failed to start Kafka Streams", e);
        }
    }

    private static String formatAsJson(Windowed<String> windowedKey, Long totalCount) {
        try {
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm").withZone(ZoneId.systemDefault());

            Instant windowStart = Instant.ofEpochMilli(windowedKey.window().start());
            Instant windowEnd = Instant.ofEpochMilli(windowedKey.window().end());

            // Format the data
            Map<String, Object> output = new HashMap<>();
            output.put("day", dateFormatter.format(windowStart));
            output.put("time_from", timeFormatter.format(windowStart));
            output.put("time_to", timeFormatter.format(windowEnd));
            output.put("totalTrades", totalCount);

            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(output);
        } catch (Exception e) {
            logger.error("Failed to format JSON output", e);
            return "{}";
        }
    }
}
