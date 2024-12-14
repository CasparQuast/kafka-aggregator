package org.example;

public class Config {
    private final String binanceUrl;
    private final String kafkaBootstrap;
    private final String kafkaTopic;
    private final String kafkaAggregatedTopic;
    private final int windowSizeMinutes;

    public Config() {
        this.binanceUrl = System.getenv().getOrDefault(
                "BINANCE_WS_URL",
                "wss://stream.binance.com:9443/ws/btcusdt@trade"
        );
        this.kafkaBootstrap = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS",
                "138.201.203.245:9092"
        );
        this.kafkaTopic = System.getenv().getOrDefault(
                "KAFKA_TARGET_TOPIC",
                "binance-trades"
        );
        this.kafkaAggregatedTopic = System.getenv().getOrDefault(
                "KAFKA_AGGREGATED_TOPIC",
                "binance-trades-aggregated"
        );
        this.windowSizeMinutes = Integer.parseInt(
                System.getenv().getOrDefault("WINDOW_SIZE_MINUTES", "10")
        );
    }

    public String getBinanceUrl() { return binanceUrl; }
    public String getKafkaBootstrap() { return kafkaBootstrap; }
    public String getKafkaTopic() { return kafkaTopic; }
    public String getKafkaAggregatedTopic() { return kafkaAggregatedTopic; }
    public int getWindowSizeMinutes() { return windowSizeMinutes; }

    // Optional: Add validation logic in the constructor or separate method
    // to ensure configurations are valid.
}

