package org.example;


import com.fasterxml.jackson.annotation.JsonProperty;

public class Trade {
    @JsonProperty("e")
    private String eventType;

    @JsonProperty("E")
    private long eventTime;

    @JsonProperty("s")
    private String symbol;

    @JsonProperty("t")
    private long tradeId;

    @JsonProperty("p")
    private double price; // Changed from String to double

    @JsonProperty("q")
    private double quantity; // Changed from String to double

    @JsonProperty("T")
    private long tradeTime;

    @JsonProperty("m")
    private boolean isMarketMaker;

    @JsonProperty("M")
    private boolean ignore;

    public String getSymbol() {
        return symbol;
    }
    
    public double getQuantity() {
        return quantity;
    }

    public String getEventType() {
        return eventType;
    }
    

    // Getters and Setters
}


