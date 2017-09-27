package com.sbibits.ptrc.tutorials.kafka.advanced;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Used to deserialize Stockprice objects from the Kafka topics
 *
 */
public class StockDeserializer implements Deserializer<StockPrice> {

  @Override
  public StockPrice deserialize(final String topic, final byte[] data) {
    return new StockPrice(new String(data, StandardCharsets.UTF_8));
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

}
