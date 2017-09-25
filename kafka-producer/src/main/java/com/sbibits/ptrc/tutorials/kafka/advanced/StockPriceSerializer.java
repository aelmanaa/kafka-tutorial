package com.sbibits.ptrc.tutorials.kafka.advanced;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Converts a StockPrice into a byte array
 *
 */
public class StockPriceSerializer implements Serializer<StockPrice> {

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public byte[] serialize(String topic, StockPrice data) {
    return data.toJson().getBytes(StandardCharsets.UTF_8);
  }

}
