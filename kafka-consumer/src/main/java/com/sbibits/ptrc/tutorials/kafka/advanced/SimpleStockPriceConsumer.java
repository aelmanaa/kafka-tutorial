package com.sbibits.ptrc.tutorials.kafka.advanced;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleStockPriceConsumer {
  private static final Logger logger = LoggerFactory.getLogger(SimpleStockPriceConsumer.class);

  public static void main(String... args) throws Exception {
    runConsumer();
  }

  /**
   * This method drains Kafka topic. It creates map of current stock prices and Calls
   * displayRecordsStatsAndStocks()
   * 
   * @throws InterruptedException
   */
  static void runConsumer() throws InterruptedException {
    final Consumer<String, StockPrice> consumer = createConsumer();
    final Map<String, StockPrice> map = new HashMap<>();
    try {
      final int giveUp = 1000;
      int noRecordsCount = 0;
      int readCount = 0;
      while (true) {
        final ConsumerRecords<String, StockPrice> consumerRecords = consumer.poll(1000);
        if (consumerRecords.count() == 0) {
          noRecordsCount++;
          if (noRecordsCount > giveUp)
            break;
          else
            continue;
        }
        readCount++;
        consumerRecords.forEach(record -> {
          map.put(record.key(), record.value());
        });
        if (readCount % 100 == 0) {
          displayRecordsStatsAndStocks(map, consumerRecords);
        }
        consumer.commitAsync();
      }
    } finally {
      consumer.close();
    }
    logger.info("DONE");
  }

  private static Consumer<String, StockPrice> createConsumer() {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, StockAppConstants.BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    // Custom Deserializer
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StockDeserializer.class.getName());
    // We define a maximum of 500 messages to be returned per poll
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
    // heartbeat intervals
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
    // session timeout
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
    // Create the consumer using props.
    final Consumer<String, StockPrice> consumer = new KafkaConsumer<>(props);
    // Subscribe to the topic.
    consumer.subscribe(Collections.singletonList(StockAppConstants.TOPIC));
    return consumer;
  }

  /**
   * Prints out size of each partition read and total record count. Then it prints out each stock at
   * its current price
   * 
   * @param stockPriceMap
   * @param consumerRecords
   */
  private static void displayRecordsStatsAndStocks(final Map<String, StockPrice> stockPriceMap,
      final ConsumerRecords<String, StockPrice> consumerRecords) {
    logger.debug("New ConsumerRecords par count %d count %d\n", consumerRecords.partitions().size(),
        consumerRecords.count());
    stockPriceMap.forEach((s, stockPrice) -> System.out.printf("ticker %s price %d.%d \n",
        stockPrice.getName(), stockPrice.getDollars(), stockPrice.getCents()));
    logger.debug("");
  }
}
