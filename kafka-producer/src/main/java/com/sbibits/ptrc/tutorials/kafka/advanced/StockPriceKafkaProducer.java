package com.sbibits.ptrc.tutorials.kafka.advanced;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.advantageous.boon.core.Lists;

/**
 * StockPriceKafkaProducer import classes and sets up a logger. It has a createProducer method to
 * create a KafkaProducer instance. It has a setupBootstrapAndSerializers to initialize bootstrap
 * servers, client id, key serializer and custom serializer (StockPriceSerializer). It has a main()
 * method that creates the producer, creates a StockSender list passing each instance the producer,
 * and it creates a thread pool, so every stock sender gets it own thread, and then it runs each
 * stockSender in its own thread using the thread pool.
 *
 */
public class StockPriceKafkaProducer {
  private static final Logger logger = LoggerFactory.getLogger(StockPriceKafkaProducer.class);

  public static void main(String... args) throws Exception {
    // Create kafka producer
    final Producer<String, StockPrice> producer = createProducer();
    // Create StockSender list
    final List<StockSender> stockSenders = getStockSenderList(producer);
    // Create a thread pool so every stock sender gets it own.
    // increase by 1 to fit metrics.

    final ExecutorService executorService = Executors.newFixedThreadPool(stockSenders.size() + 1);

    // Run Metrics Producer reporter which is runnable passing it the producer.
    executorService.submit(new MetricsProducerReporter(producer));

    // Run each stock sender in its own thread.
    stockSenders.forEach(executorService::submit);

    // Register nice shutdown of thread pool, then flush and close producer
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      executorService.shutdown();
      try {
        executorService.awaitTermination(200, TimeUnit.MILLISECONDS);
        logger.info("Flushing and closing producer");
        producer.flush();
        producer.close(10_000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger.warn("shutting down", e);
      }
    }));
  }

  private static List<StockSender> getStockSenderList(Producer<String, StockPrice> producer) {
    return Lists.list(
        new StockSender(StockAppConstants.TOPIC, new StockPrice("IBM", 100, 99),
            new StockPrice("IBM", 50, 10), producer, 1, 3),
        new StockSender(StockAppConstants.TOPIC, new StockPrice("SUN", 100, 99),
            new StockPrice("IBM", 50, 10), producer, 1, 3),
        new StockSender(StockAppConstants.TOPIC, new StockPrice("APPLE", 100, 99),
            new StockPrice("APPLE", 50, 10), producer, 1, 3),
        new StockSender(StockAppConstants.TOPIC, new StockPrice("MST", 100, 99),
            new StockPrice("MST", 50, 10), producer, 1, 3),
        new StockSender(StockAppConstants.TOPIC, new StockPrice("FFF", 100, 99),
            new StockPrice("FFF", 50, 10), producer, 1, 3),
        new StockSender(StockAppConstants.TOPIC, new StockPrice("FCB", 100, 99),
            new StockPrice("FCB", 50, 10), producer, 1, 3),
        new StockSender(StockAppConstants.TOPIC, new StockPrice("MAN", 100, 99),
            new StockPrice("MAN", 50, 10), producer, 1, 3),
        new StockSender(StockAppConstants.TOPIC, new StockPrice("TOT", 100, 99),
            new StockPrice("TOT", 50, 10), producer, 1, 3),
        new StockSender(StockAppConstants.TOPIC, new StockPrice("KFC", 100, 99),
            new StockPrice("KFC", 50, 10), producer, 1, 3),
        new StockSender(StockAppConstants.TOPIC, new StockPrice("MIU", 100, 99),
            new StockPrice("MIU", 50, 10), producer, 1, 3));
  }

  private static Producer<String, StockPrice> createProducer() {
    final Properties props = new Properties();
    setupBootstrapAndSerializers(props);
    return new KafkaProducer<>(props);
  }

  private static void setupBootstrapAndSerializers(Properties props) {
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, StockAppConstants.BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "StockPriceKafkaProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Custom Serializer - config "value.serializer"
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StockPriceSerializer.class.getName());
  }
}
