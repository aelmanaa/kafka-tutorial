package com.sbibits.ptrc.tutorials.kafka.advanced;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The stock sends used the Kafka producer. It generates random stock prices for a given StockPrice
 * name. The StockSender is Runnable and runs in its own thread. There is one thread per
 * StockSender. The StockSenger is used to show using KafkaProducer from many threads. The
 * StockSender Delays random time between delayMin and delayMax, then sends a random stockPrice
 * between stockPriceHigh and stockPriceLow. The StockSender imports Kafka Producer, ProducerRecord,
 * RecordMetadata, and StockPrice. It implements Runnable, and can be submitted to ExecutionService
 * (thread pool).
 *
 */
public class StockSender implements Runnable {

  private String topic;
  private int delayMaxMs;
  private int delayMinMs;
  private Producer<String, StockPrice> producer;
  private StockPrice stockPriceLow;
  private StockPrice stockPriceHigh;
  private final Logger logger = LoggerFactory.getLogger(StockSender.class);

  public StockSender(String topic, StockPrice stockPriceHigh, StockPrice stockPriceLow,
      Producer<String, StockPrice> producer, int delayMinMs, int delayMaxMs) {
    this.stockPriceHigh = stockPriceHigh;
    this.stockPriceLow = stockPriceLow;
    this.producer = producer;
    this.delayMinMs = delayMinMs;
    this.delayMaxMs = delayMaxMs;
    this.topic = topic;
  }

  @Override
  /**
   * The StockSender run methods in a forever loop, creates random record, sends the record, waits
   * random time and then repeats
   */
  public void run() {
    final Random random = new Random(System.currentTimeMillis());
    int sentCount = 0;
    while (true) {
      sentCount++;
      final ProducerRecord<String, StockPrice> record = createRandomRecord(random);
      final int delay = randomIntBetween(random, delayMaxMs, delayMinMs);
      try {
        final Future<RecordMetadata> future = producer.send(record);
        if (sentCount % 100 == 0) {
          displayRecordMetaData(record, future);
        }
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        if (Thread.interrupted()) {
          break;
        }
      } catch (ExecutionException e) {
        logger.error("problem sending record to producer", e);
      }
    }

  }

  /**
   * The StockSender createRandomRecord method uses randomIntBetween. The createRandomRecord creates
   * StockPrice and then wraps StockPrice in ProducerRecord.
   * 
   * @param random
   * @return
   */
  private ProducerRecord<String, StockPrice> createRandomRecord(Random random) {
    final int dollarAmount =
        randomIntBetween(random, stockPriceHigh.getDollars(), stockPriceLow.getDollars());

    final int centAmount =
        randomIntBetween(random, stockPriceHigh.getCents(), stockPriceLow.getCents());

    final StockPrice stockPrice =
        new StockPrice(stockPriceHigh.getName(), dollarAmount, centAmount);

    return new ProducerRecord<>(topic, stockPrice.getName(), stockPrice);
  }

  private final int randomIntBetween(final Random random, final int max, final int min) {
    return random.nextInt(max - min + 1) + min;
  }

  /**
   * Every 100 records StockSender displayRecordMetaData method gets called, which prints out record
   * info, and recordMetadata info: key, JSON value, topic, partition, offset, time. The
   * displayRecordMetaData uses the Future from the call to producer.send().
   * 
   * @param record
   * @param future
   * @throws InterruptedException
   * @throws ExecutionException
   */
  private void displayRecordMetaData(final ProducerRecord<String, StockPrice> record,
      final Future<RecordMetadata> future) throws InterruptedException, ExecutionException {
    final RecordMetadata recordMetadata = future.get();
    logger.info(String.format(
        "\n\t\t\tkey=%s, value=%s " + "\n\t\t\tsent to topic=%s part=%d off=%d at time=%s",
        record.key(), record.value().toJson(), recordMetadata.topic(), recordMetadata.partition(),
        recordMetadata.offset(), new Date(recordMetadata.timestamp())));
  }

}
