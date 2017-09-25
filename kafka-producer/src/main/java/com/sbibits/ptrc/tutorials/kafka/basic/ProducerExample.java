package com.sbibits.ptrc.tutorials.kafka.basic;



import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class ProducerExample {
  private final static String TOPIC = "my-example-topic";
  private final static String BOOTSTRAP_SERVER = "localhost:9092,localhost:9093,localhost:9094";

  public static Producer<Long, String> createProducer() {
    Properties props = new Properties();
    setupBootstrapAndSerializers(props);
    setupRetriesInFlightTimeout(props);
    setupBatchingAndCompression(props);

    // Set number of acknowledgments - acks - default is all
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    KafkaProducer<Long, String> producer = new KafkaProducer<>(props);
    ExecutorService executorService = Executors.newFixedThreadPool(1);
    // Run Metrics Producer Reporter which is runnable passing it the producer.
    executorService.submit(new MetricsProducerReporter(producer));
    return producer;

  }

  /**
   * The code sets the retires to 3, and the maximum in-flight requests to 1 to avoid out of order
   * retries. It sets the request timeout to 15 seconds and the retry back-off to 1 second.
   * 
   * @param props
   */

  private static void setupRetriesInFlightTimeout(Properties props) {
    // Only one in-flight messages per Kafka broker connection
    // - max.in.flight.requests.per.connection(default 5)
    // Requests are pipelined to kafka brokers up to this number of maximum requests per broker
    // connection. if you set retry > 0, then you should also set
    // max.in.flight.requests.per.connection to 1, or there is the possibility that a re-tried
    // message could be delivered out of order. You have to decide if out of order message delivery
    // matters for your application.
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

    // Set the number of retries - retries
    props.put(ProducerConfig.RETRIES_CONFIG, 3);

    // Request timeout - request.timeout.ms
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);

    // Only retry after one second
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1_000);

  }

  private static void setupBatchingAndCompression(Properties props) {
    // Linger up to 100ms before sending batch if size not met
    props.put(ProducerConfig.LINGER_MS_CONFIG, 100);

    // Batch up to 64K buffer sizes
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384 * 4);

    // Use snappy compression for batch compression
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

  }

  private static void setupBootstrapAndSerializers(Properties props) {
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

  }

  static void runProducer(final int sendMessageCount) throws Exception {
    final Producer<Long, String> producer = createProducer();
    long time = System.currentTimeMillis();

    try {
      for (long index = time; index < time + sendMessageCount; index++) {
        final ProducerRecord<Long, String> record =
            new ProducerRecord<>(TOPIC, index, "Hello Mom " + index);

        RecordMetadata metadata = producer.send(record).get();

        long elapsedTime = System.currentTimeMillis() - time;
        System.out.printf(
            "sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
            record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);

      }
    } finally {
      producer.flush();
      producer.close();
    }
  }

  static void runProducerAsync(final int sendMessageCount) throws Exception {
    final Producer<Long, String> producer = createProducer();
    long time = System.currentTimeMillis();
    final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

    try {
      for (long index = time; index < time + sendMessageCount; index++) {
        final ProducerRecord<Long, String> record =
            new ProducerRecord<>(TOPIC, index, "Hello Mom " + index);
        producer.send(record, (metadata, exception) -> {
          long elapsedTime = System.currentTimeMillis() - time;
          if (metadata != null) {
            System.out.printf(
                "sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
                record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
          } else {
            exception.printStackTrace();
          }
          countDownLatch.countDown();
        });
      }
      countDownLatch.await(25, TimeUnit.SECONDS);
    } finally {
      producer.flush();
      producer.close();
    }
  }

  public static void main(String... args) throws Exception {
    if (args.length == 0) {
      runProducer(25);
      // runProducerAsync(5);
    } else {
      runProducer(Integer.parseInt(args[0]));
      // runProducerAsync(Integer.parseInt(args[0]));
    }
  }
}
