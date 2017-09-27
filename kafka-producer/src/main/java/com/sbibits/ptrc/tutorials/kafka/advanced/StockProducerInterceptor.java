package com.sbibits.ptrc.tutorials.kafka.advanced;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StockProducerInterceptor implements ProducerInterceptor {

  private final Logger logger = LoggerFactory.getLogger(StockProducerInterceptor.class);
  private int onSendCount;
  private int onAckCount;

  /**
   * 
   * The StockProducerInterceptor overrides the onSend method and increments onSendCount. Every 100
   * onSendCount, we print out record data.
   *
   *
   */
  @Override
  public ProducerRecord onSend(final ProducerRecord record) {
    onSendCount++;
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("onSend topic=%s key=%s value=%s %d \n", record.topic(),
          record.key(), record.value().toString(), record.partition()));
    } else {
      if (onSendCount % 100 == 0) {
        logger.info(String.format("onSend topic=%s key=%s value=%s %d \n", record.topic(),
            record.key(), record.value().toString(), record.partition()));
      }
    }
    return record;
  }


  @Override
  public void configure(Map<String, ?> configs) {
    // TODO Auto-generated method stub

  }


  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  /**
   * The StockProducerInterceptor overrides the onAck method and increments onAckCount. Every 100
   * onAckCount, we print out record data.
   */
  @Override
  public void onAcknowledgement(final RecordMetadata metadata, final Exception exception) {
    onAckCount++;

    if (logger.isDebugEnabled()) {
      logger.debug(String.format("onAck topic=%s, part=%d, offset=%d\n", metadata.topic(),
          metadata.partition(), metadata.offset()));
    } else {
      if (onAckCount % 100 == 0) {
        logger.info(String.format("onAck topic=%s, part=%d, offset=%d\n", metadata.topic(),
            metadata.partition(), metadata.offset()));
      }
    }
  }
}
