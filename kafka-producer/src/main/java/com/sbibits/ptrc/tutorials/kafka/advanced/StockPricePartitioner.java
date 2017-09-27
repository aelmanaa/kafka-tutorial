package com.sbibits.ptrc.tutorials.kafka.advanced;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public class StockPricePartitioner implements Partitioner {

  private final Set<String> importantStocks;

  public StockPricePartitioner() {
    importantStocks = new HashSet<>();
  }

  /**
   * StockPricePartitioner implements the configure() method with importantStocks config property.
   * The importantStocks gets parsed and added to importantStocks HashSet which is used to filter
   * the stocks.
   */
  @Override
  public void configure(Map<String, ?> configs) {
    final String importantStocksStr = (String) configs.get("importantStocks");
    Arrays.stream(importantStocksStr.split(",")).forEach(importantStocks::add);
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  /**
   * IMPORTANT STOCK: If stockName is in the importantStocks HashSet then put it in partitionNum =
   * (partitionCount -1) (last partition). REGULAR STOCK: Otherwise if not in importantStocks set
   * then not important use the the absolute value of the hash of the stockName modulus
   * partitionCount -1 as the partition to send the record partitionNum = abs(stockName.hashCode())
   * % (partitionCount - 1).
   */
  @Override
  public int partition(final String topic, final Object objectKey, final byte[] keyBytes,
      final Object value, final byte[] valueBytes, final Cluster cluster) {

    final List<PartitionInfo> partitionInfoList = cluster.availablePartitionsForTopic(topic);
    final int partitionCount = partitionInfoList.size();
    final int importantPartition = partitionCount - 1;
    final int normalPartitionCount = partitionCount - 1;

    final String key = ((String) objectKey);

    if (importantStocks.contains(key)) {
      return importantPartition;
    } else {
      return Math.abs(key.hashCode()) % normalPartitionCount;
    }

  }

}
