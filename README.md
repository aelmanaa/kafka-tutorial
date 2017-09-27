# kafka-tutorial
## Basic
## Advanced
Tutorial based "kafka Cloudrable advanced producer tutorial" [Cloudurable advanced producer](http://cloudurable.com/blog/kafka-tutorial-kafka-producer-advanced-java-examples/index.html)
Files are created under package com.sbibits.ptrc.tutorials.kafka.advanced:
* StockPrice - holds a stock price has a name, dollar, and cents
* StockPriceKafkaProducer - Configures and creates KafkaProducer, StockSender list, ThreadPool (ExecutorService), starts StockSender runnable into thread pool
* StockAppConstants - holds topic and broker list
* StockPriceSerializer - can serialize a StockPrice into byte[]
* StockSender - generates somewhat random stock prices for a given StockPrice name, Runnable, 1 thread per StockSender and shows using KafkaProducer from many threads
##Avro
Tutorial based "UNDERSTANDING APACHE AVRO: AVRO INTRODUCTION FOR BIG DATA AND DATA STREAMING ARCHITECTURES" [Cloudurable Avro](http://cloudurable.com/blog/avro/index.html)
