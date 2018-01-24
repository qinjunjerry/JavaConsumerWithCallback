package com.mapr.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class JavaConsumerWithCallback {

    public static void main(String[] args) throws IOException, InterruptedException {

        if ( args.length < 1) {
            System.out.println("Usage: JavaConsumerWithCallback <topic> [groupid]");
            return;
        }

        Properties props = new Properties();
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");

        if ( args.length == 2 ) props.put("group.id", args[1]);

        List<String> topics = Arrays.asList(args[0]);

        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
        CountDownLatch latch = new CountDownLatch(1);
        consumer.subscribe(topics, new MyConsumerRebalanceListener(latch) );

        long startTime = System.currentTimeMillis();

        // waiting for rebalance event then call poll()
        while (!latch.await(10, TimeUnit.SECONDS)) {
            System.out.println("Waiting for partition assignment ...");
        }
        ConsumerRecords<String, String> consumerRecords = consumer.poll(200);

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("Got "+ consumerRecords.count() +" messages after " + estimatedTime + " milliseconds");

        consumer.close();
        System.out.println("All done.");
    }

    private static class MyConsumerRebalanceListener implements ConsumerRebalanceListener {
        private final CountDownLatch latch;

        public MyConsumerRebalanceListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> arg) {
            System.out.println("Partition Assigned");
            latch.countDown();
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> arg) {
            System.out.println("Partition Revoked");
        }
    }

}
