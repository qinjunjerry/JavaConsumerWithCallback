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

        // As usual, create KafkaConsumer with consumer properties, optionally
        // including group.id if specified
        Properties props = new Properties();
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        if ( args.length == 2 ) props.put("group.id", args[1]);
        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);

        // Get the topic from command line to subscribe
        List<String> topics = Arrays.asList(args[0]);

        // Subscribe to the topic with MyConsumerRebalanceListener which implements
        // the interface ConsumerRebalanceListener.
        //
        // MyConsumerRebalanceListener is constructed with a CountDownLatch which
        // is initialized with 1. This way, the latch can count down to 0 when a
        // partition has been assigned (see onPartitionsAssigned() in class
        // MyConsumerRebalanceListener). When the latch's count reaches 0, the
        // main() method will wake up from waiting on the latch via await() (see
        //  the while loop below) and can continue safely to call poll().
        CountDownLatch latch = new CountDownLatch(1);
        consumer.subscribe(topics, new MyConsumerRebalanceListener(latch) );

        long startTime = System.currentTimeMillis();

        // Waiting for the latch to count down to 0
        //
        // The await() method causes the current thread to wait until the latch
        // has counted down to zero, unless the thread is interrupted, or the
        // specified waiting time elapses.
        while (!latch.await(10, TimeUnit.SECONDS)) {
            System.out.println("Waiting for partition assignment ...");
        }
        // Partition has been assigned, now we can call poll()
        ConsumerRecords<String, String> consumerRecords = consumer.poll(200);

        // Just print out the time take to get messages from a topic partition
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("Got "+ consumerRecords.count() +" messages after " + estimatedTime + " milliseconds");

        // do something with the retrieved consumerRecords here

        consumer.close();
        System.out.println("All done.");
    }


    // MyConsumerRebalanceListener

    // This class implements ConsumerRebalanceListener and is constructed with
    // a CountDownLatch which counts down by 1 when onPartitionsAssigned() is called.
    //
    // If interested in the partitions revocation event, some logic can also
    // be added into onPartitionsRevoked(). We only a print here.
    private static class MyConsumerRebalanceListener implements ConsumerRebalanceListener {
        private final CountDownLatch latch;

        public MyConsumerRebalanceListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> arg) {
            System.out.println("Partition Assigned");
            // decrease the count by 1
            latch.countDown();
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> arg) {
            System.out.println("Partition Revoked");
        }
    }

}
