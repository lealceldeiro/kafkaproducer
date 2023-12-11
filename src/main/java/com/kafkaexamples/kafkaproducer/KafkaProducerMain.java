package com.kafkaexamples.kafkaproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class KafkaProducerMain {
    private static final Logger log = Logger.getAnonymousLogger();

    public static void main(String[] args) {
        String bootstrapServer = args.length > 0 ? args[0] : "localhost:9094";
        String topic = args.length > 1 ? args[1] : "topic1";

        Scanner scanner = new Scanner(System.in);

        Producer<String, String> producer = producer(bootstrapServer);
        printPartitionsInfo(producer, topic);

        int key = 0;
        while (true) {
            log.info("Enter a message to be sent or '/exit' to finish");
            String msg = scanner.nextLine();

            if ("/exit".equals(msg)) {
                System.exit(0);
            }

            sendMessage(producer, topic, key++, msg);
        }
    }

    private static Producer<String, String> producer(String bootstrapServer) {
        return new KafkaProducer<>(properties(bootstrapServer));
    }

    private static Properties properties(String bootstrapServer) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        props.put(CLIENT_ID_CONFIG, "com.kafkaexamples.kafkaproducer");

        return props;
    }

    private static void printPartitionsInfo(Producer<String, String> producer, String topic) {
        log.info("Getting partitions info for topic " + topic + "...");
        List<PartitionInfo> info = producer.partitionsFor(topic);

        log.info(info.size() + " partitions");
        String msg = info.stream().map(PartitionInfo::partition).map(String::valueOf).collect(joining(", "));

        log.info("Partition ids: " + msg);
    }

    private static void sendMessage(Producer<String, String> producer, String topic, int key, String msg) {
        log.info("Sending message to topic " + topic + "...");
        producer.send(newRecord(topic, key, msg), KafkaProducerMain::handleResponse);
    }

    private static ProducerRecord<String, String> newRecord(String topic, int key, String msg) {
        return new ProducerRecord<>(topic, String.valueOf(key), msg);
    }

    private static void handleResponse(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            log.log(Level.SEVERE, "Error sending message to broker", e);
            return;
        }
        log.info("Successfully produced message to topic: " + recordMetadata.topic());
    }
}
