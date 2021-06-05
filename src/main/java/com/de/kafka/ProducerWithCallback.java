package com.de.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    public static void main(String[] args) {

        Logger log = LoggerFactory.getLogger(ProducerDemo.class);

        // create producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        for (int i = 0; i < 50; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello Consumer " + i);

            // send the data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Received new metadata => \n Topic: " + recordMetadata.topic() + " \n Partition: " + recordMetadata.partition() + "\n Offset: " + recordMetadata.offset() + "\n Timestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("Error while processing: " + e);
                    }
                }
            });

            // flush
            producer.flush();
        }

        // flush and close
        producer.close();

    }

}
