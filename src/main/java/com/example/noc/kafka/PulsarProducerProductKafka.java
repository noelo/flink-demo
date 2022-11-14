package com.example.noc.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.schema.ProductDetails;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Properties;
import java.util.Random;

public class PulsarProducerProductKafka {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = "noc-cluster-kafka-bootstrap-kafka.apps.ocp.sno.themadgrape.com:443";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.mechanism","SCRAM-SHA-512");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=\"bifrost\" password=\"Akjhas8732jg\";");
        properties.setProperty("schema.registry.url","http://localhost:8081");

        KafkaProducer<String, ProductDetails> producer = new KafkaProducer<>(properties);

        Random rand = new Random();
        for (int i = 0; i <= 10; i++) {
            ProductDetails product = new ProductDetails("testProduct description for product"+i, i, rand.nextLong());
            ProducerRecord<String, ProductDetails> producerRecord =
                    new ProducerRecord<>("first_topic", product);
            producer.send(producerRecord);
        }

        producer.flush();
        producer.close();
    }
}