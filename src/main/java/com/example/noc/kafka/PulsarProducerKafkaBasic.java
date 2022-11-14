package com.example.noc.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.schema.ProductDetails;

import java.util.Properties;
import java.util.Random;

public class PulsarProducerKafkaBasic {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = "noc-cluster-kafka-bootstrap-kafka.apps.ocp.sno.themadgrape.com:443";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.mechanism","SCRAM-SHA-512");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=\"bifrost\" password=\"Akjhas8732jg\";");
        properties.setProperty("schema.registry.url","http://localhost:8081");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i <= 100000; i++) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("firsttopic", "FIRST-->"+System.currentTimeMillis());
            producer.send(producerRecord);
        }

        for (int i = 0; i <= 100000; i++) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("secondtopic", "SECOND-->"+System.currentTimeMillis());
            producer.send(producerRecord);
        }

        for (int i = 0; i <= 100000; i++) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("thirdtopic", "THIRD-->"+System.currentTimeMillis());
            producer.send(producerRecord);
        }

        producer.flush();
        producer.close();
    }
}