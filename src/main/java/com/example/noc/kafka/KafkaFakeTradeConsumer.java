package com.example.noc.kafka;

import com.tradedata.schema.Trade;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaFakeTradeConsumer {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = "noc-cluster-kafka-bootstrap-kafka.apps.ocp.sno.themadgrape.com:443";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName());
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.mechanism","SCRAM-SHA-512");
        properties.setProperty("group.id", "KafkaFakeTradeConsumer");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"bifrost\" password=\"Akjhas8732jg\";");
        properties.putIfAbsent("apicurio.registry.url", "http://example-apicurioregistry-kafkasql.kafka.router-default.apps.ocp.sno.themadgrape.com/apis/registry/v2");
        properties.putIfAbsent(SerdeConfig.AUTO_REGISTER_ARTIFACT, Boolean.FALSE);
        properties.setProperty(SerdeConfig.EXPLICIT_ARTIFACT_GROUP_ID,"TestDataGroup");

        KafkaConsumer<String, Trade> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("fakeTradeData3"));
        while (true) {
            ConsumerRecords<String, Trade> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Trade> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}