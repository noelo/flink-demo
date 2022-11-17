package com.example.noc.kafka;

import com.github.javafaker.Faker;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.tradedata.schema.*;


import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class KafkaFakeTradeProducer {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = "noc-cluster-kafka-bootstrap-kafka.apps.ocp.sno.themadgrape.com:443";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class.getName());
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"bifrost\" password=\"Akjhas8732jg\";");
        properties.putIfAbsent(SerdeConfig.REGISTRY_URL, "http://example-apicurioregistry-kafkasql.kafka.router-default.apps.ocp.sno.themadgrape.com/apis/registry/v2");
        properties.putIfAbsent(SerdeConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE);
        properties.setProperty(SerdeConfig.ENABLE_HEADERS, "true");
        properties.setProperty(SerdeConfig.EXPLICIT_ARTIFACT_GROUP_ID,"TestDataGroup");


        KafkaProducer<String, Trade> producer = new KafkaProducer<>(properties);
        Faker faker = new Faker();
        Random newRandom = new Random();

        for (int i = 0; i <= 100; i++) {
            Trade newTrade = new Trade();
            newTrade.setTradeId(UUID.randomUUID());
            newTrade.setTradeTS(Instant.now());
            newTrade.setTradeInst("FAKECORP");
            newTrade.setOrderTS(Instant.now());
            newTrade.setTradePrice(faker.number().numberBetween(0, 1000));
            newTrade.setTradeType(newRandom.nextBoolean() ? TradeType.ASK : TradeType.BID);
            ProducerRecord<String, Trade> producerRecord =
                    new ProducerRecord<>("fakeTradeData3", newTrade);
            producer.send(producerRecord);
        }
        producer.flush();
        producer.close();
    }
}