package com.example.noc.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.example.schema.FixedIncomeReturnMacroSchema;
import org.example.schema.Order;

import java.util.Properties;

public class SourceCombineKafka {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = "noc-cluster-kafka-bootstrap-kafka.apps.ocp.sno.themadgrape.com:443";

        KafkaSource<String> firstSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("firsttopic")
                .setGroupId("SourceCombineKafka")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .setProperty("security.protocol", "SASL_SSL")
                .setProperty("sasl.mechanism", "SCRAM-SHA-512")
                .setProperty("group.id", "FlinkSourceCombineKafka")
                .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";")
                .setProperty("schema.registry.url", "http://localhost:8081")
                .build();

        KafkaSource<String> secondSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("secondtopic")
                .setGroupId("SourceCombineKafka")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .setProperty("security.protocol", "SASL_SSL")
                .setProperty("sasl.mechanism", "SCRAM-SHA-512")
                .setProperty("group.id", "FlinkSourceCombineKafka")
                .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";")
                .setProperty("schema.registry.url", "http://localhost:8081")
                .build();

        KafkaSource<String> thirdSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("thirdtopic")
                .setGroupId("SourceCombineKafka")
                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .setProperty("security.protocol", "SASL_SSL")
                .setProperty("sasl.mechanism", "SCRAM-SHA-512")
                .setProperty("group.id", "FlinkSourceCombineKafka")
                .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";")
                .setProperty("schema.registry.url", "http://localhost:8081")
                .build();

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("combinedtopic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setProperty("security.protocol", "SASL_SSL")
                .setProperty("sasl.mechanism", "SCRAM-SHA-512")
                .setProperty("group.id", "FlinkSourceCombineKafka")
                .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";")
                .setProperty("schema.registry.url", "http://localhost:8081")
                .build();

        HybridSource<String> hybridSource =
                HybridSource.builder(firstSource)
                        .addSource(secondSource)
                        .addSource(thirdSource)
                        .build();

        DataStream<String> fixedStreamcombined = env.fromSource(hybridSource, WatermarkStrategy.forMonotonousTimestamps(), "Combined Source")
                .returns(String.class);

        fixedStreamcombined.sinkTo(kafkaSink).name("KafkaSink");
//        fixedStreamcombined.addSink(new PrintSinkFunction<>("CombinedOutput", Boolean.TRUE)).name("OutputSink");
        env.execute("SourceCombineJob");
    }
}