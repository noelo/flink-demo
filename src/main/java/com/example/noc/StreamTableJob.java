package com.example.noc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.JSONSchema;

public class StreamTableJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        PulsarSource<Order> orderFeed = PulsarSource.builder()
                .setServiceUrl("pulsar+ssl://sslproxy-route-pulsar.apps.ocp.sno.themadgrape.com:443")
                .setAdminUrl("https://sslproxy-https-route-pulsar.apps.ocp.sno.themadgrape.com")
                .setStartCursor(StartCursor.earliest())
                .setTopics("OrderDataTopic")
                .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(JSONSchema.of(Order.class), Order.class))
                .setSubscriptionName("DataStreamJob")
                .setSubscriptionType(SubscriptionType.Shared)
                .setConfig(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, Boolean.FALSE)
                .setConfig(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, "/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .setConfig(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, Boolean.TRUE)
                .build();

        PulsarSource<String> enrichmentFeed = PulsarSource.builder()
                .setServiceUrl("pulsar+ssl://sslproxy-route-pulsar.apps.ocp.sno.themadgrape.com:443")
                .setAdminUrl("https://sslproxy-https-route-pulsar.apps.ocp.sno.themadgrape.com")
                .setStartCursor(StartCursor.earliest())
                .setTopics("EnrichmentDataTopic")
                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .setSubscriptionName("DataStreamJob")
                .setSubscriptionType(SubscriptionType.Shared)
                .setConfig(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, Boolean.FALSE)
                .setConfig(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, "/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .setConfig(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, Boolean.TRUE)
                .build();

        DataStream<Order> orderStream = env.fromSource(orderFeed, WatermarkStrategy.forMonotonousTimestamps(), "Order Source").returns(Order.class);
        DataStream<String> enrichmentStream = env.fromSource(enrichmentFeed, WatermarkStrategy.forMonotonousTimestamps(), "Enrichment Source");

        final Table orderTable = tableEnv.fromDataStream(orderStream,
                Schema.newBuilder()
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build());

        final Table enrichmentTable = tableEnv.fromDataStream(enrichmentStream).as("tag");
        orderTable.printSchema();
        orderTable.printExplain();

        Table resultTable = tableEnv.sqlQuery(
                "SELECT rowtime, user, amount FROM "
                        + orderTable);

        resultTable.printExplain();

        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

        resultStream.addSink(new PrintSinkFunction<>("resultTable", Boolean.TRUE));
        env.execute("StreamTableJob");
    }
}
