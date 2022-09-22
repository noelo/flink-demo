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
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import static org.apache.flink.table.api.Expressions.$;

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
                .setSubscriptionName("StreamTableJob")
                .setSubscriptionType(SubscriptionType.Shared)
                .setConfig(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, Boolean.FALSE)
                .setConfig(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, "/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .setConfig(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, Boolean.TRUE)
                .build();

        PulsarSource<ProductDetails> productFeed = PulsarSource.builder()
                .setServiceUrl("pulsar+ssl://sslproxy-route-pulsar.apps.ocp.sno.themadgrape.com:443")
                .setAdminUrl("https://sslproxy-https-route-pulsar.apps.ocp.sno.themadgrape.com")
                .setStartCursor(StartCursor.earliest())
                .setTopics("ProductTopic")
                .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(JSONSchema.of(ProductDetails.class), ProductDetails.class))
                .setSubscriptionName("StreamTableJob")
                .setSubscriptionType(SubscriptionType.Shared)
                .setConfig(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, Boolean.FALSE)
                .setConfig(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, "/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .setConfig(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, Boolean.TRUE)
                .build();

        DataStream<Order> orderStream = env.fromSource(orderFeed, WatermarkStrategy.forMonotonousTimestamps(), "Order Source")
                                        .returns(Order.class);
        DataStream<ProductDetails> productStream = env.fromSource(productFeed, WatermarkStrategy.forMonotonousTimestamps(), "Product Details Source")
                                        .returns(ProductDetails.class);

        final Table orderTable = tableEnv.fromDataStream(orderStream,
                Schema.newBuilder()
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build());

        tableEnv.createTemporaryView("ordersView", orderStream,
                Schema.newBuilder()
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build());

        orderTable.printSchema();
        orderTable.printExplain();

        final Table productDetailsTable = tableEnv.fromDataStream(productStream,
                Schema.newBuilder()
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build());

        tableEnv.createTemporaryView("productDetailsView", productStream,
                Schema.newBuilder()
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .watermark("rowtime", "SOURCE_WATERMARK()")
                        .build());

        productDetailsTable.printSchema();
        productDetailsTable.printExplain();

//        TemporalTableFunction enrichTT = tableEnv
//                .from("productDetailsView").createTemporalTableFunction($("rowtime"), $("f0"));
//
//        tableEnv.createTemporarySystemFunction("enrichmentFN", enrichTT);

        Table resultTable = tableEnv.sqlQuery(
                "SELECT rowtime, user, amount FROM "
                        + orderTable);

        Table resultTable2 = tableEnv.sqlQuery(
                "SELECT * FROM productDetailsView");

        Table resultTable3 = tableEnv.sqlQuery(
                "SELECT * FROM ordersView "+
                "INNER JOIN productDetailsView ON ordersView.productId = productDetailsView.productId");

        resultTable.printExplain();
        resultTable3.printExplain();
        resultTable3.printSchema();

        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);
        DataStream<Row> resultStream2 = tableEnv.toDataStream(resultTable2);
        DataStream<Row> resultStream3 = tableEnv.toDataStream(resultTable3);

        resultStream.addSink(new PrintSinkFunction<>("orderTable ", Boolean.TRUE));
        resultStream2.addSink(new PrintSinkFunction<>("productDetailsView ", Boolean.TRUE));
        resultStream3.addSink(new PrintSinkFunction<>("CombinedproductDetailsView ", Boolean.TRUE));
        env.execute("StreamTableJob");
    }
}
