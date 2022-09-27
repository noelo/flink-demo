package com.example.noc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
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
                .setUnboundedStopCursor(StopCursor.never())
                .setTopics("OrderDataTopic")
                .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(JSONSchema.of(Order.class), Order.class))
                .setSubscriptionName("StreamTableJob")
                .setSubscriptionType(SubscriptionType.Shared)
                .setConfig(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, Boolean.FALSE)
                .setConfig(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, "/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .setConfig(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_MAX_FETCH_TIME, 10L)
                .setConfig(PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS, 10)
                .build();

        PulsarSource<ProductDetails> productFeed = PulsarSource.builder()
                .setServiceUrl("pulsar+ssl://sslproxy-route-pulsar.apps.ocp.sno.themadgrape.com:443")
                .setAdminUrl("https://sslproxy-https-route-pulsar.apps.ocp.sno.themadgrape.com")
                .setStartCursor(StartCursor.earliest())
                .setUnboundedStopCursor(StopCursor.never())
                .setTopics("ProductTopic")
                .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(JSONSchema.of(ProductDetails.class), ProductDetails.class))
                .setSubscriptionName("StreamTableJob")
                .setSubscriptionType(SubscriptionType.Shared)
                .setConfig(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, Boolean.FALSE)
                .setConfig(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, "/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .setConfig(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_MAX_FETCH_TIME, 10L)
                .setConfig(PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS, 10)
                .build();

        DataStream<Order> orderStream = env.fromSource(orderFeed, WatermarkStrategy.forMonotonousTimestamps(), "Order Source")
                .returns(Order.class);
        DataStream<ProductDetails> productStream = env.fromSource(productFeed, WatermarkStrategy.forMonotonousTimestamps(), "Product Details Source")
                .returns(ProductDetails.class);

        tableEnv.createTemporaryView("ordersView", orderStream,
                Schema.newBuilder()
                        .columnByExpression("proc_time", "PROCTIME()")
                        .columnByMetadata("orderTime", "TIMESTAMP_LTZ(3)", "rowtime", Boolean.TRUE)
                        .watermark("orderTime", "SOURCE_WATERMARK()")
                        .build());
        tableEnv.from("ordersView").printSchema();

        tableEnv.createTemporaryView("versionedProductDetailsView", productStream,
                Schema.newBuilder()
                        .columnByExpression("proc_time", "PROCTIME()")
                        .columnByMetadata("updateTime", "TIMESTAMP_LTZ(3)", "rowtime", Boolean.TRUE)
                        .watermark("updateTime", "SOURCE_WATERMARK()")
                        .primaryKey("productId")
                        .build());

        tableEnv.from("versionedProductDetailsView").printSchema();


//        TemporalTableFunction enrichTT = tableEnv
//                .from("productDetailsView").createTemporalTableFunction($("rowtime"), $("f0"));
//
//        tableEnv.createTemporarySystemFunction("enrichmentFN", enrichTT);


        Table resultTable = tableEnv.sqlQuery(
                "SELECT ordersView.productId, ordersView.amount, ordersView.orderTime" +
                        ", versionedProductDetailsView.description, versionedProductDetailsView.costPrice, versionedProductDetailsView.updateTime " +
                        " FROM ordersView INNER JOIN versionedProductDetailsView ON ordersView.productId = versionedProductDetailsView.productId");

        resultTable.printExplain();
        resultTable.printSchema();

        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

        resultStream.addSink(new PrintSinkFunction<>("CombinedproductDetailsView ", Boolean.TRUE));
        env.execute("StreamTableJob");
    }
}
