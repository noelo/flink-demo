package com.example.noc.deltalake;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.example.schema.Order;

import java.util.ArrayList;
import java.util.List;

public class DeltaLakeWrite {
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

        DataStream<Order> orderStream = env.fromSource(orderFeed, WatermarkStrategy.forMonotonousTimestamps(), "Order Source")
                .returns(Order.class);

        String path = "wasb://.......core.windows.net/db";

        RowField user = new RowField("user", DataTypes.BIGINT().getLogicalType());
        RowField productId = new RowField("productId", DataTypes.INT().getLogicalType());
        RowField amount = new RowField("amount", DataTypes.INT().getLogicalType());
        RowField orderTime = new RowField("orderTime", DataTypes.TIME().getLogicalType());

        List<RowField> fields = new ArrayList<RowField>();
        fields.add(user);
        fields.add(productId);
        fields.add(amount);
        fields.add(orderTime);
        RowType rowType = new RowType(fields);

        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new Path(path),
                        new Configuration(),
                        rowType)
                .build();

        env.fromSource(orderFeed, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .flatMap(new RowDataFlatMap())
                .sinkTo(deltaSink);
    }

    public static class RowDataFlatMap extends RichFlatMapFunction<Order, RowData> {
        public RowDataFlatMap(){}

        @Override
        public void flatMap(Order record, Collector<RowData> out) throws Exception {
            GenericRowData rowData = new GenericRowData(3);

            rowData.setField(0, record.user);
            rowData.setField(1, record.productId);
            rowData.setField(2, record.amount);
            out.collect(rowData);
        }
    }
}