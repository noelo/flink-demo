package com.example.noc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.pulsar.client.api.SubscriptionType;


public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        PulsarSource<String> fixedSource = PulsarSource.builder()
                .setServiceUrl("pulsar+ssl://sslproxy-route-pulsar.apps.ocp.sno.themadgrape.com:443")
                .setAdminUrl("https://sslproxy-https-route-pulsar.apps.ocp.sno.themadgrape.com")
                .setStartCursor(StartCursor.earliest())
                .setTopics("TestDataTopic")
                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .setSubscriptionName("DataStreamJob")
                .setSubscriptionType(SubscriptionType.Shared)
                .setConfig(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, Boolean.FALSE)
                .setConfig(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, "/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .setConfig(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, Boolean.TRUE)
                .build();

        PulsarSink<String> fixedDest = PulsarSink.builder()
                .setServiceUrl("pulsar+ssl://sslproxy-route-pulsar.apps.ocp.sno.themadgrape.com:443")
                .setAdminUrl("https://sslproxy-https-route-pulsar.apps.ocp.sno.themadgrape.com")
                .setTopics("TestDataTopicSink")
                .setSerializationSchema(PulsarSerializationSchema.flinkSchema(new SimpleStringSchema()))
                .setConfig(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, Boolean.FALSE)
                .setConfig(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, "/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .setConfig(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, Boolean.TRUE)
                .build();

        DataStream<String> fixedStream = env.fromSource(fixedSource, WatermarkStrategy.noWatermarks(), "Pulsar Source")
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value.toUpperCase()+String.valueOf(System.currentTimeMillis());
                    }
                });

        fixedStream.addSink(new PrintSinkFunction<>("outputSink", Boolean.TRUE));
        fixedStream.sinkTo(fixedDest);
        env.execute("DataStreamJob");

    }
}


//	PulsarSink<String> fixedDest = PulsarSink.builder()
//				.setServiceUrl("pulsar+ssl://sslproxy-route-pulsar.apps.ocp.sno.themadgrape.com:443")
//				.setAdminUrl("https://sslproxy-https-route-pulsar.apps.ocp.sno.themadgrape.com")
//				.setTopics("persistent://public/default/TestDataTopicSink")
//				.setSerializationSchema(PulsarSerializationSchema.flinkSchema(new SimpleStringSchema()))
//				.setConfig(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE,Boolean.FALSE)
//				.setConfig(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, "/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
//				.setConfig(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, Boolean.TRUE)
//				.build();

//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    PulsarSource<String> source = PulsarSource.builder()
//            .setServiceUrl("pulsar://47.102.192.43:6650")
//            .setAdminUrl("http://47.102.192.43:8888")
//            .setStartCursor(StartCursor.earliest())
//            .setTopics("dev-message")
//            .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
//            .setSubscriptionName("flink-subscription")
//            .setSubscriptionType(SubscriptionType.Exclusive).build();
//    DataStreamSource<String> dataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Source Work");
//    dataStreamSource.print();
//    try{env.execute("pulsar");}catch(Exception e){e.printStackTrace();}