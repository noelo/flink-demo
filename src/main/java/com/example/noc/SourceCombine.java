package com.example.noc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.example.schema.FixedIncomeReturnMacroSchema;

public class SourceCombine {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        PulsarSource<FixedIncomeReturnMacroSchema> fixedFeed2009 = PulsarSource.builder()
                .setServiceUrl("pulsar+ssl://sslproxy-route-pulsar.apps.ocp.sno.tmg.com:443")
//                .setAdminUrl("https://sslproxy-https-route-pulsar.apps.ocp.sno.tmg.com:443/admin/v2/")
                .setAdminUrl("http://pulsar-mini-broker-pulsar.apps.ocp.sno.tmg.com")
                .setStartCursor(StartCursor.earliest())
//                .setBoundedStopCursor(StopCursor.latest())
                .setTopics("2009")
                .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(JSONSchema.of(FixedIncomeReturnMacroSchema.class), FixedIncomeReturnMacroSchema.class))
                .setSubscriptionName("SourceCombineJob" + System.currentTimeMillis())
                .setSubscriptionType(SubscriptionType.Shared)
//                .setConfig(PulsarSourceOptions.PULSAR_SUBSCRIPTION_MODE, SubscriptionMode.NonDurable)
                .setConfig(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, Boolean.FALSE)
                .setConfig(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, "/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .setConfig(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_MAX_FETCH_TIME, 10L)
                .setConfig(PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS, 10)
                .build();


        PulsarSource<FixedIncomeReturnMacroSchema> fixedFeed2006 = PulsarSource.builder()
                .setServiceUrl("pulsar+ssl://sslproxy-route-pulsar.apps.ocp.sno.tmg.com:443")
//                .setAdminUrl("https://sslproxy-https-route-pulsar.apps.ocp.sno.tmg.com:443/admin/v2/")
                .setAdminUrl("http://pulsar-mini-broker-pulsar.apps.ocp.sno.tmg.com")
                .setStartCursor(StartCursor.earliest())
                .setBoundedStopCursor(StopCursor.latest())
                .setTopics("2006")
                .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(JSONSchema.of(FixedIncomeReturnMacroSchema.class), FixedIncomeReturnMacroSchema.class))
                .setSubscriptionName("SourceCombineJob" + System.currentTimeMillis())
                .setSubscriptionType(SubscriptionType.Shared)
//                .setConfig(PulsarSourceOptions.PULSAR_SUBSCRIPTION_MODE, SubscriptionMode.NonDurable)
                .setConfig(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, Boolean.FALSE)
                .setConfig(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, "/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .setConfig(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_MAX_FETCH_TIME, 10L)
                .setConfig(PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS, 10)
                .build();

        PulsarSource<FixedIncomeReturnMacroSchema> fixedFeed2003 = PulsarSource.builder()
                .setServiceUrl("pulsar+ssl://sslproxy-route-pulsar.apps.ocp.sno.tmg.com:443")
//                .setAdminUrl("https://sslproxy-https-route-pulsar.apps.ocp.sno.tmg.com:443/admin/v2/")
                .setAdminUrl("http://pulsar-mini-broker-pulsar.apps.ocp.sno.tmg.com")
                .setStartCursor(StartCursor.earliest())
                .setBoundedStopCursor(StopCursor.latest())
                .setTopics("2003")
                .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(JSONSchema.of(FixedIncomeReturnMacroSchema.class), FixedIncomeReturnMacroSchema.class))
                .setSubscriptionName("SourceCombineJob" + System.currentTimeMillis())
                .setSubscriptionType(SubscriptionType.Shared)
//                .setConfig(PulsarSourceOptions.PULSAR_SUBSCRIPTION_MODE, SubscriptionMode.NonDurable)
                .setConfig(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, Boolean.FALSE)
                .setConfig(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, "/home/noelo/dev/noc-pulsar-client/client/certs/pulsar-proxy.pem")
                .setConfig(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, Boolean.TRUE)
                .setConfig(PulsarSourceOptions.PULSAR_MAX_FETCH_TIME, 10L)
                .setConfig(PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS, 10)
                .build();

        HybridSource<FixedIncomeReturnMacroSchema> hybridSource =
                HybridSource.builder(fixedFeed2003)
                        .addSource(fixedFeed2006)
                        .addSource(fixedFeed2009)
                        .build();

        DataStream<FixedIncomeReturnMacroSchema> fixedStreamcombined = env.fromSource(hybridSource, WatermarkStrategy.forMonotonousTimestamps(), "Combined Source")
                .returns(FixedIncomeReturnMacroSchema.class);


        fixedStreamcombined.addSink(new PrintSinkFunction<>("CombinedOutput", Boolean.TRUE)).name("OutputSink");
        env.execute("SourceCombineJob");
    }
}